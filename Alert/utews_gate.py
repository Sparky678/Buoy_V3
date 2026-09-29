#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==============================================================================
U-TEWS - GERBANG PERINGATAN (AND GATE) EEMD x AMPLITUDO 3 MENIT
Orange Pi 5 - Pulau Sebesi, Lampung
==============================================================================
Satu-satunya program yang mengirim notifikasi Telegram. Peringatan resmi
tsunami hanya diterbitkan bila, pada window (t_window_end) yang SAMA:

    EEMD  : warning_issued = True   (Layer 2, N_CONFIRM terpenuhi)
    DAN
    AMP   : status = CONFIRM        (range 3 menit <= ambang, "tetap")

Sumber data (dibaca, tidak diubah):
  utews_realtime.py  -> /home/orangepi/data/utews_realtime/detection_YYYY-MM-DD.csv
  utews_amplitude.py -> /home/orangepi/data/utews_amplitude/amplitude_YYYY-MM-DD.csv
                        /home/orangepi/data/utews_amplitude/status_amplitude.json

Alur tiap POLL_S detik:
  1. Cari baris baru di log EEMD (t_window_end > terakhir diproses).
  2. Cari baris log amplitudo dengan t_window_end yang sama. Bila belum ada,
     tunggu sampai AMP_WAIT_S detik setelah t_window_end; lewat dari itu
     dianggap MISSING (bukan konfirmasi).
  3. Update state episode, tentukan event, kirim Telegram, tulis log gate.
  4. Cek kesehatan: log EEMD / status amplitudo basi > STALE_S -> notifikasi.

Event Telegram:
  STARTUP  : gate mulai berjalan
  DETECT   : EEMD Layer 1 IDLE -> ACTIVE (deteksi awal, informatif)
  WARNING  : EEMD L2 DAN amplitudo CONFIRM -> PERINGATAN RESMI (sekali/episode)
  BLOCKED  : EEMD L2 terpenuhi tetapi amplitudo tidak CONFIRM (sekali/episode,
             informatif untuk operator; bukan peringatan)
  CLEAR    : EEMD Layer 1 ACTIVE -> IDLE
  HEALTH   : salah satu program berhenti memperbarui data / pulih kembali

Mode:
  python3 utews_gate.py                                         # real-time
  python3 utews_gate.py --replay "2026-09-21 00:00" "2026-09-21 06:00" \\
      --amp-prefix replay                                       # evaluasi ulang
  (replay tidak mengirim Telegram; hasil ke gate_replay_*.csv + ringkasan)

Token Telegram dapat diisi lewat environment variable (disarankan):
  UTEWS_TG_TOKEN, UTEWS_TG_CHAT_ID
  (bila kosong, dipakai nilai di bagian KONFIGURASI)

Contoh service systemd (/etc/systemd/system/utews-gate.service):
  [Unit]
  Description=U-TEWS Gerbang Peringatan (EEMD x Amplitudo)
  After=network-online.target

  [Service]
  ExecStart=/usr/bin/python3 /home/orangepi/utews/utews_gate.py
  WorkingDirectory=/home/orangepi/utews
  Restart=always
  RestartSec=5
  User=orangepi

  [Install]
  WantedBy=multi-user.target

Penulis : Aryo (UNILA), 2026
==============================================================================
"""

import os
import sys
import csv
import json
import time
import argparse
import logging
import traceback
from datetime import datetime, timedelta

import requests

# ============================================================================
# 1. KONFIGURASI
# ============================================================================

# --- SUMBER ---
EEMD_LOG_DIR       = "/home/orangepi/data/utews_realtime"
EEMD_LOG_PATTERN   = "detection_{date}.csv"
AMP_LOG_DIR        = "/home/orangepi/data/utews_amplitude"
AMP_LOG_PREFIX     = "amplitude"                 # amplitude_{date}.csv
AMP_STATUS_FILE    = "status_amplitude.json"

# --- KELUARAN GATE ---
GATE_LOG_DIR       = "/home/orangepi/data/utews_gate"
GATE_STATE_FILE    = "gate_state.json"

# --- TIMING ---
POLL_S             = 5      # periksa log tiap 5 detik
AMP_WAIT_S         = 90     # batas tunggu baris amplitudo setelah t_window_end
STALE_S            = 180    # data basi bila tidak diperbarui > 3 menit
STATE_MAX_AGE_S    = 3600   # state tersimpan > 1 jam dianggap usang saat restart

# --- NOTIFIKASI ---
NOTIFY_DETECT      = True
NOTIFY_BLOCKED     = True
NOTIFY_CLEAR       = True
NOTIFY_HEALTH      = True

# --- TELEGRAM ---
TELEGRAM_BOT_TOKEN   = os.environ.get("UTEWS_TG_TOKEN", "")
TELEGRAM_CHAT_ID     = os.environ.get("UTEWS_TG_CHAT_ID", "")
TELEGRAM_API_URL     = "https://api.telegram.org/bot{token}/sendMessage"
TELEGRAM_TIMEOUT_S   = 10
TELEGRAM_MAX_RETRIES = 3

# --- IDENTITAS ---
SYSTEM_ID          = "U-TEWS Pulau Sebesi"
SYSTEM_LOCATION    = "Lampung, Indonesia"

# ============================================================================
# 2. LOGGING
# ============================================================================

def setup_logging(log_dir):
    os.makedirs(log_dir, exist_ok=True)
    logger = logging.getLogger("utews_gate")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)
    fh = logging.FileHandler(os.path.join(
        log_dir, f"utews_gate_{datetime.now():%Y-%m-%d}.log"))
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    return logger


log = logging.getLogger("utews_gate")

# ============================================================================
# 3. PEMBACAAN LOG SUMBER
# ============================================================================

def _read_csv_rows(path):
    if not os.path.exists(path):
        return []
    try:
        with open(path, newline="", encoding="utf-8", errors="replace") as f:
            return list(csv.DictReader(f))
    except Exception as e:
        log.warning(f"[CSV] Gagal baca {path}: {e}")
        return []


def _parse_ts(s):
    try:
        return datetime.fromisoformat(str(s).strip())
    except Exception:
        return None


def _to_bool(s):
    return str(s).strip().lower() in ("true", "1", "yes")


def _to_float(s):
    try:
        return float(s)
    except (TypeError, ValueError):
        return None


def read_eemd_rows(eemd_dir, dates):
    """Baris log EEMD dari beberapa tanggal, terurut, tanpa duplikat t."""
    out = {}
    for d in dates:
        path = os.path.join(eemd_dir, EEMD_LOG_PATTERN.format(date=d.strftime("%Y-%m-%d")))
        for r in _read_csv_rows(path):
            t = _parse_ts(r.get("t_window_end"))
            if t is None:
                continue
            out[t] = {
                "t"             : t,
                "hyst_state"    : str(r.get("hyst_state", "")).strip().upper(),
                "consecutive"   : int(_to_float(r.get("consecutive_active")) or 0),
                "warning_issued": _to_bool(r.get("warning_issued")),
                "lm_abs"        : _to_float(r.get("lm_abs")),
                "thr_trigger"   : _to_float(r.get("threshold_trigger")),
            }
    return [out[k] for k in sorted(out)]


class AmpLog:
    """Akses log amplitudo per t_window_end (cache per tanggal, reload bila berubah)."""

    def __init__(self, amp_dir, prefix):
        self.amp_dir = amp_dir
        self.prefix = prefix
        self._cache = {}   # date -> (mtime, size, {t: row})

    def _load(self, d):
        path = os.path.join(self.amp_dir, f"{self.prefix}_{d:%Y-%m-%d}.csv")
        if not os.path.exists(path):
            return {}
        st = os.stat(path)
        key = (st.st_mtime, st.st_size)
        cached = self._cache.get(d)
        if cached and cached[0] == key:
            return cached[1]
        rows = {}
        for r in _read_csv_rows(path):
            t = _parse_ts(r.get("t_window_end"))
            if t is not None:
                rows[t] = {
                    "status" : str(r.get("status", "")).strip().upper(),
                    "range_m": _to_float(r.get("range_m")),
                    "thr_m"  : _to_float(r.get("threshold_m")),
                    "cov_pct": _to_float(r.get("coverage_pct")),
                }
        self._cache[d] = (key, rows)
        return rows

    def get(self, t):
        for d in (t.date(), (t - timedelta(minutes=1)).date()):
            row = self._load(d).get(t)
            if row:
                return row
        return None

# ============================================================================
# 4. STATE MACHINE GERBANG
# ============================================================================

class Gate:
    """
    State per episode (satu episode = rentang EEMD Layer 1 ACTIVE).
    Dipanggil sekali per window dengan baris EEMD dan status amplitudo.
    """

    def __init__(self):
        self.prev_hyst          = "IDLE"
        self.t_first_detect     = None
        self.t_eemd_warning     = None
        self.t_final_warning    = None
        self.final_warning_sent = False
        self.blocked_notified   = False
        self.n_blocked          = 0

    # ---- persistence ----
    def to_dict(self):
        f = lambda t: t.isoformat() if t else None
        return {
            "prev_hyst"         : self.prev_hyst,
            "t_first_detect"    : f(self.t_first_detect),
            "t_eemd_warning"    : f(self.t_eemd_warning),
            "t_final_warning"   : f(self.t_final_warning),
            "final_warning_sent": self.final_warning_sent,
            "blocked_notified"  : self.blocked_notified,
            "n_blocked"         : self.n_blocked,
        }

    def from_dict(self, d):
        self.prev_hyst          = d.get("prev_hyst", "IDLE")
        self.t_first_detect     = _parse_ts(d.get("t_first_detect")) if d.get("t_first_detect") else None
        self.t_eemd_warning     = _parse_ts(d.get("t_eemd_warning")) if d.get("t_eemd_warning") else None
        self.t_final_warning    = _parse_ts(d.get("t_final_warning")) if d.get("t_final_warning") else None
        self.final_warning_sent = bool(d.get("final_warning_sent", False))
        self.blocked_notified   = bool(d.get("blocked_notified", False))
        self.n_blocked          = int(d.get("n_blocked", 0))

    def _reset_episode(self):
        self.t_first_detect     = None
        self.t_eemd_warning     = None
        self.t_final_warning    = None
        self.final_warning_sent = False
        self.blocked_notified   = False
        self.n_blocked          = 0

    # ---- update ----
    def update(self, e, amp):
        """
        e   : baris EEMD (dict dari read_eemd_rows)
        amp : baris amplitudo atau None (MISSING)
        Returns (record_log, events[list of (type, info)])
        """
        t = e["t"]
        hyst = e["hyst_state"] if e["hyst_state"] in ("IDLE", "ACTIVE") else "IDLE"
        amp_status = amp["status"] if amp else "MISSING"
        amp_confirm = amp_status == "CONFIRM"
        events = []

        # Awal episode
        if self.prev_hyst == "IDLE" and hyst == "ACTIVE":
            self._reset_episode()
            self.t_first_detect = t
            events.append(("DETECT", {}))

        final = False
        if hyst == "ACTIVE":
            if e["warning_issued"] and self.t_eemd_warning is None:
                self.t_eemd_warning = t
            final = e["warning_issued"] and amp_confirm

            if final and not self.final_warning_sent:
                self.final_warning_sent = True
                self.t_final_warning = t
                events.append(("WARNING", {}))
            elif e["warning_issued"] and not amp_confirm and not self.final_warning_sent:
                self.n_blocked += 1
                if not self.blocked_notified:
                    self.blocked_notified = True
                    events.append(("BLOCKED", {}))

        # Akhir episode
        if self.prev_hyst == "ACTIVE" and hyst == "IDLE":
            dur = ((t - self.t_first_detect).total_seconds() / 60.0
                   if self.t_first_detect else None)
            events.append(("CLEAR", {
                "duration_min"   : dur,
                "warning_sent"   : self.final_warning_sent,
                "n_blocked"      : self.n_blocked,
                "t_first_detect" : self.t_first_detect,
                "t_eemd_warning" : self.t_eemd_warning,
                "t_final_warning": self.t_final_warning,
            }))
            self._reset_episode()

        self.prev_hyst = hyst

        record = {
            "t_window_end"      : t.isoformat(),
            "eemd_hyst_state"   : hyst,
            "eemd_consecutive"  : e["consecutive"],
            "eemd_warning"      : e["warning_issued"],
            "eemd_lm_abs"       : e["lm_abs"] if e["lm_abs"] is not None else "",
            "amp_status"        : amp_status,
            "amp_range_m"       : amp["range_m"] if amp and amp["range_m"] is not None else "",
            "final_warning"     : final,
            "events"            : "|".join(ev for ev, _ in events),
        }
        return record, events

# ============================================================================
# 5. TELEGRAM
# ============================================================================

def send_telegram(message, enabled=True):
    if not enabled:
        log.info(f"[TG-OFF]\n{message}")
        return False
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("[TG] Token/chat_id kosong - pesan tidak dikirim")
        log.info(f"[TG-DRYRUN]\n{message}")
        return False

    url = TELEGRAM_API_URL.format(token=TELEGRAM_BOT_TOKEN)
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message,
               "parse_mode": "HTML", "disable_web_page_preview": True}
    for attempt in range(1, TELEGRAM_MAX_RETRIES + 1):
        try:
            r = requests.post(url, json=payload, timeout=TELEGRAM_TIMEOUT_S)
            if r.status_code == 200:
                log.info(f"[TG] Pesan terkirim (attempt {attempt})")
                return True
            log.warning(f"[TG] HTTP {r.status_code}: {r.text[:200]}")
        except Exception as e:
            log.warning(f"[TG] Attempt {attempt}/{TELEGRAM_MAX_RETRIES} gagal: {e}")
        time.sleep(2 ** attempt)
    log.error("[TG] Semua retry gagal")
    return False


def _amp_txt(amp):
    if not amp:
        return "MISSING (data layer amplitudo tidak tersedia)"
    if amp["range_m"] is None:
        return f"{amp['status']}"
    return (f"{amp['status']} (range {amp['range_m']:.3f} m, "
            f"ambang {amp['thr_m']:.3f} m)")


def fmt_startup():
    return (
        f"\U0001F7E2 <b>U-TEWS Gate Started</b>\n\n"
        f"<b>Sistem</b> : {SYSTEM_ID}\n"
        f"<b>Waktu</b>  : {datetime.now():%Y-%m-%d %H:%M:%S} WIB\n\n"
        f"<b>Logika peringatan:</b>\n"
        f"EEMD Layer 2 (N_CONFIRM) <b>DAN</b> layer amplitudo 3 menit CONFIRM\n\n"
        f"<i>Sistem siap memonitor.</i>"
    )


def fmt_detect(e, amp):
    return (
        f"\u26A0\uFE0F <b>[L1] DETEKSI AWAL - {SYSTEM_ID}</b>\n\n"
        f"<b>Waktu</b>     : {e['t']:%Y-%m-%d %H:%M:%S} WIB\n"
        f"<b>|LM| EEMD</b> : {e['lm_abs']:.4f} m\n"
        f"<b>Amplitudo</b> : {_amp_txt(amp)}\n\n"
        f"<i>Menunggu konfirmasi EEMD Layer 2 dan layer amplitudo.</i>\n"
        f"<i>Lokasi: {SYSTEM_LOCATION}</i>"
    )


def fmt_warning(e, amp, gate):
    def dmin(a, b):
        return (a - b).total_seconds() / 60.0 if a and b else float("nan")
    return (
        f"\U0001F6A8\U0001F6A8\U0001F6A8 <b>WARNING TSUNAMI - {SYSTEM_ID}</b> "
        f"\U0001F6A8\U0001F6A8\U0001F6A8\n\n"
        f"<b>Diterbitkan</b>   : {e['t']:%Y-%m-%d %H:%M:%S} WIB\n"
        f"<b>Deteksi awal</b>  : {gate.t_first_detect:%H:%M:%S} WIB\n"
        f"<b>Konfirmasi EEMD</b>: {gate.t_eemd_warning:%H:%M:%S} WIB\n"
        f"<b>Delay total</b>   : {dmin(e['t'], gate.t_first_detect):.1f} menit "
        f"dari deteksi awal\n"
        f"<b>|LM| terkini</b>  : {e['lm_abs']:.4f} m\n"
        f"<b>Amplitudo</b>     : {_amp_txt(amp)}\n\n"
        f"<b>SEGERA EVAKUASI KE TEMPAT TINGGI!</b>\n\n"
        f"<i>Konfirmasi ganda: EEMD W-REF (Layer 2) + amplitudo 3 menit</i>\n"
        f"<i>Lokasi: {SYSTEM_LOCATION}</i>"
    )


def fmt_blocked(e, amp):
    return (
        f"\u2139\uFE0F <b>[INFO] Konfirmasi EEMD tertahan - {SYSTEM_ID}</b>\n\n"
        f"<b>Waktu</b>     : {e['t']:%Y-%m-%d %H:%M:%S} WIB\n"
        f"<b>EEMD</b>      : Layer 2 terpenuhi (|LM| {e['lm_abs']:.4f} m)\n"
        f"<b>Amplitudo</b> : {_amp_txt(amp)}\n\n"
        f"<i>Peringatan resmi BELUM diterbitkan. Gate terus memantau; warning "
        f"terbit otomatis bila layer amplitudo CONFIRM selama episode ini.</i>"
    )


def fmt_clear(e, info):
    dur = info["duration_min"]
    if info["warning_sent"]:
        status = "peringatan resmi sempat diterbitkan"
    elif info["n_blocked"]:
        status = f"tanpa peringatan resmi ({info['n_blocked']} window EEMD L2 tertahan)"
    else:
        status = "tanpa peringatan resmi"
    dur_txt = f"{dur:.1f} menit ACTIVE" if dur is not None else "-"
    return (
        f"\u2705 <b>[CLEAR] Kembali Normal - {SYSTEM_ID}</b>\n\n"
        f"<b>Waktu</b>  : {e['t']:%Y-%m-%d %H:%M:%S} WIB\n"
        f"<b>Durasi</b> : {dur_txt}\n"
        f"<b>Episode</b>: {status}\n\n"
        f"<i>Sistem kembali memonitor normal.</i>"
    )


def fmt_health(name, ok, detail):
    if ok:
        return (f"\U0001F7E2 <b>[HEALTH] {name} pulih - {SYSTEM_ID}</b>\n\n"
                f"{detail}\n<i>{datetime.now():%Y-%m-%d %H:%M:%S} WIB</i>")
    return (f"\U0001F7E0 <b>[HEALTH] {name} tidak memperbarui data - {SYSTEM_ID}</b>\n\n"
            f"{detail}\n\n<i>Peringatan resmi TIDAK dapat diterbitkan selama "
            f"kondisi ini. Periksa service terkait.</i>\n"
            f"<i>{datetime.now():%Y-%m-%d %H:%M:%S} WIB</i>")

# ============================================================================
# 6. LOG GATE & STATE
# ============================================================================

GATE_LOG_FIELDS = ["t_window_end", "eemd_hyst_state", "eemd_consecutive",
                   "eemd_warning", "eemd_lm_abs", "amp_status", "amp_range_m",
                   "final_warning", "events"]


def append_gate_log(record, log_dir, prefix="gate"):
    date_str = record["t_window_end"][:10]
    path = os.path.join(log_dir, f"{prefix}_{date_str}.csv")
    new = not os.path.exists(path)
    try:
        with open(path, "a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=GATE_LOG_FIELDS)
            if new:
                w.writeheader()
            w.writerow(record)
    except Exception as ex:
        log.error(f"[LOG] Gagal tulis gate log: {ex}")


def save_state(gate, last_t, log_dir):
    path = os.path.join(log_dir, GATE_STATE_FILE)
    data = {"saved_at": datetime.now().isoformat(timespec="seconds"),
            "last_t": last_t.isoformat() if last_t else None,
            "gate": gate.to_dict()}
    tmp = path + ".tmp"
    try:
        with open(tmp, "w") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp, path)
    except Exception as ex:
        log.error(f"[STATE] Gagal simpan state: {ex}")


def load_state(gate, log_dir):
    path = os.path.join(log_dir, GATE_STATE_FILE)
    if not os.path.exists(path):
        return None
    try:
        with open(path) as f:
            data = json.load(f)
        saved = _parse_ts(data.get("saved_at"))
        if saved is None or (datetime.now() - saved).total_seconds() > STATE_MAX_AGE_S:
            log.info("[STATE] State tersimpan usang - diabaikan")
            return None
        gate.from_dict(data.get("gate", {}))
        return _parse_ts(data.get("last_t")) if data.get("last_t") else None
    except Exception as ex:
        log.warning(f"[STATE] Gagal baca state: {ex}")
        return None

# ============================================================================
# 7. PENGIRIMAN EVENT
# ============================================================================

def dispatch(events, e, amp, gate_snapshot, tg_enabled=True):
    for ev, info in events:
        if ev == "DETECT":
            log.warning(f"[EVENT] DETECT {e['t']:%H:%M} (amp {_amp_txt(amp)})")
            if NOTIFY_DETECT:
                send_telegram(fmt_detect(e, amp), tg_enabled)
        elif ev == "WARNING":
            log.warning(f"[EVENT] >>> WARNING RESMI {e['t']:%H:%M} <<<")
            send_telegram(fmt_warning(e, amp, gate_snapshot), tg_enabled)
        elif ev == "BLOCKED":
            log.warning(f"[EVENT] EEMD L2 tertahan amplitudo {e['t']:%H:%M} "
                        f"({_amp_txt(amp)})")
            if NOTIFY_BLOCKED:
                send_telegram(fmt_blocked(e, amp), tg_enabled)
        elif ev == "CLEAR":
            log.info(f"[EVENT] CLEAR {e['t']:%H:%M}")
            if NOTIFY_CLEAR:
                send_telegram(fmt_clear(e, info), tg_enabled)

# ============================================================================
# 8. MODE REAL-TIME
# ============================================================================

class Health:
    def __init__(self):
        self.stale = {"EEMD": False, "AMPLITUDO": False}

    def check(self, name, age_s, detail):
        is_stale = age_s is None or age_s > STALE_S
        if is_stale and not self.stale[name]:
            self.stale[name] = True
            log.warning(f"[HEALTH] {name} basi ({detail})")
            if NOTIFY_HEALTH:
                send_telegram(fmt_health(name, False, detail))
        elif not is_stale and self.stale[name]:
            self.stale[name] = False
            log.info(f"[HEALTH] {name} pulih")
            if NOTIFY_HEALTH:
                send_telegram(fmt_health(name, True, detail))


def amp_status_age(amp_dir):
    path = os.path.join(amp_dir, AMP_STATUS_FILE)
    try:
        with open(path) as f:
            d = json.load(f)
        t = _parse_ts(d.get("t_written"))
        return (datetime.now() - t).total_seconds() if t else None
    except Exception:
        return None


def run_realtime(eemd_dir, amp_dir, gate_dir):
    log.info("=" * 70)
    log.info(f"U-TEWS GATE (EEMD x AMPLITUDO) - {SYSTEM_ID}")
    log.info("=" * 70)
    log.info(f"EEMD log : {eemd_dir}/{EEMD_LOG_PATTERN}")
    log.info(f"AMP log  : {amp_dir}/{AMP_LOG_PREFIX}_{{date}}.csv")
    log.info(f"Gate dir : {gate_dir}")
    log.info(f"Poll {POLL_S}s, tunggu amplitudo {AMP_WAIT_S}s, basi {STALE_S}s")

    gate = Gate()
    amp_log = AmpLog(amp_dir, AMP_LOG_PREFIX)
    health = Health()

    last_t = load_state(gate, gate_dir)
    if last_t is None:
        # Mulai dari baris EEMD terbaru (tidak memutar ulang riwayat)
        today = datetime.now().date()
        rows = read_eemd_rows(eemd_dir, [today - timedelta(days=1), today])
        if rows:
            last_t = rows[-1]["t"]
            gate.prev_hyst = rows[-1]["hyst_state"] or "IDLE"
            log.info(f"[INIT] Mulai setelah {last_t} (hyst={gate.prev_hyst})")
        else:
            last_t = datetime.now().replace(second=0, microsecond=0)
            log.info("[INIT] Log EEMD belum ada, menunggu data baru")
    else:
        log.info(f"[INIT] State dipulihkan, terakhir {last_t}")

    send_telegram(fmt_startup())

    while True:
        try:
            now = datetime.now()
            today = now.date()
            rows = read_eemd_rows(eemd_dir, [today - timedelta(days=1), today])
            new_rows = [r for r in rows if r["t"] > last_t]

            for e in new_rows:
                amp = amp_log.get(e["t"])
                if amp is None and (now - e["t"]).total_seconds() < AMP_WAIT_S:
                    break           # tunggu layer amplitudo, jaga urutan
                record, events = gate.update(e, amp)
                append_gate_log(record, gate_dir)
                log.info(f"[GATE] {e['t']:%H:%M} EEMD={record['eemd_hyst_state']}"
                         f"/L2={record['eemd_warning']} AMP={record['amp_status']} "
                         f"-> FINAL={record['final_warning']}"
                         + (f" [{record['events']}]" if record["events"] else ""))
                dispatch(events, e, amp, gate)
                last_t = e["t"]
                save_state(gate, last_t, gate_dir)

            # Kesehatan
            latest_eemd = rows[-1]["t"] if rows else None
            eemd_age = (now - latest_eemd).total_seconds() if latest_eemd else None
            health.check("EEMD", eemd_age,
                         f"Window terakhir: {latest_eemd:%Y-%m-%d %H:%M}"
                         if latest_eemd else "Log detection belum ada")
            a_age = amp_status_age(amp_dir)
            health.check("AMPLITUDO", a_age,
                         f"Status terakhir {a_age:.0f} detik lalu"
                         if a_age is not None else "status_amplitude.json tidak terbaca")

        except Exception as ex:
            log.error(f"[LOOP] Error: {ex}\n{traceback.format_exc()}")

        time.sleep(POLL_S)

# ============================================================================
# 9. MODE REPLAY (EVALUASI)
# ============================================================================

def run_replay(eemd_dir, amp_dir, gate_dir, start_str, end_str, amp_prefix):
    t0 = datetime.fromisoformat(start_str)
    t1 = datetime.fromisoformat(end_str)
    dates, d = [], t0.date()
    while d <= t1.date():
        dates.append(d)
        d += timedelta(days=1)

    rows = [r for r in read_eemd_rows(eemd_dir, dates) if t0 <= r["t"] <= t1]
    amp_log = AmpLog(amp_dir, amp_prefix)
    gate = Gate()
    episodes, cur = [], None
    n_missing = 0

    log.info(f"[REPLAY] {t0} s/d {t1}: {len(rows)} window EEMD, amp prefix '{amp_prefix}'")
    for e in rows:
        amp = amp_log.get(e["t"])
        n_missing += amp is None
        record, events = gate.update(e, amp)
        append_gate_log(record, gate_dir, prefix="gate_replay")
        for ev, info in events:
            if ev == "DETECT":
                cur = {"t_first_detect": e["t"], "t_eemd_warning": None,
                       "t_final_warning": None, "n_blocked": 0}
            if ev in ("WARNING", "BLOCKED") or record["eemd_warning"]:
                log.info(f"[REPLAY] {e['t']:%Y-%m-%d %H:%M} {ev or ''} "
                         f"EEMD_L2={record['eemd_warning']} AMP={record['amp_status']} "
                         f"range={record['amp_range_m']}")
        if cur is not None:
            cur["t_eemd_warning"] = gate.t_eemd_warning or cur["t_eemd_warning"]
            cur["t_final_warning"] = gate.t_final_warning or cur["t_final_warning"]
            cur["n_blocked"] = max(cur["n_blocked"], gate.n_blocked)
        for ev, info in events:
            if ev == "CLEAR" and cur is not None:
                cur.update({"t_eemd_warning": info["t_eemd_warning"],
                            "t_final_warning": info["t_final_warning"],
                            "n_blocked": info["n_blocked"], "t_clear": e["t"]})
                episodes.append(cur)
                cur = None
    if cur is not None:
        cur["t_clear"] = None
        episodes.append(cur)

    def dm(a, b):
        return f"{(a - b).total_seconds() / 60:.1f}" if a and b else "-"

    log.info("-" * 70)
    log.info(f"[REPLAY] Episode ACTIVE: {len(episodes)} | window tanpa data amplitudo: {n_missing}")
    hm = lambda t: f"{t:%H:%M}" if t else "-"
    for i, ep in enumerate(episodes, 1):
        log.info(f"  #{i} deteksi awal {ep['t_first_detect']:%Y-%m-%d %H:%M} | "
                 f"EEMD L2 {hm(ep['t_eemd_warning'])} | "
                 f"WARNING RESMI {hm(ep['t_final_warning']) if ep['t_final_warning'] else 'tidak terbit'} | "
                 f"clear {hm(ep.get('t_clear'))}")
        log.info(f"     tambahan delay gate vs EEMD L2: "
                 f"{dm(ep['t_final_warning'], ep['t_eemd_warning'])} menit | "
                 f"window tertahan: {ep['n_blocked']}")
    log.info(f"[REPLAY] Log: {gate_dir}/gate_replay_*.csv")
    return episodes

# ============================================================================
# 10. ENTRY POINT
# ============================================================================

def main():
    global log
    ap = argparse.ArgumentParser(description="U-TEWS gerbang peringatan EEMD x amplitudo")
    ap.add_argument("--eemd-dir", default=EEMD_LOG_DIR)
    ap.add_argument("--amp-dir", default=AMP_LOG_DIR)
    ap.add_argument("--gate-dir", default=GATE_LOG_DIR)
    ap.add_argument("--replay", nargs=2, metavar=("MULAI", "SELESAI"))
    ap.add_argument("--amp-prefix", default=AMP_LOG_PREFIX,
                    help="prefix log amplitudo untuk replay (amplitude / replay)")
    args = ap.parse_args()

    log = setup_logging(args.gate_dir)
    try:
        if args.replay:
            run_replay(args.eemd_dir, args.amp_dir, args.gate_dir,
                       *args.replay, args.amp_prefix)
        else:
            run_realtime(args.eemd_dir, args.amp_dir, args.gate_dir)
    except KeyboardInterrupt:
        log.info("[EXIT] Dihentikan user")
    except Exception as ex:
        log.error(f"[FATAL] {ex}\n{traceback.format_exc()}")
        sys.exit(1)


if __name__ == "__main__":
    main()
