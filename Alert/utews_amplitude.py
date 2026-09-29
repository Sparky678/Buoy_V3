#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==============================================================================
U-TEWS - LAYER PEMANTAUAN AMPLITUDO 3 MENIT
Orange Pi 5 - Pulau Sebesi, Lampung
==============================================================================
Program terpisah dari utews_realtime.py (EEMD). Mengamati level air MENTAH
selama periode pengawasan 3 menit dan menilai apakah perubahannya "tetap"
(relatif kecil). Hasilnya ditulis ke file status JSON untuk gerbang AND:
peringatan resmi hanya keluar bila EEMD DAN layer ini sama-sama konfirmasi.

Arsitektur:
  water_monitoring_10hz.py -> water_raw_YYYY-MM-DD.csv (10 Hz)
       |
       v
  Tail-read 3 menit terakhir -> water_level / 100 (m) -> bin 1 detik (mean)
       -> range = max - min (180 bin) -> range <= ambang ? CONFIRM : REJECT
       -> status_amplitude.json (atomik) + log CSV harian

Status keluaran:
  CONFIRM : coverage cukup dan range <= THRESHOLD_RANGE  ("tetap")
  REJECT  : coverage cukup dan range >  THRESHOLD_RANGE  ("tidak tetap")
  NO_DATA : file tidak ada / coverage < MIN_COVERAGE (bukan konfirmasi)

Kalibrasi ambang (data normal 11-17 Maret 2025, 6.204 window 3 menit):
  range normal: median 0,17 m | P99 0,32 m | max 0,43 m
  THRESHOLD_RANGE = k x max = 1,5 x 0,43 = 0,645 m  (0 window normal melebihi)

Catatan desain (sesuai kesepakatan):
  - Sumber data CSV RAW, bukan processed; tidak memakai quality_flag.
  - Tidak ada penyaringan kode error 65535 maupun deteksi sensor macet.
    Nilai non-numerik/kosong hanya dikonversi ke NaN agar parsing tidak crash.
    Kode error 65535 (-> 655,35 m) akan melonjakkan range sehingga window
    tersebut berstatus REJECT.
  - Evaluasi tiap 1 menit, t_end di batas menit (sama dengan tick EEMD),
    window [t_end - 180 s, t_end). Dijalankan EVAL_DELAY_S detik setelah
    batas menit agar batch CSV terakhir sudah ter-flush.

Mode:
  python3 utews_amplitude.py                         # real-time (service)
  python3 utews_amplitude.py --once                  # satu evaluasi, sekarang
  python3 utews_amplitude.py --once --t-end "2026-09-23 12:05"
  python3 utews_amplitude.py --replay "2026-09-20 00:00" "2026-09-21 00:00"
      (evaluasi ulang tiap menit pada data historis -> log replay_*.csv,
       untuk pengujian dengan data simulasi tsunami)

Contoh service systemd (/etc/systemd/system/utews-amplitude.service):
  [Unit]
  Description=U-TEWS Layer Amplitudo 3 Menit
  After=network.target

  [Service]
  ExecStart=/usr/bin/python3 /home/orangepi/utews/utews_amplitude.py
  WorkingDirectory=/home/orangepi/utews
  Restart=always
  RestartSec=5
  User=orangepi

  [Install]
  WantedBy=multi-user.target

Penulis : Aryo (UNILA), 2026
==============================================================================
"""

import io
import os
import sys
import csv
import json
import time
import argparse
import logging
import traceback
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

# ============================================================================
# 1. KONFIGURASI
# ============================================================================

# --- PATHS ---
DATA_DIR             = "/home/orangepi/data/water"            # dari water_monitoring_10hz.py
LOG_DIR              = "/home/orangepi/data/utews_amplitude"  # log + status layer ini
STATUS_FILE_NAME     = "status_amplitude.json"                # dibaca gerbang AND

# --- CSV RAW (water_monitoring_10hz.py) ---
RAW_FILENAME_PATTERN = "water_raw_{date}.csv"                 # date = YYYY-MM-DD
COL_TIMESTAMP        = "timestamp"
COL_LEVEL            = "water_level"
LEVEL_SCALE          = 0.01             # raw / 100 -> meter (sama dgn script akuisisi)
TIMESTAMP_FORMAT     = "%Y-%m-%d %H:%M:%S.%f"

# --- PERIODE PENGAWASAN ---
WINDOW_S             = 180              # 3 menit
BIN_S                = 1                # resample 1 detik (mean 10 sampel)
N_BINS               = WINDOW_S // BIN_S
STEP_S               = 60               # evaluasi tiap 1 menit
MIN_COVERAGE         = 0.95             # minimal 171 dari 180 bin terisi
EVAL_DELAY_S         = 2                # jeda setelah batas menit (flush CSV)

# --- AMBANG (Kalibrasi 7 hari, 11-17 Maret 2025) ---
CALIB_MAX_RANGE_M    = 0.43             # max range 3 menit kondisi normal
K_FACTOR             = 1.5              # sama dengan faktor threshold EEMD
THRESHOLD_RANGE      = K_FACTOR * CALIB_MAX_RANGE_M          # 0.645 m

# --- TAIL READ (real-time) ---
# 10 Hz x ~100 byte/baris ~ 1 kB/detik. 2 MB ~ 30+ menit data, jauh di atas
# kebutuhan 3 menit, aman meski panjang baris bervariasi.
TAIL_BYTES           = 2 * 1024 * 1024

# --- IDENTITAS ---
SYSTEM_ID            = "U-TEWS Pulau Sebesi"
LAYER_ID             = "amplitude_3min"

# ============================================================================
# 2. LOGGING
# ============================================================================

def setup_logging(log_dir):
    os.makedirs(log_dir, exist_ok=True)
    logger = logging.getLogger("utews_amp")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)
    fh = logging.FileHandler(os.path.join(
        log_dir, f"utews_amplitude_{datetime.now():%Y-%m-%d}.log"))
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    return logger


log = logging.getLogger("utews_amp")

# ============================================================================
# 3. PEMBACAAN CSV RAW
# ============================================================================

def raw_path(data_dir, date):
    return os.path.join(data_dir,
                        RAW_FILENAME_PATTERN.format(date=date.strftime("%Y-%m-%d")))


def dates_in_range(t_start, t_end):
    """Tanggal file yang menutupi [t_start, t_end). Loop pakai cur_date."""
    out = []
    cur = t_start.date()
    last = (t_end - timedelta(microseconds=1)).date()
    while cur <= last:
        out.append(cur)
        cur += timedelta(days=1)
    return out


def parse_rows(header_line, body_text):
    """Parse teks CSV -> DataFrame [ts, wl_m]. Baris rusak/parsial dibuang."""
    cols = next(csv.reader([header_line.strip()]))
    if COL_TIMESTAMP not in cols or COL_LEVEL not in cols:
        raise ValueError(f"Kolom hilang. Ada: {cols}")
    if not body_text.strip():
        return pd.DataFrame(columns=["ts", "wl_m"])

    df = pd.read_csv(io.StringIO(body_text), names=cols, header=None,
                     usecols=[COL_TIMESTAMP, COL_LEVEL], dtype=str,
                     on_bad_lines="skip", engine="c")
    ts = pd.to_datetime(df[COL_TIMESTAMP], format=TIMESTAMP_FORMAT, errors="coerce")
    wl = pd.to_numeric(df[COL_LEVEL], errors="coerce") * LEVEL_SCALE
    out = pd.DataFrame({"ts": ts, "wl_m": wl}).dropna()
    return out


def read_tail(path, nbytes=TAIL_BYTES):
    """
    Baca header + ekor file (nbytes terakhir) tanpa memuat seluruh file.
    Returns (header, body, is_full) - is_full True bila seluruh isi terbaca.
    """
    size = os.path.getsize(path)
    with open(path, "rb") as f:
        header = f.readline().decode("utf-8", errors="replace")
        start_body = f.tell()
        is_full = size - start_body <= nbytes
        if not is_full:
            f.seek(size - nbytes)
            f.readline()                       # buang baris parsial pertama
        body = f.read().decode("utf-8", errors="replace")
    return header, body, is_full


def read_tail_covering(path, t_start, nbytes=TAIL_BYTES):
    """
    Tail-read adaptif: perbesar ekor (x4) sampai data paling awal yang
    terbaca <= t_start, atau seluruh file sudah terbaca. Menjamin awal
    window tidak terpotong walau panjang baris lebih besar dari perkiraan.
    """
    while True:
        header, body, is_full = read_tail(path, nbytes)
        df = parse_rows(header, body)
        if is_full or (len(df) > 0 and df["ts"].iloc[0] <= t_start):
            return df
        nbytes *= 4


def read_full(path):
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        header = f.readline()
        body = f.read()
    return header, body


class RawSource:
    """
    Penyedia data mentah.
      mode "tail"  : real-time, hanya membaca ekor file (hemat CPU/IO)
      mode "full"  : replay historis, file dibaca penuh sekali lalu di-cache
    """

    def __init__(self, data_dir, mode="tail"):
        self.data_dir = data_dir
        self.mode = mode
        self._cache = {}

    def _load(self, date, t_start):
        path = raw_path(self.data_dir, date)
        if not os.path.exists(path):
            return None
        if self.mode == "full":
            if date not in self._cache:
                log.info(f"[CSV] Memuat penuh {os.path.basename(path)} ...")
                self._cache[date] = parse_rows(*read_full(path))
            return self._cache[date]
        return read_tail_covering(path, t_start)

    def get(self, t_start, t_end):
        frames, missing = [], []
        for d in dates_in_range(t_start, t_end):
            try:
                df = self._load(d, t_start)
            except Exception as e:
                log.warning(f"[CSV] Gagal baca file {d}: {e}")
                df = None
            if df is None:
                missing.append(str(d))
                continue
            frames.append(df[(df["ts"] >= t_start) & (df["ts"] < t_end)])
        if missing:
            log.warning(f"[CSV] File raw tidak ada: {', '.join(missing)}")
        if not frames:
            return pd.DataFrame(columns=["ts", "wl_m"])
        return pd.concat(frames, ignore_index=True)

# ============================================================================
# 4. EVALUASI SATU PERIODE 3 MENIT
# ============================================================================

def evaluate_window(source, t_end, threshold=THRESHOLD_RANGE):
    """
    Evaluasi window [t_end - WINDOW_S, t_end).
    Returns dict hasil (selalu ada, status CONFIRM / REJECT / NO_DATA).
    """
    t0 = time.monotonic()
    t_end = pd.Timestamp(t_end)
    t_start = t_end - pd.Timedelta(seconds=WINDOW_S)

    raw = source.get(t_start, t_end)
    n_samples = len(raw)

    res = {
        "t_window_start": t_start,
        "t_window_end"  : t_end,
        "status"        : "NO_DATA",
        "confirm"       : False,
        "range_m"       : np.nan,
        "threshold_m"   : threshold,
        "std_m"         : np.nan,
        "max_step_m"    : np.nan,
        "min_m"         : np.nan,
        "max_m"         : np.nan,
        "n_bins"        : 0,
        "coverage_pct"  : 0.0,
        "n_samples"     : n_samples,
        "t_proc_s"      : 0.0,
    }

    if n_samples > 0:
        # Bin 1 detik: mean sampel dalam tiap detik, grid penuh 180 bin
        grid = pd.date_range(t_start, periods=N_BINS, freq=f"{BIN_S}s")
        binned = (raw.set_index("ts")["wl_m"]
                     .resample(f"{BIN_S}s").mean()
                     .reindex(grid))
        vals = binned.dropna()
        n_bins = len(vals)
        coverage = n_bins / N_BINS

        res["n_bins"] = n_bins
        res["coverage_pct"] = round(coverage * 100, 2)

        if coverage >= MIN_COVERAGE:
            v = vals.values
            rng = float(v.max() - v.min())
            res.update({
                "range_m"   : rng,
                "std_m"     : float(np.std(v)),
                "max_step_m": float(np.nanmax(np.abs(np.diff(binned.values))))
                              if n_bins > 1 else 0.0,
                "min_m"     : float(v.min()),
                "max_m"     : float(v.max()),
            })
            if rng <= threshold:
                res["status"], res["confirm"] = "CONFIRM", True
            else:
                res["status"], res["confirm"] = "REJECT", False
        else:
            log.warning(f"[DATA] Coverage {coverage*100:.1f}% "
                        f"({n_bins}/{N_BINS} bin) < {MIN_COVERAGE*100:.0f}%")

    res["t_proc_s"] = round(time.monotonic() - t0, 3)
    return res

# ============================================================================
# 5. KELUARAN: STATUS JSON + LOG CSV
# ============================================================================

LOG_FIELDS = ["t_window_end", "status", "confirm", "range_m", "threshold_m",
              "std_m", "max_step_m", "min_m", "max_m", "n_bins",
              "coverage_pct", "n_samples", "t_proc_s"]


def _fmt(v, nd=4):
    if isinstance(v, float):
        return "" if np.isnan(v) else round(v, nd)
    return v


def write_status(res, log_dir):
    """Tulis status JSON secara atomik (tmp + os.replace)."""
    payload = {
        "layer"          : LAYER_ID,
        "system"         : SYSTEM_ID,
        "t_written"      : datetime.now().isoformat(timespec="seconds"),
        "t_window_start" : res["t_window_start"].isoformat(),
        "t_window_end"   : res["t_window_end"].isoformat(),
        "status"         : res["status"],
        "confirm"        : bool(res["confirm"]),
        "range_m"        : _fmt(res["range_m"]) if res["range_m"] == res["range_m"] else None,
        "threshold_m"    : round(res["threshold_m"], 4),
        "coverage_pct"   : res["coverage_pct"],
        "window_s"       : WINDOW_S,
    }
    path = os.path.join(log_dir, STATUS_FILE_NAME)
    tmp = path + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except Exception as e:
        log.error(f"[STATUS] Gagal tulis status: {e}")


def append_log(res, log_dir, prefix="amplitude"):
    date_str = res["t_window_end"].strftime("%Y-%m-%d")
    path = os.path.join(log_dir, f"{prefix}_{date_str}.csv")
    new = not os.path.exists(path)
    row = {k: _fmt(res[k]) for k in LOG_FIELDS}
    row["t_window_end"] = res["t_window_end"].isoformat()
    try:
        with open(path, "a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=LOG_FIELDS)
            if new:
                w.writeheader()
            w.writerow(row)
    except Exception as e:
        log.error(f"[LOG] Gagal tulis log: {e}")


def log_result(res):
    if res["status"] == "NO_DATA":
        log.info(f"[AMP] {res['t_window_end']:%Y-%m-%d %H:%M} NO_DATA "
                 f"(coverage {res['coverage_pct']:.1f}%, {res['n_samples']} sampel)")
        return
    log.info(f"[AMP] {res['t_window_end']:%Y-%m-%d %H:%M} {res['status']:<7} "
             f"range={res['range_m']:.3f} m (ambang {res['threshold_m']:.3f}) "
             f"std={res['std_m']:.4f} step={res['max_step_m']:.3f} "
             f"cov={res['coverage_pct']:.1f}% t={res['t_proc_s']:.2f}s")

# ============================================================================
# 6. MODE REAL-TIME
# ============================================================================

def sleep_until_next_eval():
    now = datetime.now()
    target = now.replace(second=0, microsecond=0) + timedelta(seconds=EVAL_DELAY_S)
    if target <= now:
        target += timedelta(minutes=1)
    time.sleep(max(0.0, (target - datetime.now()).total_seconds()))


def run_realtime(data_dir, log_dir):
    log.info("=" * 70)
    log.info(f"U-TEWS LAYER AMPLITUDO 3 MENIT - {SYSTEM_ID}")
    log.info("=" * 70)
    log.info(f"Sumber   : {data_dir}/{RAW_FILENAME_PATTERN}")
    log.info(f"Window   : {WINDOW_S}s, bin {BIN_S}s, step {STEP_S}s, "
             f"min coverage {MIN_COVERAGE*100:.0f}%")
    log.info(f"Ambang   : {THRESHOLD_RANGE:.3f} m (= {K_FACTOR} x {CALIB_MAX_RANGE_M} m)")
    log.info(f"Status   : {os.path.join(log_dir, STATUS_FILE_NAME)}")

    source = RawSource(data_dir, mode="tail")
    prev_status = None
    n_eval = 0

    while True:
        sleep_until_next_eval()
        t_end = pd.Timestamp(datetime.now().replace(second=0, microsecond=0))
        try:
            res = evaluate_window(source, t_end)
            write_status(res, log_dir)
            append_log(res, log_dir)
            log_result(res)
            if prev_status is not None and res["status"] != prev_status:
                log.warning(f"[EVENT] Status berubah {prev_status} -> {res['status']}")
            prev_status = res["status"]
            n_eval += 1
        except Exception as e:
            log.error(f"[TICK] Error: {e}\n{traceback.format_exc()}")

# ============================================================================
# 7. MODE ONCE & REPLAY
# ============================================================================

def run_once(data_dir, log_dir, t_end_str=None):
    if t_end_str:
        t_end = pd.Timestamp(t_end_str).floor("min")
        source = RawSource(data_dir, mode="full")
    else:
        t_end = pd.Timestamp(datetime.now().replace(second=0, microsecond=0))
        source = RawSource(data_dir, mode="tail")
    res = evaluate_window(source, t_end)
    log_result(res)
    return res


def run_replay(data_dir, log_dir, start_str, end_str):
    """Evaluasi tiap menit pada rentang historis. Tidak menulis status JSON."""
    t = pd.Timestamp(start_str).floor("min")
    t_stop = pd.Timestamp(end_str).floor("min")
    source = RawSource(data_dir, mode="full")
    counts = {"CONFIRM": 0, "REJECT": 0, "NO_DATA": 0}
    ranges = []

    log.info(f"[REPLAY] {t} s/d {t_stop}")
    while t <= t_stop:
        res = evaluate_window(source, t)
        append_log(res, log_dir, prefix="replay")
        counts[res["status"]] += 1
        if res["status"] != "NO_DATA":
            ranges.append(res["range_m"])
        if res["status"] == "REJECT":
            log_result(res)
        t += pd.Timedelta(seconds=STEP_S)

    total = sum(counts.values())
    log.info("-" * 60)
    log.info(f"[REPLAY] Selesai: {total} window | CONFIRM {counts['CONFIRM']} | "
             f"REJECT {counts['REJECT']} | NO_DATA {counts['NO_DATA']}")
    if ranges:
        r = np.array(ranges)
        log.info(f"[REPLAY] Range: median {np.median(r):.3f} m, "
                 f"P99 {np.percentile(r, 99):.3f} m, max {r.max():.3f} m")
    log.info(f"[REPLAY] Log: {log_dir}/replay_*.csv")
    return counts

# ============================================================================
# 8. ENTRY POINT
# ============================================================================

def main():
    global log
    ap = argparse.ArgumentParser(description="U-TEWS layer amplitudo 3 menit")
    ap.add_argument("--data-dir", default=DATA_DIR)
    ap.add_argument("--log-dir", default=LOG_DIR)
    ap.add_argument("--once", action="store_true", help="satu evaluasi lalu keluar")
    ap.add_argument("--t-end", help="akhir window untuk --once (YYYY-MM-DD HH:MM)")
    ap.add_argument("--replay", nargs=2, metavar=("MULAI", "SELESAI"),
                    help="evaluasi ulang tiap menit pada data historis")
    args = ap.parse_args()

    log = setup_logging(args.log_dir)

    try:
        if args.replay:
            run_replay(args.data_dir, args.log_dir, *args.replay)
        elif args.once:
            run_once(args.data_dir, args.log_dir, args.t_end)
        else:
            run_realtime(args.data_dir, args.log_dir)
    except KeyboardInterrupt:
        log.info("[EXIT] Dihentikan user")
    except Exception as e:
        log.error(f"[FATAL] {e}\n{traceback.format_exc()}")
        sys.exit(1)


if __name__ == "__main__":
    main()
