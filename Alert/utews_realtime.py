#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==============================================================================
U-TEWS REAL-TIME EEMD TSUNAMI DETECTION PIPELINE
Orange Pi 5 - Pulau Sebesi, Lampung
==============================================================================
Sistem deteksi dini tsunami berbasis EEMD W-REF (Wang et al., 2020) dengan
Dual-Layer Warning System (Hysteresis + N_CONFIRM persistence) dan notifikasi
via Telegram bot.

Arsitektur:
  Thread A (eksternal, water_monitoring_10hz.py) -> menulis CSV processed 10 Hz
       |
       v
  CSV  ->  Reader (1 menit) -> Buffer 3 jam -> EEMD W-REF -> Hysteresis L1 -> Warning L2 -> Telegram
                                                                                    v
                                                                          Detection log CSV

Referensi:
  [1] Wang et al. (2020) - Seismological Research Letters, doi:10.1785/0220200115
  [2] Wu & Huang (2009)  - Advances in Adaptive Data Analysis, Vol.1, No.1
  [3] Huang et al. (1998) - Proceedings Royal Society A, 454, 903-995
  [4] Laporan Progres v7  - Konfigurasi final W-REF + Hysteresis (alpha=0.5) + L2 (N=3)

Revisi (gerbang AND dengan layer amplitudo 3 menit):
  - Program ini TIDAK lagi mengirim Telegram (mode dry-run). Semua pesan
    hanya dicatat di log sebagai [TG-DRYRUN]. Pengiriman peringatan resmi
    dipindahkan ke utews_gate.py, yang membaca detection_YYYY-MM-DD.csv
    program ini dan status layer amplitudo (utews_amplitude.py).
  - Logika deteksi (EEMD, hysteresis, N_CONFIRM) dan format detection log
    TIDAK berubah, sehingga hasil evaluasi Bab IV tetap berlaku.
  - Perbaikan: urutan scheduler saat window di-skip, guard sinyal datar /
    nol IMF, durasi episode pada event CLEAR, komentar parameter.

Penulis : Aryo (UNILA), 2026
Lisensi : Akademik, untuk keperluan skripsi
==============================================================================
"""

import os
import sys
import csv
import time
import glob
import logging
import warnings
import traceback
from datetime import datetime, timedelta
from pathlib import Path

warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd
import requests
from scipy.signal import hilbert
from PyEMD import EEMD as PyEEMD

# ============================================================================
# 1. KONFIGURASI - SUMBER TUNGGAL KEBENARAN
# ============================================================================
# Semua parameter di sini SINKRON dengan eemd_sliding_window_claude_5.py
# dan Laporan Progres v7 Sec.12.9 (Konfigurasi Final W-REF Pulau Sebesi).
# ----------------------------------------------------------------------------

# --- PATHS ---
DATA_DIR        = "/home/orangepi/data/water"             # dari water_monitoring_10hz.py
LOG_DIR         = "/home/orangepi/data/utews_realtime"    # output detection log
STATE_FILE      = os.path.join(LOG_DIR, "state.json")     # untuk persistence antar restart

# --- WINDOW W-REF (Wang et al. 2020 - replikasi penuh) ---
WINDOW_HOURS         = 3            # window length: 3 jam
WINDOW_MINUTES       = WINDOW_HOURS * 60  # = 180 menit = 180 sampel @ 1 menit
RESAMPLE_SECONDS     = 60           # resample target: 60 detik (1 menit)
STEP_MINUTES         = 1            # step sliding: 1 menit (responsivitas maksimal)
N_PTS_PER_WINDOW     = WINDOW_MINUTES  # 180 titik per window

# --- EEMD (Wu & Huang 2009) ---
EEMD_NOISE_RATIO     = 0.2          # 20% std sinyal
EEMD_N_ENSEMBLE      = 100          # 100 trials ensemble
EEMD_RANDOM_SEED     = 42           # reproducibility
EEMD_TIMEOUT_S       = 50           # safety: skip window kalau > 50 detik

# --- BAND TSUNAMI ---
TSUNAMI_BAND_MIN_S   = 300          # 5 menit
TSUNAMI_BAND_MAX_S   = 3600         # 60 menit
# CATATAN: band 5-60 menit (300-3600 s) sesuai IOC-UNESCO / klasifikasi NIWA
# yang dipakai di skripsi (Subbab 2.2, 3.5, 4.4). Komentar lama yang menyebut
# 3600 s = "120 menit" dan menyarankan 7200 s tidak sesuai dengan dasar
# metodologi skripsi; nilai 3600 s dipertahankan.

# --- THRESHOLD (Kalibrasi 7 hari, 11-17 Maret 2025) ---
# Sumber: Laporan v7 Sec.3, Tabel 3
# threshold_trigger = k x max(lm_amp_sum)   dengan k=1.5
THRESHOLD_TRIGGER    = 0.052110 * 1.5     # m - trigger ACTIVE = 0.078165 m
ALPHA_HOLD           = 0.6          # rasio hysteresis (sesuai kode v5)
THRESHOLD_HOLD       = ALPHA_HOLD * THRESHOLD_TRIGGER   # 0.046899 m

# --- WARNING STATE MACHINE (Layer 2) ---
N_CONFIRM            = 3            # 3 window ACTIVE berurutan -> WARNING

# --- LOOP SCHEDULER ---
LOOP_PERIOD_S        = 60           # cek/proses tiap 60 detik

# --- TELEGRAM ---
# SENGAJA placeholder: program ini berjalan dry-run (pesan hanya dicatat di
# log). Peringatan resmi dikirim oleh utews_gate.py setelah gerbang AND
# (EEMD L2 DAN layer amplitudo). Token asli dipindahkan ke utews_gate.py.
# Untuk kembali ke mode lama (tanpa gerbang), isi token & chat_id asli.
TELEGRAM_BOT_TOKEN   = "PLACEHOLDER_TOKEN"
TELEGRAM_CHAT_ID     = "PLACEHOLDER_CHAT_ID"
TELEGRAM_API_URL     = "https://api.telegram.org/bot{token}/sendMessage"
TELEGRAM_TIMEOUT_S   = 10
TELEGRAM_MAX_RETRIES = 3

# --- CSV INPUT (dari water_monitoring_10hz.py) ---
CSV_TIMESTAMP_COL    = "timestamp"
CSV_VALUE_COL        = "WaterLevel_m"
CSV_FLAG_COL         = "quality_flag"   # kolom QC rev2 (opsional; file lama tak punya)
CSV_FILENAME_PATTERN = "water_processed_{date}.csv"  # date = YYYY-MM-DD

# Flag QC yang nilainya BOLEH masuk buffer EEMD. Hanya pengukuran nyata:
#   OK       = pembacaan sah
#   SMOOTHED = spike dirata-rata (masih berbasis pengukuran)
#   CLAMPED  = overshoot dipotong ke batas fisik (masih pengukuran)
# DITOLAK untuk EEMD: HELD (last-value buatan), STALE/MISSING/REJECTED (None).
# Bila kolom flag tidak ada (file lama), filter ini dilewati otomatis.
CSV_FLAG_ACCEPT      = {"OK", "SMOOTHED", "CLAMPED"}

# --- IDENTITAS SISTEM ---
SYSTEM_ID            = "U-TEWS Pulau Sebesi"
SYSTEM_LOCATION      = "Lampung, Indonesia"
SYSTEM_HARDWARE      = "Orange Pi 5 (RK3588, 8GB)"

# ============================================================================
# 2. LOGGING - KONSOL + FILE
# ============================================================================

def setup_logging():
    """Setup logger: tulis ke stdout + file harian rotating manual."""
    os.makedirs(LOG_DIR, exist_ok=True)

    logger = logging.getLogger("utews")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()  # hindari duplikasi handler kalau modul di-reload

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Konsol (untuk journalctl -u utews)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    # File harian - rotasi dengan tanggal hari ini
    log_path = os.path.join(LOG_DIR, f"utews_{datetime.now():%Y-%m-%d}.log")
    fh = logging.FileHandler(log_path)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger


log = setup_logging()

# ============================================================================
# 3. CSV READER & RESAMPLER - 10 Hz -> 1 menit
# ============================================================================

def get_csv_files_in_range(t_start, t_end):
    """
    Dapatkan list CSV processed yang menutupi rentang waktu [t_start, t_end].
    File harian: water_processed_YYYY-MM-DD.csv.
    """
    files = []
    cur_date = t_start.date()
    end_date = t_end.date()
    while cur_date <= end_date:
        # BUGFIX (lihat CATATAN di bawah): filename HARUS dibentuk dari
        # cur_date (tanggal yang sedang di-loop), BUKAN dari waktu sistem
        # saat ini (time.strftime tanpa argumen = now()). Versi lama selalu
        # menghasilkan nama file hari-ini untuk setiap iterasi, sehingga
        # file H-1 (kemarin) tidak pernah ikut terbaca saat window sliding
        # 3 jam melewati tengah malam.
        fname = CSV_FILENAME_PATTERN.format(date=cur_date.strftime("%Y-%m-%d"))
        fpath = os.path.join(DATA_DIR, fname)
        if os.path.exists(fpath):
            files.append(fpath)
        cur_date += timedelta(days=1)
    return files


def load_and_resample_window(t_end, window_hours=WINDOW_HOURS,
                              resample_s=RESAMPLE_SECONDS):
    """
    Baca CSV processed harian, ambil rentang [t_end - window_hours, t_end],
    resample ke 1 menit dengan MEAN per bin (anti-aliasing alami).

    Returns:
        (ts: pd.DatetimeIndex, wl: np.ndarray, t_numeric: np.ndarray, fs: float)
        atau (None, None, None, None) jika data tidak cukup.

    Catatan implementasi:
      - Baca multi-file kalau window menyebrang tengah malam (1 atau 2 file)
      - Mean per bin via pandas .resample(f"{resample_s}s").mean()
      - Drop NaN bin (saat sensor down), tapi kalau coverage < 95%, return None
    """
    t_start = t_end - pd.Timedelta(hours=window_hours)

    files = get_csv_files_in_range(t_start, t_end)
    if not files:
        log.warning(f"[CSV] Tidak ada file processed untuk rentang "
                    f"{t_start} - {t_end}")
        return None, None, None, None

    # Baca & gabung
    dfs = []
    for f in files:
        try:
            df = pd.read_csv(f)
            dfs.append(df)
        except Exception as e:
            log.warning(f"[CSV] Gagal baca {f}: {e}")
            continue

    if not dfs:
        return None, None, None, None

    df = pd.concat(dfs, ignore_index=True)

    # Validasi kolom
    if CSV_TIMESTAMP_COL not in df.columns or CSV_VALUE_COL not in df.columns:
        log.error(f"[CSV] Kolom hilang. Ada: {df.columns.tolist()}, "
                  f"perlu: {CSV_TIMESTAMP_COL}, {CSV_VALUE_COL}")
        return None, None, None, None

    # Parse timestamp & sort
    df[CSV_TIMESTAMP_COL] = pd.to_datetime(df[CSV_TIMESTAMP_COL])

    # --- FILTER KUALITAS (QC rev2) ---
    # Bila kolom quality_flag tersedia, HANYA terima pengukuran nyata
    # (OK/SMOOTHED/CLAMPED). Tolak HELD/STALE/MISSING/REJECTED agar nilai
    # last-value-hold buatan tidak menekan variasi sinyal & merusak amplitudo
    # IMF. File lama tanpa kolom flag dilewati (backward-compatible).
    if CSV_FLAG_COL in df.columns:
        n_before = len(df)
        df = df[df[CSV_FLAG_COL].astype(str).str.upper().isin(CSV_FLAG_ACCEPT)]
        n_rejected = n_before - len(df)
        if n_rejected > 0:
            log.info(f"[CSV] QC filter: {n_rejected}/{n_before} sampel ditolak "
                     f"(non-{sorted(CSV_FLAG_ACCEPT)})")

    # --- KOERSI NUMERIK AMAN ---
    # to_numeric(errors='coerce') mengubah string kosong / nilai non-numerik
    # nyasar menjadi NaN, BUKAN melempar ValueError di .astype() hilir.
    # Ini mencegah satu nilai rusak menghentikan seluruh window deteksi.
    df[CSV_VALUE_COL] = pd.to_numeric(df[CSV_VALUE_COL], errors="coerce")

    df = (df.dropna(subset=[CSV_VALUE_COL])
            .sort_values(CSV_TIMESTAMP_COL)
            .reset_index(drop=True))

    # Filter ke rentang window
    mask = (df[CSV_TIMESTAMP_COL] >= t_start) & (df[CSV_TIMESTAMP_COL] <= t_end)
    df_win = df[mask].copy()

    if len(df_win) == 0:
        log.warning(f"[CSV] Data dalam rentang [{t_start}, {t_end}] kosong")
        return None, None, None, None

    # Resample MEAN per bin - anti-aliasing alami
    df_win = df_win.set_index(CSV_TIMESTAMP_COL)
    resampled = (df_win[CSV_VALUE_COL]
                 .resample(f"{resample_s}s")
                 .mean()
                 .dropna())

    # Cek coverage: harus minimal 95% dari N_PTS_PER_WINDOW
    n_expected = int(window_hours * 3600 / resample_s)
    coverage   = len(resampled) / n_expected if n_expected > 0 else 0.0

    if coverage < 0.95:
        log.warning(f"[CSV] Coverage hanya {coverage*100:.1f}% "
                    f"({len(resampled)}/{n_expected} bins) - skip window")
        return None, None, None, None

    # Ambil tepat N_PTS_PER_WINDOW titik terakhir
    if len(resampled) > N_PTS_PER_WINDOW:
        resampled = resampled.iloc[-N_PTS_PER_WINDOW:]

    ts = resampled.index
    wl = resampled.values.astype(np.float64)
    t_numeric = np.arange(len(wl)) * resample_s  # detik sejak awal window
    fs = 1.0 / resample_s

    return ts, wl, t_numeric, fs

# ============================================================================
# 4. EEMD ENGINE - PORT DARI eemd_sliding_window_claude_5.py
# ============================================================================
# Implementasi identik dengan kode v5 untuk konfigurasi W-REF:
#   - run_eemd()                  : EEMD wrapper PyEMD
#   - compute_dominant_period()   : HHT untuk identifikasi periode dominan
#   - select_tsunami_imf_wref()   : Adaptive IMF selection (sum IMF in-band)
#   - get_lm_amplitude()          : Last-moment amplitude
# ----------------------------------------------------------------------------

def run_eemd(signal, t, noise_ratio=EEMD_NOISE_RATIO,
             n_ensemble=EEMD_N_ENSEMBLE, random_seed=EEMD_RANDOM_SEED):
    """
    Wrapper EEMD (Wu & Huang 2009).
    noise amplitude = noise_ratio x std(signal), dikonversi ke
    noise_width PyEMD (yang berbasis peak-to-peak).
    """
    sig_std = float(np.std(signal))
    sig_ptp = float(np.ptp(signal))
    noise_width = (noise_ratio * sig_std) / sig_ptp if sig_ptp > 0 else 0.0

    eemd_obj = PyEEMD(
        trials      = n_ensemble,
        noise_width = noise_width,
        parallel    = False,
    )
    eemd_obj.noise_seed(random_seed)
    eIMFs = eemd_obj.eemd(signal, t)

    if eIMFs.shape[0] == 0:
        # Tidak ada komponen sama sekali -> kembalikan kosong, ditangani pemanggil
        return [], np.zeros_like(signal)

    imfs    = [eIMFs[i] for i in range(eIMFs.shape[0] - 1)]
    residue = eIMFs[-1]
    return imfs, residue


def compute_dominant_period(imf, fs):
    """
    Periode dominan satu IMF via HHT (Huang et al. 1998).
    Frekuensi rata-rata ditimbang oleh energi instantaneous (amp^2).
    """
    analytic   = hilbert(imf)
    inst_amp   = np.abs(analytic)
    inst_phase = np.unwrap(np.angle(analytic))
    inst_freq  = np.diff(inst_phase) / (2.0 * np.pi) * fs

    weights = inst_amp[:-1] ** 2
    total_w = np.sum(weights)

    if total_w > 0 and len(inst_freq) > 0:
        mean_freq = np.sum(inst_freq * weights) / total_w
        mean_freq = max(abs(float(mean_freq)), 1e-10)
        period_s  = 1.0 / mean_freq
    else:
        period_s = np.nan

    return period_s, inst_amp


def select_tsunami_imf_wref(imfs, fs,
                             band_min_s=TSUNAMI_BAND_MIN_S,
                             band_max_s=TSUNAMI_BAND_MAX_S):
    """
    Adaptive IMF selection W-REF (replikasi Wang et al. 2020).
    Jumlahkan semua IMF dengan periode dalam band tsunami,
    lalu hitung inst_amp dari sinyal SUM.

    Returns:
        n_combined, combined_imf, inst_amp_comb, imf_info, imf_nos_used
    """
    imf_info = []
    for i, imf in enumerate(imfs):
        period_s, inst_amp = compute_dominant_period(imf, fs)
        in_band = (not np.isnan(period_s)
                   and band_min_s <= period_s <= band_max_s)
        imf_info.append({
            "imf_no"    : i + 1,
            "period_s"  : period_s,
            "period_min": period_s / 60 if not np.isnan(period_s) else np.nan,
            "in_band"   : in_band,
        })

    band_imfs    = [d for d in imf_info if d["in_band"]]
    imf_nos_used = [d["imf_no"] for d in band_imfs]

    if not band_imfs:
        # Fallback: IMF tengah
        mid_idx = len(imfs) // 2
        imf_nos_used = [mid_idx + 1]
        combined_imf = imfs[mid_idx]
        log.warning(f"[EEMD] Tidak ada IMF di band tsunami "
                    f"({band_min_s}-{band_max_s}s) -> fallback IMF{mid_idx+1}")
    else:
        combined_imf = np.sum([imfs[d["imf_no"] - 1] for d in band_imfs], axis=0)

    analytic_comb  = hilbert(combined_imf)
    inst_amp_comb  = np.abs(analytic_comb)

    return len(imf_nos_used), combined_imf, inst_amp_comb, imf_info, imf_nos_used


def get_lm_amplitude(combined_imf):
    """LM = instantaneous amplitude di titik PALING AKHIR window."""
    return float(combined_imf[-1])


def process_window_eemd(wl, t_numeric, fs):
    """
    Pipeline lengkap satu window:
      1. Center signal (zero-mean)
      2. EEMD -> IMFs
      3. HHT periode dominan setiap IMF
      4. SUM IMF di band tsunami
      5. inst_amp(SUM) via Hilbert
      6. LM = inst_amp[-1]

    Returns:
        dict dengan lm_amp, n_combined, imf_nos_used, t_eemd, t_hht
        atau None jika gagal.
    """
    try:
        # Guard sinyal datar: std = 0 -> EEMD tidak menghasilkan IMF
        # (sebelumnya memicu IndexError di select_tsunami_imf_wref).
        # Pakai toleransi: std sinyal konstan bisa ~1e-15 (pembulatan float).
        if float(np.ptp(wl)) < 1e-9:
            log.warning("[EEMD] Sinyal datar (std=0) - window di-skip "
                        "(indikasi sensor macet / nilai tertahan)")
            return None

        wl_centered = wl - np.mean(wl)

        t0 = time.time()
        imfs, residue = run_eemd(wl_centered, t_numeric)
        t_eemd = time.time() - t0

        if len(imfs) == 0:
            log.warning("[EEMD] Tidak ada IMF yang dihasilkan - window di-skip")
            return None

        t1 = time.time()
        n_combined, combined_imf, inst_amp_comb, imf_info, imf_nos_used = \
            select_tsunami_imf_wref(imfs, fs)
        t_hht = time.time() - t1

        lm_amp = get_lm_amplitude(combined_imf)

        # Periode rata-rata untuk logging
        periods_used = [d["period_s"] for d in imf_info
                        if d["imf_no"] in imf_nos_used
                        and not np.isnan(d["period_s"])]
        period_avg_min = (np.mean(periods_used) / 60.0
                          if periods_used else float("nan"))

        return {
            "lm_amp"         : lm_amp,
            "lm_abs"         : abs(lm_amp),
            "n_imfs_raw"     : len(imfs),
            "n_combined"     : n_combined,
            "imf_nos_used"   : imf_nos_used,
            "period_avg_min" : period_avg_min,
            "t_eemd_s"       : round(t_eemd, 3),
            "t_hht_s"        : round(t_hht, 3),
            "t_total_s"      : round(t_eemd + t_hht, 3),
        }
    except Exception as e:
        log.error(f"[EEMD] Error: {e}\n{traceback.format_exc()}")
        return None

# ============================================================================
# 5. DUAL-LAYER WARNING SYSTEM
# ============================================================================
# Layer 1 - Hysteresis State Machine (IDLE <-> ACTIVE)
#   IDLE   : |LM| >= trigger -> ACTIVE
#   ACTIVE : |LM| < hold    -> IDLE   (release)
#
# Layer 2 - Warning State Machine (Persistence)
#   ACTIVE berurutan >= N_CONFIRM -> WARNING ISSUED
#   Reset saat kembali ke IDLE
# ----------------------------------------------------------------------------

class DualLayerWarning:
    """
    State machine dua lapis untuk konfirmasi warning.
    Dipanggil sekali per window (1 menit) dengan nilai |LM| terkini.

    State variables:
        hyst_state           : "IDLE" | "ACTIVE"
        consecutive_active   : counter window ACTIVE berurutan
        warning_issued       : True setelah N_CONFIRM tercapai
        t_warning_issued     : timestamp konfirmasi pertama
        t_first_detect       : timestamp ACTIVE pertama dalam episode
    """

    def __init__(self, threshold_trigger=THRESHOLD_TRIGGER,
                 threshold_hold=THRESHOLD_HOLD,
                 n_confirm=N_CONFIRM):
        self.threshold_trigger = threshold_trigger
        self.threshold_hold    = threshold_hold
        self.n_confirm         = n_confirm

        self.hyst_state         = "IDLE"
        self.consecutive_active = 0
        self.warning_issued     = False
        self.t_warning_issued   = None
        self.t_first_detect     = None

        # Untuk event-driven notifikasi (transisi)
        self.prev_hyst_state    = "IDLE"
        self.prev_warning       = False
        self.last_episode_len   = 0     # panjang episode ACTIVE terakhir (window)

    def update(self, lm_abs, t_window):
        """
        Update kedua state machine berdasarkan |LM| dan timestamp window.

        Returns:
            dict berisi event terjadi pada window ini:
              {
                  "hyst_state"        : "IDLE" | "ACTIVE"
                  "consecutive_active": int
                  "warning_issued"    : bool
                  "event_detect"      : True jika transisi IDLE->ACTIVE
                  "event_warning"     : True jika WARNING pertama kali dikonfirmasi
                  "event_clear"       : True jika ACTIVE->IDLE (back to normal)
                  "t_first_detect"    : timestamp (untuk hitung delay)
                  "t_warning_issued"  : timestamp konfirmasi
              }
        """
        # -- LAYER 1: Hysteresis ------------------------------------------
        self.prev_hyst_state = self.hyst_state

        if self.hyst_state == "IDLE":
            if lm_abs >= self.threshold_trigger:
                self.hyst_state     = "ACTIVE"
                self.t_first_detect = t_window
        elif self.hyst_state == "ACTIVE":
            if lm_abs < self.threshold_hold:
                self.hyst_state = "IDLE"
            # else: tetap ACTIVE (hold)

        # -- LAYER 2: Warning State Machine -------------------------------
        self.prev_warning = self.warning_issued

        if self.hyst_state == "ACTIVE":
            self.consecutive_active += 1
        else:
            # Simpan panjang episode sebelum counter di-reset
            if self.prev_hyst_state == "ACTIVE":
                self.last_episode_len = self.consecutive_active
            # Reset saat kembali ke IDLE
            self.consecutive_active = 0
            self.warning_issued     = False
            self.t_warning_issued   = None
            self.t_first_detect     = None

        # Konfirmasi WARNING jika persistence tercapai
        if (self.consecutive_active >= self.n_confirm
                and not self.warning_issued):
            self.warning_issued   = True
            self.t_warning_issued = t_window

        # -- Deteksi transisi event untuk notifikasi ---------------------
        event_detect  = (self.prev_hyst_state == "IDLE"
                         and self.hyst_state == "ACTIVE")
        event_warning = (not self.prev_warning and self.warning_issued)
        event_clear   = (self.prev_hyst_state == "ACTIVE"
                         and self.hyst_state == "IDLE")

        return {
            "hyst_state"         : self.hyst_state,
            "consecutive_active" : self.consecutive_active,
            "warning_issued"     : self.warning_issued,
            "event_detect"       : event_detect,
            "event_warning"      : event_warning,
            "event_clear"        : event_clear,
            "t_first_detect"     : self.t_first_detect,
            "t_warning_issued"   : self.t_warning_issued,
            "episode_len"        : self.last_episode_len if event_clear else 0,
        }


# ============================================================================
# 6. TELEGRAM ALERTER
# ============================================================================

def send_telegram(message, parse_mode="HTML"):
    """
    Kirim pesan ke Telegram dengan retry. Sync via requests.
    Aman dipanggil dari thread main - non-fatal kalau gagal.
    """
    if (TELEGRAM_BOT_TOKEN == "PLACEHOLDER_TOKEN"
            or TELEGRAM_CHAT_ID == "PLACEHOLDER_CHAT_ID"):
        log.warning("[TG] Token/chat_id placeholder - pesan tidak dikirim")
        log.info(f"[TG-DRYRUN]\n{message}")
        return False

    url = TELEGRAM_API_URL.format(token=TELEGRAM_BOT_TOKEN)
    payload = {
        "chat_id"   : TELEGRAM_CHAT_ID,
        "text"      : message,
        "parse_mode": parse_mode,
        "disable_web_page_preview": True,
    }

    for attempt in range(1, TELEGRAM_MAX_RETRIES + 1):
        try:
            r = requests.post(url, json=payload, timeout=TELEGRAM_TIMEOUT_S)
            if r.status_code == 200:
                log.info(f"[TG] Pesan terkirim (attempt {attempt})")
                return True
            else:
                log.warning(f"[TG] HTTP {r.status_code}: {r.text[:200]}")
        except Exception as e:
            log.warning(f"[TG] Attempt {attempt}/{TELEGRAM_MAX_RETRIES} gagal: {e}")
        time.sleep(2 ** attempt)  # exponential backoff

    log.error("[TG] Semua retry gagal")
    return False


def fmt_detect_message(t_window, lm_abs, threshold_trigger):
    """Format pesan untuk Layer 1 DETECT (deteksi awal)."""
    return (
        f"\u26A0\uFE0F <b>[L1] DETEKSI AWAL - {SYSTEM_ID}</b>\n"
        f"\n"
        f"<b>Waktu</b>     : {t_window:%Y-%m-%d %H:%M:%S} WIB\n"
        f"<b>|LM|</b>      : {lm_abs:.4f} m\n"
        f"<b>Threshold</b> : {threshold_trigger:.4f} m\n"
        f"<b>Status</b>    : Hysteresis ACTIVE (Layer 1)\n"
        f"\n"
        f"<i>Menunggu konfirmasi {N_CONFIRM} window berurutan untuk WARNING resmi.</i>\n"
        f"<i>Lokasi: {SYSTEM_LOCATION}</i>"
    )


def fmt_warning_message(t_window, t_first_detect, lm_abs):
    """Format pesan untuk Layer 2 WARNING (konfirmasi resmi)."""
    delay_min = ((t_window - t_first_detect).total_seconds() / 60.0
                 if t_first_detect else float("nan"))
    return (
        f"\U0001F6A8\U0001F6A8\U0001F6A8 <b>[L2] WARNING TSUNAMI - {SYSTEM_ID}</b> \U0001F6A8\U0001F6A8\U0001F6A8\n"
        f"\n"
        f"<b>Konfirmasi</b>  : {t_window:%Y-%m-%d %H:%M:%S} WIB\n"
        f"<b>Deteksi awal</b>: {t_first_detect:%H:%M:%S} WIB\n"
        f"<b>Conf. delay</b> : {delay_min:.1f} menit "
        f"({N_CONFIRM} window berurutan)\n"
        f"<b>|LM| terkini</b>: {lm_abs:.4f} m\n"
        f"\n"
        f"<b>SEGERA EVAKUASI KE TEMPAT TINGGI!</b>\n"
        f"\n"
        f"<i>Sistem: {SYSTEM_ID}, {SYSTEM_LOCATION}</i>\n"
        f"<i>Algoritma: EEMD W-REF (Wang et al. 2020) + Dual-Layer Warning</i>"
    )


def fmt_clear_message(t_window, duration_min):
    """Format pesan untuk back-to-normal (ACTIVE -> IDLE)."""
    return (
        f"\u2705 <b>[CLEAR] Kembali Normal - {SYSTEM_ID}</b>\n"
        f"\n"
        f"<b>Waktu</b>     : {t_window:%Y-%m-%d %H:%M:%S} WIB\n"
        f"<b>Durasi</b>    : {duration_min:.1f} menit ACTIVE\n"
        f"<b>Status</b>    : Hysteresis IDLE (normal)\n"
        f"\n"
        f"<i>Sistem kembali memonitor normal.</i>"
    )


def fmt_startup_message():
    """Format pesan startup."""
    return (
        f"\U0001F7E2 <b>U-TEWS Started</b>\n"
        f"\n"
        f"<b>Sistem</b>    : {SYSTEM_ID}\n"
        f"<b>Lokasi</b>    : {SYSTEM_LOCATION}\n"
        f"<b>Hardware</b>  : {SYSTEM_HARDWARE}\n"
        f"<b>Waktu</b>     : {datetime.now():%Y-%m-%d %H:%M:%S} WIB\n"
        f"\n"
        f"<b>Konfigurasi W-REF (Wang et al. 2020):</b>\n"
        f"* Window: {WINDOW_HOURS} jam, resample {RESAMPLE_SECONDS}s, step {STEP_MINUTES} min\n"
        f"* EEMD: {EEMD_N_ENSEMBLE} trials, noise ratio {EEMD_NOISE_RATIO}\n"
        f"* Band tsunami: {TSUNAMI_BAND_MIN_S//60}-{TSUNAMI_BAND_MAX_S//60} menit\n"
        f"* Threshold trigger: {THRESHOLD_TRIGGER:.4f} m\n"
        f"* Threshold hold: {THRESHOLD_HOLD:.4f} m (alpha={ALPHA_HOLD})\n"
        f"* N_CONFIRM: {N_CONFIRM} window berurutan\n"
        f"\n"
        f"<i>Sistem siap memonitor.</i>"
    )

# ============================================================================
# 7. DETECTION LOG CSV
# ============================================================================

DETECTION_LOG_FIELDS = [
    "t_window_end",
    "lm_amp",
    "lm_abs",
    "threshold_trigger",
    "threshold_hold",
    "hyst_state",
    "consecutive_active",
    "warning_issued",
    "n_imfs_raw",
    "n_combined",
    "imf_nos_used",
    "period_avg_min",
    "t_eemd_s",
    "t_hht_s",
    "t_total_s",
    "coverage_pct",
]


def append_detection_log(record):
    """Append satu baris ke CSV detection log harian."""
    date_str = datetime.now().strftime("%Y-%m-%d")
    fpath = os.path.join(LOG_DIR, f"detection_{date_str}.csv")

    new_file = not os.path.exists(fpath)
    try:
        with open(fpath, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=DETECTION_LOG_FIELDS)
            if new_file:
                writer.writeheader()
            writer.writerow(record)
    except Exception as e:
        log.error(f"[LOG] Gagal tulis detection log: {e}")


# ============================================================================
# 8. MAIN LOOP - REAL-TIME SLIDING WINDOW
# ============================================================================

def align_to_minute_boundary():
    """
    Tunggu sampai detik 00 menit berikutnya agar window selalu ber-boundary
    di awal menit (memudahkan korelasi dengan timestamp CSV).
    """
    now = datetime.now()
    next_minute = (now.replace(second=0, microsecond=0)
                   + timedelta(minutes=1))
    wait_s = (next_minute - now).total_seconds()
    if wait_s > 0:
        log.info(f"[INIT] Sinkronisasi ke menit boundary, tunggu {wait_s:.1f}s")
        time.sleep(wait_s)


def main_loop():
    """
    Loop utama real-time:
      Setiap 60 detik:
        1. Ambil window 3 jam terakhir dari CSV (resample mean 1 menit)
        2. Jalankan EEMD W-REF
        3. Hitung LM-SUM-IMF
        4. Update Dual-Layer state machine
        5. Kirim Telegram jika event (DETECT / WARNING / CLEAR)
        6. Append detection log CSV
        7. Tidur sampai menit berikutnya
    """
    log.info("=" * 70)
    log.info(f"U-TEWS REAL-TIME EEMD DETECTION - {SYSTEM_ID}")
    log.info("=" * 70)
    log.info(f"Konfigurasi W-REF: window={WINDOW_HOURS}j, "
             f"resample={RESAMPLE_SECONDS}s, step={STEP_MINUTES}min")
    log.info(f"Threshold trigger={THRESHOLD_TRIGGER:.4f}m, "
             f"hold={THRESHOLD_HOLD:.4f}m (alpha={ALPHA_HOLD})")
    log.info(f"N_CONFIRM={N_CONFIRM} window berurutan")
    log.info(f"Data dir: {DATA_DIR}")
    log.info(f"Log dir : {LOG_DIR}")

    # Kirim notifikasi startup
    send_telegram(fmt_startup_message())

    # Init state machine
    dlw = DualLayerWarning()

    # Hitung counter
    n_windows_processed = 0
    n_windows_skipped   = 0

    # Sinkronisasi ke awal menit
    align_to_minute_boundary()

    next_tick = time.monotonic()

    while True:
        cycle_start = time.monotonic()

        # -- Tentukan t_end window (akhir menit sebelumnya) --------------
        # Misal sekarang 12:05:00, kita proses window yang berakhir di 12:04:59
        # (yaitu window [09:05:00, 12:05:00] dengan data sampai sebelum sekarang)
        now = datetime.now()
        t_end = now.replace(second=0, microsecond=0)

        log.info(f"\n{'-' * 60}")
        log.info(f"[TICK] {now:%H:%M:%S} - proses window berakhir {t_end:%H:%M}")

        # -- Step 1: Load & resample window 3 jam ------------------------
        ts, wl, t_numeric, fs = load_and_resample_window(
            t_end=pd.Timestamp(t_end),
            window_hours=WINDOW_HOURS,
            resample_s=RESAMPLE_SECONDS,
        )

        if ts is None:
            log.warning("[TICK] Window tidak valid - skip")
            n_windows_skipped += 1
            # BUGFIX: naikkan tick DULU baru tidur (sama dgn jalur normal).
            # Urutan lama membuat loop langsung berputar lagi tanpa jeda dan
            # memproses window yang sama dua kali setelah window di-skip.
            next_tick += LOOP_PERIOD_S
            _sleep_until_next_tick(next_tick)
            continue

        coverage_pct = len(wl) / N_PTS_PER_WINDOW * 100
        log.info(f"[DATA] {len(wl)} pts (coverage {coverage_pct:.1f}%), "
                 f"range [{ts[0]:%H:%M} - {ts[-1]:%H:%M}]")
        log.info(f"[DATA] WL stats: mean={np.mean(wl):.3f}m, "
                 f"std={np.std(wl):.4f}m, ptp={np.ptp(wl):.3f}m")

        # -- Step 2: EEMD + HHT + LM -------------------------------------
        eemd_t0 = time.time()
        result = process_window_eemd(wl, t_numeric, fs)
        eemd_dt = time.time() - eemd_t0

        if result is None:
            log.error("[TICK] EEMD gagal - skip window")
            n_windows_skipped += 1
            # BUGFIX: naikkan tick DULU baru tidur (sama dgn jalur normal).
            # Urutan lama membuat loop langsung berputar lagi tanpa jeda dan
            # memproses window yang sama dua kali setelah window di-skip.
            next_tick += LOOP_PERIOD_S
            _sleep_until_next_tick(next_tick)
            continue

        if eemd_dt > EEMD_TIMEOUT_S:
            log.warning(f"[TICK] EEMD lambat ({eemd_dt:.1f}s > {EEMD_TIMEOUT_S}s) "
                        f"tapi tetap diproses")

        log.info(f"[EEMD] {result['n_imfs_raw']} IMF, "
                 f"SUM{result['imf_nos_used']}, "
                 f"T_avg={result['period_avg_min']:.1f}min, "
                 f"t={result['t_total_s']:.2f}s")
        log.info(f"[LM]   |LM|={result['lm_abs']:.5f} m "
                 f"(trigger={THRESHOLD_TRIGGER:.4f}m, hold={THRESHOLD_HOLD:.4f}m)")

        # -- Step 3: Update Dual-Layer Warning ---------------------------
        sm = dlw.update(result["lm_abs"], pd.Timestamp(t_end))

        log.info(f"[L1]   hyst_state={sm['hyst_state']}, "
                 f"consecutive_active={sm['consecutive_active']}")
        log.info(f"[L2]   warning_issued={sm['warning_issued']}")

        # -- Step 4: Notifikasi Telegram berbasis event ------------------
        if sm["event_detect"]:
            log.warning(f"[EVENT] >>> DETECT (Layer 1) di {t_end:%H:%M} <<<")
            send_telegram(fmt_detect_message(
                t_window=t_end,
                lm_abs=result["lm_abs"],
                threshold_trigger=THRESHOLD_TRIGGER,
            ))

        if sm["event_warning"]:
            log.warning(f"[EVENT] >>> WARNING ISSUED (Layer 2) di {t_end:%H:%M} <<<")
            send_telegram(fmt_warning_message(
                t_window=t_end,
                t_first_detect=sm["t_first_detect"],
                lm_abs=result["lm_abs"],
            ))

        if sm["event_clear"]:
            # Durasi episode = jumlah window ACTIVE berurutan (1 window = 1 menit)
            duration_min = sm["episode_len"] * STEP_MINUTES
            log.info(f"[EVENT] === CLEAR (back to IDLE) di {t_end:%H:%M} ===")
            send_telegram(fmt_clear_message(
                t_window=t_end,
                duration_min=duration_min if duration_min > 0 else 1.0,
            ))

        # -- Step 5: Append detection log CSV ----------------------------
        append_detection_log({
            "t_window_end"      : t_end.isoformat(),
            "lm_amp"            : f"{result['lm_amp']:.6f}",
            "lm_abs"            : f"{result['lm_abs']:.6f}",
            "threshold_trigger" : f"{THRESHOLD_TRIGGER:.6f}",
            "threshold_hold"    : f"{THRESHOLD_HOLD:.6f}",
            "hyst_state"        : sm["hyst_state"],
            "consecutive_active": sm["consecutive_active"],
            "warning_issued"    : sm["warning_issued"],
            "n_imfs_raw"        : result["n_imfs_raw"],
            "n_combined"        : result["n_combined"],
            "imf_nos_used"      : "|".join(map(str, result["imf_nos_used"])),
            "period_avg_min"    : f"{result['period_avg_min']:.2f}",
            "t_eemd_s"          : result["t_eemd_s"],
            "t_hht_s"           : result["t_hht_s"],
            "t_total_s"         : result["t_total_s"],
            "coverage_pct"      : f"{coverage_pct:.1f}",
        })

        n_windows_processed += 1
        cycle_dt = time.monotonic() - cycle_start
        log.info(f"[STAT] processed={n_windows_processed}, skipped={n_windows_skipped}, "
                 f"cycle_time={cycle_dt:.2f}s")

        # -- Step 6: Sleep sampai tick berikutnya ------------------------
        next_tick += LOOP_PERIOD_S
        _sleep_until_next_tick(next_tick)


def _sleep_until_next_tick(next_tick):
    """Sleep akurat sampai next_tick (monotonic clock)."""
    sleep_s = next_tick - time.monotonic()
    if sleep_s > 0:
        time.sleep(sleep_s)
    else:
        # Cycle telat - log warning tapi jangan tunggu (kejar)
        log.warning(f"[SCHED] Cycle terlambat {-sleep_s:.2f}s, kejar tick berikutnya")


# ============================================================================
# 9. ENTRY POINT
# ============================================================================

def main():
    try:
        main_loop()
    except KeyboardInterrupt:
        log.info("\n[EXIT] KeyboardInterrupt - shutdown gracefully")
        send_telegram(
            f"\U0001F534 <b>U-TEWS Stopped</b>\n\n"
            f"Sistem dihentikan oleh user pada "
            f"{datetime.now():%Y-%m-%d %H:%M:%S} WIB."
        )
    except Exception as e:
        log.error(f"[FATAL] Unhandled exception: {e}\n{traceback.format_exc()}")
        send_telegram(
            f"\U0001F534 <b>U-TEWS CRASH</b>\n\n"
            f"<b>Error</b>: <code>{str(e)[:200]}</code>\n"
            f"<b>Waktu</b>: {datetime.now():%Y-%m-%d %H:%M:%S}\n\n"
            f"<i>Cek log untuk detail.</i>"
        )
        sys.exit(1)


if __name__ == "__main__":
    main()