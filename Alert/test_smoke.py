#!/usr/bin/env python3
"""
Smoke test: jalankan satu siklus end-to-end dengan data sintetik
untuk memvalidasi bahwa pipeline berfungsi sebagaimana mestinya.

Test ini TIDAK menggantikan validasi laboratorium, tapi memastikan:
  1. CSV reader & resampler bekerja
  2. EEMD engine menghasilkan output
  3. Dual-Layer Warning state machine transisi dengan benar
  4. Telegram alerter (dry-run mode) format pesannya benar
"""

import os
import sys
import tempfile
import shutil
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

# Setup path dummy SEBELUM import modul utama
TMP_DIR = tempfile.mkdtemp(prefix="utews_test_")
DATA_DIR = os.path.join(TMP_DIR, "water")
LOG_DIR  = os.path.join(TMP_DIR, "utews_realtime")
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# Override env BEFORE import
os.environ["UTEWS_TG_TOKEN"]   = "PLACEHOLDER_TOKEN"
os.environ["UTEWS_TG_CHAT_ID"] = "PLACEHOLDER_CHAT_ID"

# Monkey-patch DATA_DIR & LOG_DIR
import utews_realtime as ut
ut.DATA_DIR = DATA_DIR
ut.LOG_DIR  = LOG_DIR


def make_synthetic_csv(t_end, hours=4, sampling_rate_hz=10,
                       baseline=3.0, noise_std=0.02,
                       tsunami_amp=0.0, tsunami_arrival_offset_min=None):
    """
    Buat CSV processed sintetik 10 Hz.
    Format kolom: timestamp (microsecond), WaterLevel_m

    Args:
        t_end: pd.Timestamp akhir data
        hours: durasi data ke belakang dari t_end
        tsunami_amp: amplitudo simulasi tsunami (m). 0 = normal noise saja.
        tsunami_arrival_offset_min: berapa menit sebelum t_end tsunami "tiba".
    """
    t_start = t_end - pd.Timedelta(hours=hours)
    n_samples = int(hours * 3600 * sampling_rate_hz)

    # Generate timestamp 10 Hz
    timestamps = pd.date_range(start=t_start, periods=n_samples,
                                freq=f"{1000//sampling_rate_hz}ms")

    # Baseline: sin tidal panjang (period 12 jam) + noise gaussian
    t_sec = np.arange(n_samples) / sampling_rate_hz
    tidal = 0.3 * np.sin(2 * np.pi * t_sec / (12 * 3600))
    noise = np.random.normal(0, noise_std, n_samples)
    wl = baseline + tidal + noise

    # Tambah simulasi tsunami: N-wave (1 cycle) dengan period 10 menit
    if tsunami_amp > 0 and tsunami_arrival_offset_min is not None:
        t_arrival = t_end - pd.Timedelta(minutes=tsunami_arrival_offset_min)
        arrival_idx = int((t_arrival - t_start).total_seconds() * sampling_rate_hz)

        T_tsunami = 10 * 60   # 10 menit periode
        n_cycle   = int(T_tsunami * sampling_rate_hz)

        if arrival_idx + n_cycle * 6 < n_samples:
            t_wave = np.arange(n_cycle * 6) / sampling_rate_hz
            # N-wave dengan envelope decay
            envelope = np.exp(-t_wave / (T_tsunami * 3))
            wave = tsunami_amp * envelope * np.sin(2 * np.pi * t_wave / T_tsunami)
            wl[arrival_idx:arrival_idx + len(wave)] += wave

    df = pd.DataFrame({
        "timestamp"   : timestamps.strftime("%Y-%m-%d %H:%M:%S.%f").str[:-3],
        "WaterLevel_m": wl,
    })

    # Tulis ke file harian sesuai konvensi
    date_str = t_end.strftime("%Y-%m-%d")
    fpath = os.path.join(DATA_DIR, f"water_processed_{date_str}.csv")
    df.to_csv(fpath, index=False)
    return fpath


def test_csv_reader():
    """Test 1: CSV reader & resampler 10 Hz → 1 menit."""
    print("\n" + "="*70)
    print("TEST 1: CSV Reader & Resampler 10 Hz → 1 menit")
    print("="*70)

    t_end = pd.Timestamp("2026-05-26 12:00:00")
    make_synthetic_csv(t_end, hours=4, baseline=3.0, noise_std=0.02)

    ts, wl, t_n, fs = ut.load_and_resample_window(t_end, window_hours=3,
                                                    resample_s=60)

    assert ts is not None, "Reader gagal load data"
    assert len(wl) == 180, f"Expected 180 bins, got {len(wl)}"
    assert fs == 1.0/60, f"Expected fs=1/60, got {fs}"

    print(f"✓ Loaded {len(wl)} bins")
    print(f"✓ Mean WL: {np.mean(wl):.3f}m, std: {np.std(wl):.4f}m")
    print(f"✓ Range: [{ts[0]:%H:%M} — {ts[-1]:%H:%M}]")
    print(f"✓ Sampling rate: {fs} Hz (= 1/{1/fs:.0f} detik)")
    return True


def test_eemd_pipeline():
    """Test 2: EEMD pipeline pada window normal (tidak ada tsunami)."""
    print("\n" + "="*70)
    print("TEST 2: EEMD Pipeline (normal noise only)")
    print("="*70)

    t_end = pd.Timestamp("2026-05-26 12:00:00")
    ts, wl, t_n, fs = ut.load_and_resample_window(t_end, window_hours=3,
                                                    resample_s=60)

    print(f"Memproses window {len(wl)} pts, ini bisa lambat (10-30s)...")
    result = ut.process_window_eemd(wl, t_n, fs)

    assert result is not None, "EEMD pipeline gagal"
    assert "lm_amp" in result
    assert "n_combined" in result

    print(f"✓ EEMD selesai dalam {result['t_total_s']}s")
    print(f"✓ N IMF raw: {result['n_imfs_raw']}, "
          f"N combined (in-band): {result['n_combined']}")
    print(f"✓ IMF yang dipakai: {result['imf_nos_used']}")
    print(f"✓ Periode rata-rata: {result['period_avg_min']:.1f} menit")
    print(f"✓ |LM|: {result['lm_abs']:.5f} m "
          f"(threshold trigger: {ut.THRESHOLD_TRIGGER:.4f} m)")

    # Untuk data normal, |LM| seharusnya < threshold trigger
    if result['lm_abs'] < ut.THRESHOLD_TRIGGER:
        print(f"✓ Data normal: |LM| < trigger (sebagaimana harapan)")
    else:
        print(f"⚠ |LM| > trigger pada data normal — cek noise std")

    return result


def test_state_machine_normal_to_warning():
    """Test 3: State machine — simulasi transisi IDLE → ACTIVE → WARNING."""
    print("\n" + "="*70)
    print("TEST 3: Dual-Layer State Machine (simulasi event)")
    print("="*70)

    dlw = ut.DualLayerWarning(
        threshold_trigger=ut.THRESHOLD_TRIGGER,
        threshold_hold=ut.THRESHOLD_HOLD,
        n_confirm=ut.N_CONFIRM,
    )

    # Skenario:
    # menit 1: normal (di bawah trigger) → IDLE
    # menit 2: normal → IDLE
    # menit 3: |LM| = 0.20 (> trigger) → ACTIVE, consecutive=1, event_detect=True
    # menit 4: |LM| = 0.18 (> hold) → ACTIVE, consecutive=2
    # menit 5: |LM| = 0.17 (> hold) → ACTIVE, consecutive=3, event_warning=True
    # menit 6: |LM| = 0.05 (< hold) → IDLE, event_clear=True

    scenarios = [
        (1, 0.05, "IDLE",   1),  # under
        (2, 0.08, "IDLE",   2),
        (3, 0.20, "ACTIVE", 3),  # trigger ⇒ DETECT
        (4, 0.18, "ACTIVE", 4),
        (5, 0.17, "ACTIVE", 5),  # consec=3 ⇒ WARNING
        (6, 0.05, "IDLE",   6),  # release ⇒ CLEAR
    ]

    base_t = datetime(2026, 5, 26, 12, 0, 0)
    events_seen = {"detect": 0, "warning": 0, "clear": 0}

    for minute, lm_abs, expected_state, idx in scenarios:
        t = base_t + timedelta(minutes=minute)
        sm = dlw.update(lm_abs, t)

        marker = ""
        if sm["event_detect"]:
            marker += " [DETECT]"
            events_seen["detect"] += 1
        if sm["event_warning"]:
            marker += " [WARNING]"
            events_seen["warning"] += 1
        if sm["event_clear"]:
            marker += " [CLEAR]"
            events_seen["clear"] += 1

        print(f"  t={t:%H:%M} |LM|={lm_abs:.3f} → "
              f"state={sm['hyst_state']:6s}, "
              f"consec={sm['consecutive_active']}, "
              f"warning={sm['warning_issued']}{marker}")

        assert sm["hyst_state"] == expected_state, \
            f"Expected {expected_state}, got {sm['hyst_state']} at minute {minute}"

    assert events_seen["detect"]  == 1, "Harus 1× DETECT event"
    assert events_seen["warning"] == 1, "Harus 1× WARNING event"
    assert events_seen["clear"]   == 1, "Harus 1× CLEAR event"

    print(f"\n✓ Semua transisi state benar")
    print(f"✓ Events: {events_seen}")
    return True


def test_telegram_dryrun():
    """Test 4: Format pesan Telegram (dry-run, tidak benar-benar kirim)."""
    print("\n" + "="*70)
    print("TEST 4: Telegram Message Formatting (Dry-Run)")
    print("="*70)

    t = datetime(2026, 5, 26, 12, 5, 0)
    t_detect = datetime(2026, 5, 26, 12, 3, 0)

    print("\n--- Startup Message ---")
    print(ut.fmt_startup_message())

    print("\n--- DETECT Message ---")
    print(ut.fmt_detect_message(t, lm_abs=0.20, threshold_trigger=0.1687))

    print("\n--- WARNING Message ---")
    print(ut.fmt_warning_message(t, t_first_detect=t_detect, lm_abs=0.17))

    print("\n--- CLEAR Message ---")
    print(ut.fmt_clear_message(t, duration_min=3.0))

    # Test send (dry-run karena token = PLACEHOLDER)
    print("\n--- Dry-run send (cek graceful handling) ---")
    result = ut.send_telegram("Test message")
    assert result is False, "Harus return False saat token placeholder"
    print("✓ Graceful handling untuk token placeholder")

    return True


def test_detection_log():
    """Test 5: Detection log CSV."""
    print("\n" + "="*70)
    print("TEST 5: Detection Log CSV")
    print("="*70)

    record = {
        "t_window_end"      : "2026-05-26T12:05:00",
        "lm_amp"            : "0.123456",
        "lm_abs"            : "0.123456",
        "threshold_trigger" : "0.168662",
        "threshold_hold"    : "0.084331",
        "hyst_state"        : "IDLE",
        "consecutive_active": 0,
        "warning_issued"    : False,
        "n_imfs_raw"        : 7,
        "n_combined"        : 2,
        "imf_nos_used"      : "3|4",
        "period_avg_min"    : "12.5",
        "t_eemd_s"          : 8.5,
        "t_hht_s"           : 0.3,
        "t_total_s"         : 8.8,
        "coverage_pct"      : "99.4",
    }

    ut.append_detection_log(record)
    ut.append_detection_log(record)

    date_str = datetime.now().strftime("%Y-%m-%d")
    fpath = os.path.join(LOG_DIR, f"detection_{date_str}.csv")
    assert os.path.exists(fpath), f"Log file tidak terbuat: {fpath}"

    df = pd.read_csv(fpath)
    assert len(df) == 2, f"Expected 2 rows, got {len(df)}"
    print(f"✓ Log file: {fpath}")
    print(f"✓ {len(df)} rows tertulis")
    print(f"✓ Columns: {df.columns.tolist()}")
    return True


def run_all_tests():
    """Jalankan semua test."""
    print("\n" + "█"*70)
    print("U-TEWS REAL-TIME PIPELINE — SMOKE TESTS")
    print("█"*70)

    np.random.seed(42)  # reproducibility

    tests = [
        ("CSV Reader",         test_csv_reader),
        ("EEMD Pipeline",      test_eemd_pipeline),
        ("State Machine",      test_state_machine_normal_to_warning),
        ("Telegram Format",    test_telegram_dryrun),
        ("Detection Log",      test_detection_log),
    ]

    passed = 0
    failed = []

    for name, fn in tests:
        try:
            fn()
            passed += 1
        except Exception as e:
            import traceback
            print(f"\n✗ TEST FAILED: {name}")
            print(traceback.format_exc())
            failed.append((name, str(e)))

    print("\n" + "█"*70)
    print(f"HASIL: {passed}/{len(tests)} passed")
    if failed:
        print(f"Failed: {[n for n,_ in failed]}")
    print("█"*70)

    # Cleanup
    shutil.rmtree(TMP_DIR)

    return len(failed) == 0


if __name__ == "__main__":
    sys.exit(0 if run_all_tests() else 1)
