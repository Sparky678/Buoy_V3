"""
quality_control.py  (rev 2.0 - edge-grade)
==========================================
Data cleaning & quality control untuk sistem akuisisi Buoy-V3 (U-TEWS).

Perubahan kunci vs rev 1.x:
  - Tidak pernah memfabrikasi nilai. Saat data tidak sah, value = None.
  - Setiap fungsi mengembalikan QCResult (value, status, flag, reason),
    bukan float telanjang. Pemanggil yang memutuskan tindakan hilir.
  - History hanya menyimpan pengukuran NYATA + timestamp monotonic.
  - Last-value hold dibatasi waktu (MAX_HOLD_AGE_S) -> deteksi STALE.
  - Per-channel state diisolasi dalam objek Channel (thread-safe).

Kompatibilitas: tersedia wrapper *_value() yang mengembalikan float|None
agar drop-in ke kode lama yang hanya butuh angka.

Usage (disarankan):
    from quality_control import water_qc
    res = water_qc.clean(raw_value)          # res.value bisa None
    if res.is_usable:
        tulis_csv(res.value, res.status.name)

Usage (kompat lama):
    from quality_control import clean_water_level_value
    v = clean_water_level_value(raw_value)   # float | None
"""

from __future__ import annotations

import time
import threading
from dataclasses import dataclass, field
from enum import Enum
from collections import deque
from typing import Optional


# ==========================================
# STATUS & HASIL
# ==========================================
class QCStatus(Enum):
    OK = "ok"                    # nilai asli, lolos semua cek
    SMOOTHED = "smoothed"        # spike terdeteksi, dirata-rata
    CLAMPED = "clamped"          # di luar batas lunak, dipotong ke batas
    HELD = "held"               # data hilang, pakai nilai sah terakhir (masih segar)
    STALE = "stale"             # data hilang terlalu lama -> value None
    MISSING = "missing"          # data hilang & belum ada history -> value None
    REJECTED = "rejected"        # di luar batas keras / NaN -> value None


# status yang nilainya boleh dipakai konsumen biasa (CSV/MQTT display)
_USABLE = {QCStatus.OK, QCStatus.SMOOTHED, QCStatus.CLAMPED, QCStatus.HELD}
# status yang nilainya boleh masuk ALERT/DART (lebih ketat: hanya pengukuran nyata)
_ALERT_SAFE = {QCStatus.OK, QCStatus.SMOOTHED}


@dataclass(frozen=True)
class QCResult:
    value: Optional[float]
    status: QCStatus
    reason: str = ""

    @property
    def is_usable(self) -> bool:
        """Boleh ditulis ke CSV/MQTT (termasuk HELD)."""
        return self.status in _USABLE and self.value is not None

    @property
    def is_alert_safe(self) -> bool:
        """Boleh dipakai memutuskan alarm (hanya pengukuran nyata)."""
        return self.status in _ALERT_SAFE and self.value is not None

    @property
    def flag(self) -> str:
        """String pendek untuk kolom quality_flag di CSV."""
        return self.status.value.upper()


# ==========================================
# CHANNEL: satu sensor = satu Channel
# ==========================================
@dataclass
class ChannelConfig:
    name: str
    # batas keras: di luar ini -> REJECTED (mustahil secara fisik / kode error)
    hard_min: float
    hard_max: float
    # batas lunak: di luar ini -> CLAMPED ke batas (overshoot wajar)
    soft_min: Optional[float] = None
    soft_max: Optional[float] = None
    # ambang spike (perubahan per detik). None = nonaktif.
    max_rate_per_s: Optional[float] = None
    # berapa lama nilai terakhir boleh di-hold sebelum STALE (detik)
    max_hold_age_s: float = 3.0
    # ukuran history
    history_len: int = 200


class Channel:
    """
    State & logika QC untuk satu kanal sensor. Thread-safe.
    History menyimpan tuple (value_asli, t_monotonic) -- HANYA pengukuran nyata.
    """

    def __init__(self, cfg: ChannelConfig):
        self.cfg = cfg
        self._hist = deque(maxlen=cfg.history_len)   # (value, t_mono)
        self._lock = threading.Lock()

    # ---- util internal ----
    def _last(self):
        return self._hist[-1] if self._hist else None

    def _commit(self, value: float, t: float):
        self._hist.append((value, t))

    # ---- API utama ----
    def clean(self, value, now: Optional[float] = None) -> QCResult:
        cfg = self.cfg
        now = time.monotonic() if now is None else now

        with self._lock:
            # 1. MISSING / NaN
            if value is None or (isinstance(value, float) and value != value):
                last = self._last()
                if last is None:
                    return QCResult(None, QCStatus.MISSING,
                                    "no data and no history")
                age = now - last[1]
                if age <= cfg.max_hold_age_s:
                    return QCResult(last[0], QCStatus.HELD,
                                    f"held {age:.2f}s")
                return QCResult(None, QCStatus.STALE,
                                f"stale {age:.2f}s > {cfg.max_hold_age_s:.2f}s")

            # konversi aman ke float
            try:
                v = float(value)
            except (TypeError, ValueError):
                return QCResult(None, QCStatus.REJECTED, f"not numeric: {value!r}")

            # 2. HARD range -> REJECTED (jangan di-hold; ini kemungkinan kode error)
            if v < cfg.hard_min or v > cfg.hard_max:
                return QCResult(None, QCStatus.REJECTED,
                                f"hard range {v:.3f} not in "
                                f"[{cfg.hard_min},{cfg.hard_max}]")

            status = QCStatus.OK
            reason = ""
            clamped = False

            # 3. SOFT range -> CLAMP
            if cfg.soft_min is not None and v < cfg.soft_min:
                reason = f"clamped {v:.3f}->{cfg.soft_min}"
                v = cfg.soft_min
                status = QCStatus.CLAMPED
                clamped = True
            elif cfg.soft_max is not None and v > cfg.soft_max:
                reason = f"clamped {v:.3f}->{cfg.soft_max}"
                v = cfg.soft_max
                status = QCStatus.CLAMPED
                clamped = True

            # 4. SPIKE (rate-based, bukan absolut) -> SMOOTH
            last = self._last()
            if cfg.max_rate_per_s is not None and last is not None:
                last_v, last_t = last
                dt = max(now - last_t, 1e-6)
                rate = abs(v - last_v) / dt
                if rate > cfg.max_rate_per_s:
                    smoothed = (last_v + v) / 2.0
                    # commit nilai smoothed sebagai pengukuran (dengan timestamp now)
                    self._commit(smoothed, now)
                    note = f"spike {rate:.2f}/s -> smoothed"
                    if clamped:
                        note = reason + "; " + note
                    return QCResult(smoothed, QCStatus.SMOOTHED, note)

            # 5. OK / CLAMPED: commit ke history
            self._commit(v, now)
            return QCResult(v, status, reason)

    def reset(self):
        with self._lock:
            self._hist.clear()

    def stats(self) -> dict:
        with self._lock:
            last = self._last()
            return {
                "name": self.cfg.name,
                "buffer_size": len(self._hist),
                "last_value": None if last is None else last[0],
                "last_age_s": None if last is None else time.monotonic() - last[1],
            }


# ==========================================
# INSTANSIASI KANAL (sesuaikan kalibrasi di sini)
# ==========================================
# Water level (meter). hard range = batas fisik mutlak; 65535/0.04-stuck akan
# tertangkap sebagai REJECTED bila di-feed nilai kode-error mentah.
water_qc = Channel(ChannelConfig(
    name="water_level",
    hard_min=-3.0, hard_max=200.0,      # di luar ini = mustahil/kode error
    soft_min=-2.0, soft_max=60.0,      # overshoot wajar dipotong
    max_rate_per_s=1.0,                # >1 m/s perubahan = spike
    max_hold_age_s=2.0,                # @10Hz: hold maks 2 dtk (20 sampel)
    history_len=200,
))

anem_qc = Channel(ChannelConfig(
    name="anemometer",
    hard_min=0.0, hard_max=120.0,
    soft_min=0.0, soft_max=100.0,
    max_rate_per_s=None,               # angin bisa berubah cepat; nonaktif
    max_hold_age_s=3.0,
    history_len=20,
))

wind_qc = Channel(ChannelConfig(
    name="wind_angle",
    hard_min=0.0, hard_max=360.0,
    soft_min=0.0, soft_max=360.0,
    max_rate_per_s=None,
    max_hold_age_s=3.0,
    history_len=20,
))


# ==========================================
# WRAPPER KOMPAT (float | None) - drop-in kode lama
# ==========================================
def clean_water_level(value) -> QCResult:
    return water_qc.clean(value)

def clean_anemometer(value) -> QCResult:
    return anem_qc.clean(value)

def clean_wind_angle(value) -> QCResult:
    return wind_qc.clean(value)

def clean_water_level_value(value):
    """Kembalikan float|None saja (None bila data tak sah)."""
    return water_qc.clean(value).value

def clean_anemometer_value(value):
    return anem_qc.clean(value).value

def clean_wind_angle_value(value):
    return wind_qc.clean(value).value


# ==========================================
# VALIDASI ALERT (strict) -- kini berbasis QCResult
# ==========================================
def validate_water_for_alert(result: QCResult) -> bool:
    """
    Hanya nilai pengukuran NYATA yang boleh memicu/menahan alarm.
    HELD/STALE/MISSING/REJECTED/CLAMPED ditolak agar sensor mati tidak
    pernah menyamar sebagai pembacaan sah di buffer DART.
    """
    if not isinstance(result, QCResult):
        # toleransi pemanggilan lama dgn float mentah -> tolak demi keamanan
        return False
    if not result.is_alert_safe:
        return False
    return True


def get_statistics() -> dict:
    return {
        "water": water_qc.stats(),
        "anem": anem_qc.stats(),
        "wind": wind_qc.stats(),
    }

def reset_buffers():
    water_qc.reset()
    anem_qc.reset()
    wind_qc.reset()


# ==========================================
# SELF-TEST
# ==========================================
if __name__ == "__main__":
    import math

    print("== Self-test quality_control rev2 ==\n")
    reset_buffers()

    seq = [
        ("1) sampel sah pertama", 0.04, None),
        ("2) None (history msh segar)", None, None),
        ("3) None lagi", None, None),
        ("4) None setelah lama", None, 5.0),   # now+5s -> STALE
        ("5) sah lagi", 0.05, 6.0),
        ("6) spike >1m/s", 3.0, 6.1),
        ("7) overshoot soft", 16.0, 7.0),
        ("8) hard range / kode error", 65535.0, 8.0),
        ("9) NaN", float("nan"), 9.0),
    ]
    t0 = time.monotonic()
    for label, val, off in seq:
        now = None if off is None else t0 + off
        r = water_qc.clean(val, now=now)
        print(f"{label:38s} in={str(val):>10} -> "
              f"value={r.value} status={r.status.name:8s} "
              f"alert_safe={r.is_alert_safe} | {r.reason}")

    print("\nstats:", get_statistics()["water"])