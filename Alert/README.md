# U-TEWS Real-Time EEMD Detection — Deployment Guide

Sistem deteksi dini tsunami real-time untuk Orange Pi 5 di Pulau Sebesi, Lampung.

---

## Arsitektur

```
┌──────────────────────────────────────────────────────────────────┐
│                    Orange Pi 5 (Ubuntu Jammy)                    │
│                                                                  │
│  ┌───────────────────────┐                                       │
│  │ water_monitoring_10hz │  10 Hz  ┌──────────────────────────┐  │
│  │ (sudah jalan)         │────────►│ CSV processed harian     │  │
│  └───────────────────────┘         │ /home/orangepi/data/     │  │
│           │                        │   water/                 │  │
│           ▼                        │   water_processed_*.csv  │  │
│       Modbus RTU                   └────────┬─────────────────┘  │
│       GLT500                                │                    │
│                                             │ baca tiap 1 menit  │
│                                             ▼                    │
│                              ┌──────────────────────────────┐    │
│                              │ utews_realtime.py            │    │
│                              │ (yang ini)                   │    │
│                              │                              │    │
│                              │  1. Resample mean → 1 menit  │    │
│                              │  2. Window 3 jam (180 pts)   │    │
│                              │  3. EEMD W-REF (Wang 2020)   │    │
│                              │  4. HHT + Adaptive SUM IMF   │    │
│                              │  5. LM-SUM-IMF amplitude     │    │
│                              │  6. Hysteresis (α=0.5)       │    │
│                              │  7. N_CONFIRM=3 persistence  │    │
│                              │  8. Telegram alert           │    │
│                              └──────┬───────────────┬───────┘    │
│                                     │               │            │
│                                     ▼               ▼            │
│                          detection_*.csv       Telegram Bot      │
│                          /utews_realtime/      → BPBD / tim      │
└──────────────────────────────────────────────────────────────────┘
```

---

## Prasyarat

```bash
# Python 3.10+ (sudah ada di Ubuntu Jammy)
python3 --version

# Library yang dibutuhkan
pip3 install --break-system-packages \
    numpy pandas scipy EMD-signal requests
```

**Catatan:** `EMD-signal` adalah nama PyPI dari `PyEMD` (yang di-import sebagai `from PyEMD import EEMD`). Jangan keliru dengan package `pyemd` yang berbeda.

---

## Setup Telegram Bot

### 1. Buat Bot
1. Buka Telegram → cari **@BotFather**
2. Kirim `/newbot`, ikuti instruksi
3. Catat **TOKEN** yang diberikan (format: `1234567890:AAH...`)

### 2. Dapatkan Chat ID

**Untuk personal chat:**
1. Cari bot kamu di Telegram, kirim pesan apapun (misal `/start`)
2. Buka di browser: `https://api.telegram.org/bot<TOKEN>/getUpdates`
3. Cari `"chat":{"id":XXXXXXX}` — itu chat ID kamu

**Untuk grup:**
1. Tambah bot ke grup
2. Kirim pesan di grup yang men-mention bot, contoh: `@namabot test`
3. Buka URL `getUpdates` di atas → chat ID grup biasanya negatif (mis. `-1001234567890`)

### 3. Test Manual
```bash
TOKEN="ISI_DI_SINI"
CHAT_ID="ISI_DI_SINI"
curl -X POST "https://api.telegram.org/bot${TOKEN}/sendMessage" \
     -d "chat_id=${CHAT_ID}&text=Test U-TEWS"
```

Jika berhasil, pesan akan masuk.

---

## Instalasi di Orange Pi 5

### 1. Salin file
```bash
# Buat direktori
sudo mkdir -p /home/orangepi/utews
sudo chown orangepi:orangepi /home/orangepi/utews

# Salin file (asumsi sudah di-scp dari laptop)
cp utews_realtime.py /home/orangepi/utews/
cp utews.env.template /home/orangepi/utews/utews.env

# Edit env file dengan TOKEN & CHAT_ID asli
nano /home/orangepi/utews/utews.env

# Lindungi credential
chmod 600 /home/orangepi/utews/utews.env
```

### 2. Buat direktori output
```bash
sudo mkdir -p /home/orangepi/data/utews_realtime
sudo chown orangepi:orangepi /home/orangepi/data/utews_realtime
```

### 3. Test manual dulu (sebelum jadikan service)
```bash
cd /home/orangepi/utews
set -a; source utews.env; set +a   # load env ke shell
python3 utews_realtime.py
```

Biarkan jalan minimal 2-3 menit. Cek output di terminal & file `detection_*.csv`. Hentikan dengan `Ctrl+C`.

### 4. Install sebagai systemd service
```bash
sudo cp utews.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable utews.service     # auto-start saat boot
sudo systemctl start utews.service      # langsung jalankan
```

### 5. Monitoring
```bash
# Status service
sudo systemctl status utews

# Log live (Ctrl+C untuk keluar — service tetap jalan)
sudo journalctl -u utews -f

# Log file harian
tail -f /home/orangepi/data/utews_realtime/utews_$(date +%Y-%m-%d).log

# Detection log CSV
tail -f /home/orangepi/data/utews_realtime/detection_$(date +%Y-%m-%d).csv
```

### 6. Kontrol service
```bash
sudo systemctl stop utews        # hentikan
sudo systemctl restart utews     # restart
sudo systemctl disable utews     # cabut auto-start
```

---

## Konfigurasi Final (Sesuai Laporan v7 §12.9)

| Parameter | Nilai | Sumber |
|---|---|---|
| Window length | 3 jam | Wang et al. (2020) |
| Resample | 60 detik (1 menit) | Wang et al. (2020) |
| Step sliding | 1 menit | Optimal responsivitas |
| N points/window | 180 | 3h × 60min |
| EEMD trials | 100 | Wu & Huang (2009) |
| Noise ratio | 0.2 | Wu & Huang (2009) |
| Band tsunami | 5–60 menit | Kalibrasi empiris |
| Threshold trigger | 0.1687 m | Kalibrasi 7 hari × k=1.5 |
| α hysteresis | 0.5 | Kontribusi original |
| Threshold hold | 0.0843 m | α × trigger |
| N_CONFIRM | 3 window | Persistence criterion |
| Confirmation delay | +2 menit | (N_CONFIRM−1) × step |

---

## Troubleshooting

### "Tidak ada file processed"
Pastikan `water_monitoring_10hz.py` sudah jalan dan menulis ke
`/home/orangepi/data/water/water_processed_YYYY-MM-DD.csv`.

```bash
ls -la /home/orangepi/data/water/
tail /home/orangepi/data/water/water_processed_$(date +%Y-%m-%d).csv
```

### "Coverage hanya XX%"
Sensor sempat down. Sistem akan skip window tersebut dan coba lagi 1 menit kemudian. Cek log `water_monitoring`:

```bash
sudo journalctl -u water-monitoring -n 100  # kalau sudah jadi service
```

### EEMD lambat (>50 detik)
Cek beban CPU:
```bash
htop
```
Jika ada proses lain yang berat, pertimbangkan untuk menonaktifkan. Orange Pi 5 dari benchmark kamu sebelumnya seharusnya bisa selesai dalam ~10-20 detik untuk window 180 titik.

### Telegram tidak terkirim
1. Pastikan `utews.env` terisi dengan TOKEN & CHAT_ID yang benar
2. Pastikan service membaca env file: `sudo systemctl show utews | grep Environment`
3. Test koneksi internet: `curl https://api.telegram.org`
4. Cek log: `sudo journalctl -u utews | grep -i telegram`

---

## Validasi Berbasis Data Historis

Untuk validasi end-to-end sebelum deployment lapangan, kamu bisa:

1. Letakkan file CSV historis (dari skenario injeksi v3 misalnya) di `/home/orangepi/data/water/water_processed_2025-03-15.csv`
2. Set tanggal sistem ke 2025-03-15: `sudo timedatectl set-time "2025-03-15 06:00:00"` (sementara, untuk testing)
3. Jalankan service dan amati detection event
4. Bandingkan hasil dengan output `eemd_sliding_window_claude_5.py` offline

**PENTING:** Jangan lupa kembalikan waktu sistem ke real-time setelah testing dengan `sudo timedatectl set-ntp true`.

---

## File yang Dihasilkan

| Path | Isi |
|---|---|
| `/home/orangepi/data/utews_realtime/detection_YYYY-MM-DD.csv` | Log per-window |
| `/home/orangepi/data/utews_realtime/utews_YYYY-MM-DD.log` | Log lengkap aplikasi |

Kolom detection CSV:
- `t_window_end`: timestamp akhir window
- `lm_amp`, `lm_abs`: nilai LM-SUM-IMF
- `threshold_trigger`, `threshold_hold`: threshold yang dipakai
- `hyst_state`: IDLE / ACTIVE
- `consecutive_active`: counter window ACTIVE berurutan
- `warning_issued`: True/False (Layer 2)
- `n_imfs_raw`: jumlah IMF hasil EEMD
- `n_combined`: jumlah IMF yang dijumlahkan (di band tsunami)
- `imf_nos_used`: daftar nomor IMF, pipe-separated
- `period_avg_min`: periode rata-rata IMF terpilih
- `t_eemd_s`, `t_hht_s`, `t_total_s`: waktu komputasi
- `coverage_pct`: persen data sensor tersedia dalam window
