"""
wind_monitoring.py  (rev3 - mitigasi kegagalan I2C IMU)
=======================================================
Akuisisi sensor angin 10 Hz dengan koreksi arah angin sejati (true wind).

Sensor:
- Anemometer (wind speed)  -> anemometer.py    (Modbus RTU, slave ID 3)
- Wind direction (relatif) -> WindDirect.py    (Modbus RTU, slave ID 2)
- IMU kompas               -> ADXL345 (accel) + ITG3200 (gyro) + VCM5883L (mag), I2C
- Penyearah arah angin     -> wind_true = (wind_relative + heading) % 360

PERUBAHAN rev3 -- MITIGASI KEGAGALAN I2C (ENXIO / Errno 6)
----------------------------------------------------------
Gejala: VCM5883L gagal ACK alamat (ENXIO) secara intermiten. Muncul setelah
GPS ditambahkan ke port USB 3. Akar masalah DIDUGA KELISTRIKAN (sag rail
3.3V/5V akibat bertambahnya beban USB, dan/atau derau pada jalur SDA/SCL),
BUKAN software. Kode di bawah hanya MEMITIGASI supaya sistem tidak mati total;
perbaikan permanen tetap di sisi hardware (powered USB hub, LDO terpisah untuk
IMU, pull-up 2.2k, clock I2C 100 kHz, kabel lebih pendek/berpelindung).

Mitigasi yang ditambahkan:
1. IMU_PERIOD dipisah dari SAMPLE_PERIOD -- laju IMU tidak lagi terikat
   laju main loop maupun laju polling RS485.
2. Ambang kegagalan BERBASIS WAKTU (IMU_FAIL_TIMEOUT_S), bukan jumlah sampel.
   Sebelumnya MAX_CONSEC_FAIL=10 berarti 1 detik @10 Hz tapi 10 detik @1 Hz --
   inilah sebab "10 Hz mati total, 1 Hz aman".
3. REINIT_PERIOD_S 30s -> 5s, dan handle SMBus DITUTUP lalu DIBUKA ULANG saat
   re-init (fd lama bisa tertinggal dalam state buruk setelah brownout).
4. Retry singkat per-pembacaan (IMU_READ_RETRY) dengan jeda mikro, cukup untuk
   melewati glitch transien tanpa dihitung sebagai kegagalan.
5. Flush data-protection: datasheet VCM5883L menyatakan begitu salah satu dari
   register 00H..05H diakses, register data TIDAK di-update sampai byte 05H
   selesai dibaca. Block read yang putus di tengah bisa mengunci chip memegang
   data lama -> setelah setiap error dilakukan pembacaan 6-byte lalu dibuang.
6. Soft reset (0BH=0x80) di awal setiap init supaya pemulihan pasca-brownout
   selalu mulai dari state bersih. CATATAN LAPANGAN: chip ini menjatuhkan ACK
   pada write soft reset (selalu ENXIO) walau resetnya sukses -- ENXIO di sini
   sengaja diabaikan dan tidak boleh di-retry. Lihat vcm5883_init().
7. Log kegagalan di-rate-limit + ringkasan kesehatan per-chip tiap
   IMU_HEALTH_REPORT_S detik. Ringkasan ini yang dipakai untuk uji A/B GPS:
   bandingkan error-rate saat GPS dicolok vs dicabut.

PERUBAHAN rev4 -- IMU ESENSIAL vs OPSIONAL (ITG3200 tidak wajib)
------------------------------------------------------------------
Ditemukan di lapangan: ITG3200 (gyro, 0x68) absen total dari i2cdetect,
sementara ADXL345 (0x53) dan VCM5883L (0x0C) tetap terbaca normal. Sebelumnya
_try_init() bersifat all-or-nothing -- satu chip gagal init membuat SELURUH
IMU (termasuk heading) dinyatakan tidak tersedia, padahal:

    calculate_heading(mx, my, mz, roll, pitch) HANYA memakai accel + mag.
    Gyro tidak pernah masuk ke perhitungan heading / wind_true, ia cuma
    dicatat sebagai data mentah 10-DOF ke CSV & MQTT topic buoyV3/imu.

Karena itu ADXL345 + VCM5883L sekarang diperlakukan sebagai "esensial"
(wajib untuk heading), sedangkan ITG3200 diperlakukan "opsional" -- diinit
dan dibaca secara independen, boleh gagal terus-menerus tanpa mematikan
heading maupun anemometer/wind-direction (yang memang sudah di thread/bus
terpisah sejak awal). Saat gyro gagal, kolom gyro_*_dps di CSV & field gx/gy/gz
di MQTT cukup berisi None; tidak ada langkah lain yang perlu diubah karena
konsumen data sudah menerima None secara graceful (lihat save_imu_to_csv
dan _r()).

CATATAN REGISTER VCM5883L (sesuai datasheet VTran Tech, JANGAN diubah asal)
--------------------------------------------------------------------------
Peta register VCM5883L BERBEDA dari QMC5883L. Yang benar:
  00H..05H  data XOUT/YOUT/ZOUT (LSB dulu, 16-bit two's complement)
  0AH       Control Register 2: bit[3:2]=ODR, bit[0]=MODE
            bit[7:4] WAJIB ditulis 0100b saat inisialisasi
            ODR: 00=200Hz (default), 01=100Hz, 10=50Hz, 11=10Hz
            MODE: 0=Standby, 1=Normal
  0BH       Control Register 1: bit[7]=SOFT_RST, bit[1:0]=SET/RESET mode
            SET/RESET: 00=SET/RESET, 01=SET, 10=NO SET/RESET, 11=reserved
  0CH       Chip ID (read only) = 0x82
TIDAK ADA register status/DRDY. DRDY hanya tersedia sebagai PIN fisik.
Peringatan datasheet: menulis '1' pada bit yang tidak terdefinisi dapat sangat
mempengaruhi fungsi dan performa chip, bahkan berpotensi merusaknya.

Keterbatasan (untuk dokumentasi metodologi):
- Sensor angin Modbus internalnya umumnya <=1 Hz; polling RS485 dapat
  meng-oversample (nilai berulang). IMU mudah >10 Hz (ODR chip 200 Hz).
- Heading masih MAGNETIK (belum dikoreksi deklinasi).
- Akurasi heading bergantung kalibrasi hard/soft-iron VCM5883L.
- Pada kegagalan daya mendadak, hingga FLUSH_EVERY_N sampel terakhir
  (~1 detik) berpotensi hilang karena flush dikelompokkan.
- Sejak rev4: gyro (gx/gy/gz) bisa bernilai None kapan saja tanpa
  memengaruhi Heading_deg / WindTrue_deg -- cek imu_available_gyro
  (atau kolom gyro_*_dps == None) kalau perlu tahu status gyro secara
  eksplisit saat analisis data.
"""

import csv
from datetime import datetime
import json
import time
import os
import math
import struct
import threading
import socket

import smbus2
import paho.mqtt.client as mqtt

from anemometer import read_sensor_data as read_anemometer
from WindDirect import read_sensor_data as read_wind_direction
from quality_control import anem_qc, wind_qc

# =====================================================
# KONFIGURASI LAJU & TIMING
# =====================================================

SAMPLE_PERIOD = 0.1                      # main loop: 10 Hz
IMU_PERIOD = 0.1                         # laju baca IMU (terpisah dari main loop)
SMOOTHING_WINDOW_S = 1.0                 # jendela smoothing heading/wind_true (detik)

# Ukuran buffer dihitung per-domain agar jendela tetap 1 detik walau laju beda.
WIND_BUFFER_SIZE = max(1, int(round(SMOOTHING_WINDOW_S / SAMPLE_PERIOD)))
IMU_BUFFER_SIZE = max(1, int(round(SMOOTHING_WINDOW_S / IMU_PERIOD)))

# --- RS485 single-bus (anemometer ID 3 + wind direction ID 2 dibaca berurutan) ---
# DIPISAH dari SAMPLE_PERIOD: di 9600 baud, dua transaksi Modbus berurutan
# hampir pasti >100 ms, jadi target 0.1 s hanya akan membuat thread resync terus.
RS485_POLL_PERIOD = 1.0                  # target laju polling bus RS485 (detik)
RS485_INTER_FRAME_DELAY = 0.01           # jeda antar-slave pada satu bus (detik)

PUBLISH_EVERY_N = 1                      # publish MQTT tiap N sampel
FLUSH_EVERY_N = max(1, int(round(1.0 / SAMPLE_PERIOD)))         # flush CSV ~tiap 1 detik
STATUS_PRINT_EVERY_N = max(1, int(round(1.0 / SAMPLE_PERIOD)))  # cetak status ~tiap 1 detik
CONN_CHECK_PERIOD_S = 5.0                # cek internet tiap 5 detik (thread terpisah)

# =====================================================
# KONFIGURASI KETAHANAN IMU
# =====================================================

IMU_FAIL_TIMEOUT_S = 3.0      # berapa lama gagal beruntun sebelum heading dinyatakan hilang
REINIT_PERIOD_S = 5.0         # jeda antar-percobaan init ulang (dulu 30s -- terlalu lama)
IMU_READ_RETRY = 2            # percobaan tambahan per-pembacaan saat OSError
IMU_RETRY_DELAY_S = 0.002     # jeda antar-retry (2 ms)
# IMU_HEALTH_REPORT_S = 30.0    # interval ringkasan kesehatan I2C
IMU_LOG_MIN_INTERVAL_S = 2.0  # rate-limit log kegagalan (hindari banjir log @10 Hz)

# Ambang berbasis waktu -> otomatis konsisten saat IMU_PERIOD diubah.
# Dipakai untuk DUA domain independen sejak rev4: kegagalan esensial
# (accel+mag, mematikan heading) dan kegagalan gyro (opsional, tidak
# mematikan apa pun selain kolom gyro_*_dps).
MAX_CONSEC_FAIL = max(5, int(round(IMU_FAIL_TIMEOUT_S / IMU_PERIOD)))

# =====================================================
# KONFIGURASI MQTT
# =====================================================

# CATATAN JARINGAN (akar masalah terverifikasi 14-Jun-2026):
# DNS di lokasi buoy meresolusi 'c-greenproject.org' ke IP Fortinet block-page
# (208.91.112.55) atau ke IPv6 yang unreachable -> [Errno 101]. IP broker asli
# 77.37.63.21 terbukti reachable (CONNACK rc=0). Pakai IP langsung, lewati DNS.
MQTT_BROKER = "77.37.63.21"
MQTT_PORT = 1883
MQTT_USER = "adminvps"
MQTT_PASSWORD = "pwdMQTT@123"
MQTT_TOPIC_WIND = "buoyV3/wind"
MQTT_TOPIC_IMU = "buoyV3/imu"
# MQTT_TOPIC_HEALTH = "buoyV3/health"   # ringkasan kesehatan I2C (baru)
mqtt_connected = False


def _on_connect(c, userdata, flags, reason_code, properties):
    global mqtt_connected
    mqtt_connected = (reason_code == 0)
    print(f"[Wind] MQTT on_connect rc={reason_code} -> connected={mqtt_connected}")


def _on_disconnect(c, userdata, flags, reason_code, properties):
    global mqtt_connected
    mqtt_connected = False
    print(f"[Wind] MQTT terputus (rc={reason_code}), reconnect otomatis di background...")


client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="buoyV3_wind")
client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
client.on_connect = _on_connect
client.on_disconnect = _on_disconnect
client.reconnect_delay_set(min_delay=1, max_delay=60)
client.connect_async(MQTT_BROKER, MQTT_PORT, keepalive=60)
client.loop_start()

# =====================================================
# KONFIGURASI I2C (IMU KOMPAS)
# =====================================================

I2C_BUS_NUM = 3       # OPi5: IMU terdeteksi di bus 3 (verifikasi: i2cdetect -y 3)
ADXL345_ADDR = 0x53   # accelerometer (roll/pitch)
ITG3200_ADDR = 0x68   # gyroscope (raw rate, dps)
VCM5883_ADDR = 0x0C   # magnetometer (heading)

ITG3200_LSB_PER_DPS = 14.375   # full-scale +-2000 dps (datasheet ITG3200)

# --- Register VCM5883L (datasheet VTran Tech) ---
VCM_REG_DATA = 0x00      # 00H..05H : XOUT/YOUT/ZOUT, LSB dulu
VCM_REG_CTRL1 = 0x0B     # SOFT_RST[7], SET/RESET[1:0]
VCM_REG_CTRL2 = 0x0A     # bit[7:4]=0100 wajib, ODR[3:2], MODE[0]
VCM_REG_CHIPID = 0x0C
VCM_CHIPID_EXPECTED = 0x82

VCM_CTRL2_BASE = 0x40    # bit[7:4] = 0100b -- WAJIB, jangan diutak-atik
VCM_ODR_200HZ = 0b00 << 2
VCM_ODR_100HZ = 0b01 << 2
VCM_ODR_50HZ = 0b10 << 2
VCM_ODR_10HZ = 0b11 << 2
VCM_MODE_STANDBY = 0x00
VCM_MODE_NORMAL = 0x01

VCM_SOFT_RESET = 0x80
VCM_SR_SET_RESET = 0x00   # offset cancellation penuh (rekomendasi datasheet)
VCM_SR_SET_ONLY = 0x01    # nilai yang dipakai rev2

# ODR 200 Hz = 20x laju polling 10 Hz -> tidak akan pernah jadi bottleneck.
# Hasilnya 0x40 | 0x00 | 0x01 = 0x41 (sama dengan rev2, memang sudah benar).
VCM_CTRL2_VALUE = VCM_CTRL2_BASE | VCM_ODR_200HZ | VCM_MODE_NORMAL
# Dipertahankan 0x01 (SET only) agar karakteristik pembacaan tidak berubah dari
# rev2. Bila ingin offset cancellation penuh, ganti ke VCM_SR_SET_RESET lalu
# KALIBRASI ULANG hard/soft-iron -- nilai mentah akan bergeser.
VCM_CTRL1_VALUE = VCM_SR_SET_ONLY

# Soft reset memberi state bersih saat pemulihan pasca-brownout atau setelah
# chip tertinggal di kondisi setengah jadi. TERVERIFIKASI di perangkat: chip
# mereset dirinya seketika dan MENJATUHKAN ACK byte terakhir, sehingga master
# selalu melihat ENXIO -- padahal resetnya BERHASIL (chip ID tetap 0x82 dan
# CTRL2 kembali ke 0x00). Karena itu write-nya tidak boleh di-retry dan
# statusnya tidak boleh dipercaya. Set False untuk kembali persis ke urutan
# init rev2 (tanpa soft reset).
VCM_USE_SOFT_RESET = True

# =====================================================
# STATISTIK KESEHATAN I2C (untuk uji A/B GPS)
# =====================================================

imu_health = {
    "ok": 0,
    "fail_adxl": 0,
    "fail_itg": 0,
    "fail_vcm": 0,
    "reinit": 0,
    "last_error": "",
    "window_start": time.monotonic(),
}
health_lock = threading.Lock()


def _health_bump(key, err=""):
    with health_lock:
        imu_health[key] = imu_health.get(key, 0) + 1
        if err:
            imu_health["last_error"] = err


def _health_snapshot_and_reset():
    """Ambil ringkasan jendela berjalan lalu reset counter."""
    with health_lock:
        elapsed = max(1e-6, time.monotonic() - imu_health["window_start"])
        total = (imu_health["ok"] + imu_health["fail_adxl"]
                 + imu_health["fail_itg"] + imu_health["fail_vcm"])
        snap = {
            "window_s": round(elapsed, 1),
            "reads": total,
            "ok": imu_health["ok"],
            "fail_adxl": imu_health["fail_adxl"],
            "fail_itg": imu_health["fail_itg"],
            "fail_vcm": imu_health["fail_vcm"],
            "reinit": imu_health["reinit"],
            "fail_pct": round(100.0 * (total - imu_health["ok"]) / total, 2) if total else 0.0,
            "last_error": imu_health["last_error"],
        }
        imu_health.update(ok=0, fail_adxl=0, fail_itg=0, fail_vcm=0,
                          reinit=0, window_start=time.monotonic())
    return snap


# =====================================================
# FUNGSI IMU: INISIALISASI & PEMBACAAN
# =====================================================

def _i2c_retry(fn, *args, **kwargs):
    """Jalankan operasi I2C dengan retry singkat.

    Glitch kelistrikan transien (sag rail, derau saat GPS/USB aktif) sering
    hanya berlangsung ratusan mikrodetik. Satu-dua retry berjeda 2 ms cukup
    untuk melewatinya, sehingga tidak dihitung sebagai kegagalan sungguhan.
    """
    last_exc = None
    for attempt in range(1 + IMU_READ_RETRY):
        try:
            return fn(*args, **kwargs)
        except OSError as e:
            last_exc = e
            if attempt < IMU_READ_RETRY:
                time.sleep(IMU_RETRY_DELAY_S)
    raise last_exc


def adxl345_init(bus):
    _i2c_retry(bus.write_byte_data, ADXL345_ADDR, 0x2D, 0x08)   # POWER_CTL: measure
    _i2c_retry(bus.write_byte_data, ADXL345_ADDR, 0x31, 0x08)   # DATA_FORMAT: full-res
    time.sleep(0.05)


def get_accel(bus):
    data = _i2c_retry(bus.read_i2c_block_data, ADXL345_ADDR, 0x32, 6)
    x = struct.unpack('<h', bytes([data[0], data[1]]))[0] * 0.004
    y = struct.unpack('<h', bytes([data[2], data[3]]))[0] * 0.004
    z = struct.unpack('<h', bytes([data[4], data[5]]))[0] * 0.004
    return x, y, z


def itg3200_init(bus):
    # Reset (0x3E=0x80), DLPF 42 Hz @1kHz (0x16=0x18), sample-rate div (0x15=0x09),
    # clock ref = X-gyro PLL (0x3E=0x01).
    _i2c_retry(bus.write_byte_data, ITG3200_ADDR, 0x3E, 0x80)
    time.sleep(0.1)
    _i2c_retry(bus.write_byte_data, ITG3200_ADDR, 0x16, 0x18)
    _i2c_retry(bus.write_byte_data, ITG3200_ADDR, 0x15, 0x09)
    _i2c_retry(bus.write_byte_data, ITG3200_ADDR, 0x3E, 0x01)
    time.sleep(0.1)


def get_gyro(bus):
    # Register 0x1D..0x22, big-endian signed (datasheet ITG3200). Skala -> dps.
    data = _i2c_retry(bus.read_i2c_block_data, ITG3200_ADDR, 0x1D, 6)
    gx = struct.unpack('>h', bytes([data[0], data[1]]))[0] / ITG3200_LSB_PER_DPS
    gy = struct.unpack('>h', bytes([data[2], data[3]]))[0] / ITG3200_LSB_PER_DPS
    gz = struct.unpack('>h', bytes([data[4], data[5]]))[0] / ITG3200_LSB_PER_DPS
    return gx, gy, gz


def vcm5883_init(bus):
    """Init VCM5883L dengan soft reset lebih dulu.

    Soft reset penting untuk PEMULIHAN: setelah brownout, chip bisa tertinggal
    di state setengah jadi (mis. data-protection masih aktif, atau kembali ke
    standby tanpa kita tahu). Reset mengembalikan semua register ke default,
    lalu kita program ulang dari nol.
    """
    # PENTING: soft reset TIDAK dibungkus _i2c_retry dan ENXIO-nya diabaikan.
    # Chip mereset dirinya seketika lalu menjatuhkan ACK byte terakhir, jadi
    # write ini SELALU tampak gagal walau resetnya berhasil. Membungkusnya
    # dengan retry justru memicu reset berulang-ulang. Keberhasilan divalidasi
    # lewat pembacaan chip ID di bawah, bukan lewat status write ini.
    if VCM_USE_SOFT_RESET:
        try:
            bus.write_byte_data(VCM5883_ADDR, VCM_REG_CTRL1, VCM_SOFT_RESET)
        except OSError:
            pass           # ACK hilang -- perilaku normal chip ini
        time.sleep(0.05)   # PORT max 350 us; 50 ms sangat longgar

    _i2c_retry(bus.write_byte_data, VCM5883_ADDR, VCM_REG_CTRL1, VCM_CTRL1_VALUE)
    time.sleep(0.01)
    _i2c_retry(bus.write_byte_data, VCM5883_ADDR, VCM_REG_CTRL2, VCM_CTRL2_VALUE)
    time.sleep(0.05)

    chip_id = _i2c_retry(bus.read_byte_data, VCM5883_ADDR, VCM_REG_CHIPID)
    if chip_id != VCM_CHIPID_EXPECTED:
        raise RuntimeError(
            f"VCM5883L chip ID tak sesuai: 0x{chip_id:02X} "
            f"(harusnya 0x{VCM_CHIPID_EXPECTED:02X}) -- "
            "indikasi kuat masalah daya/derau pada bus I2C"
        )
    print(f"[Wind] VCM5883L Chip ID : 0x{chip_id:02X} | "
          f"CTRL2=0x{VCM_CTRL2_VALUE:02X} (ODR 200 Hz, Normal) | "
          f"CTRL1=0x{VCM_CTRL1_VALUE:02X}")


def get_mag_raw(bus):
    data = _i2c_retry(bus.read_i2c_block_data, VCM5883_ADDR, VCM_REG_DATA, 6)
    mx = struct.unpack('<h', bytes([data[0], data[1]]))[0]
    my = struct.unpack('<h', bytes([data[2], data[3]]))[0]
    mz = struct.unpack('<h', bytes([data[4], data[5]]))[0]
    return mx, my, mz


def vcm5883_flush_data(bus):
    """Bebaskan data-protection VCM5883L setelah pembacaan gagal.

    Datasheet: begitu salah satu register 00H..05H diakses, register data tidak
    di-update sampai byte terakhir (05H) selesai dibaca. Kalau block read putus
    di tengah karena NACK, chip bisa tertahan memegang data lama selamanya.
    Satu pembacaan 6-byte penuh (hasilnya dibuang) melepaskan kunci itu.
    """
    try:
        bus.read_i2c_block_data(VCM5883_ADDR, VCM_REG_DATA, 6)
    except OSError:
        pass   # kalau ini pun gagal, re-init yang akan menanganinya


def _open_bus():
    return smbus2.SMBus(I2C_BUS_NUM)


def _close_bus(bus):
    try:
        bus.close()
    except Exception:
        pass


def _try_init_essential(bus):
    """Inisialisasi ADXL345 + VCM5883L -- KEDUANYA wajib untuk heading.

    Bus SELALU dibuka ulang di sini. Setelah brownout, file descriptor lama
    bisa tertinggal dalam state buruk sehingga init ulang di atas fd yang sama
    gagal terus -- inilah sebab 're-init tiap 30 detik' di rev2 tak pernah
    berhasil memulihkan.

    ITG3200 SENGAJA TIDAK di sini (lihat rev4 di header modul): gyro tidak
    dipakai calculate_heading(), jadi kegagalannya tidak boleh menyeret
    accel+mag yang justru sudah sehat. Init gyro ditangani terpisah oleh
    _try_init_gyro().
    """
    _close_bus(bus)
    time.sleep(0.05)
    new_bus = _open_bus()
    stage = "buka bus"
    try:
        # Nama tahap dicatat supaya pesan error menyebut chip pelakunya --
        # tanpa ini, 'Init IMU gagal' tidak memberi tahu apa pun.
        stage = "ADXL345"
        adxl345_init(new_bus)
        stage = "VCM5883L"
        vcm5883_init(new_bus)
        print("[Wind] IMU esensial terinisialisasi (ADXL345 + VCM5883L) -- heading tersedia")
        return True, new_bus
    except Exception as e:
        print(f"[Wind] Init IMU esensial gagal pada tahap {stage}: {e}")
        _health_bump("reinit", f"init/{stage}: {e}")
        return False, new_bus


def _try_init_gyro(bus):
    """Inisialisasi ITG3200 secara independen dari accel+mag.

    Dipanggil di ATAS bus yang sama dengan chip esensial (tanpa menutup/
    membuka ulang fd -- itu sudah terjadi di _try_init_essential). Kalau
    gagal, heading/wind_true TIDAK terpengaruh sama sekali; yang hilang cuma
    kolom gyro_*_dps di CSV & field gx/gy/gz di MQTT topic buoyV3/imu.
    """
    try:
        itg3200_init(bus)
        print("[Wind] ITG3200 (gyro) terinisialisasi -- data gyro tersedia")
        return True
    except Exception as e:
        print(f"[Wind] Init ITG3200 (gyro) gagal: {e} -- heading TETAP berjalan tanpa gyro")
        _health_bump("reinit", f"init/ITG3200: {e}")
        return False


# =====================================================
# FUNGSI KOMPUTASI HEADING & ARAH ANGIN
# =====================================================

def calculate_roll_pitch(ax, ay, az):
    roll = math.atan2(ay, az)
    pitch = math.atan2(-ax, math.sqrt(ay * ay + az * az))
    return math.degrees(roll), math.degrees(pitch)


def calculate_heading(mx, my, mz, roll, pitch):
    roll_rad = math.radians(roll)
    pitch_rad = math.radians(pitch)

    mx2 = (mx * math.cos(pitch_rad)
           + mz * math.sin(pitch_rad))

    my2 = (mx * math.sin(roll_rad) * math.sin(pitch_rad)
           + my * math.cos(roll_rad)
           - mz * math.sin(roll_rad) * math.cos(pitch_rad))

    heading = math.degrees(math.atan2(my2, mx2))
    if heading < 0:
        heading += 360
    return heading


def degree_to_direction(degree):
    directions = [
        "North", "North-Northeast", "Northeast", "East-Northeast",
        "East", "East-Southeast", "Southeast", "South-Southeast",
        "South", "South-Southwest", "Southwest", "West-Southwest",
        "West", "West-Northwest", "Northwest", "North-Northwest"
    ]
    index = round(degree / 22.5) % 16
    return directions[index]


def circular_moving_average(buffer, value, size):
    """Rata-rata bergerak untuk besaran sudut (derajat) memakai circular mean,
    menghindari kesalahan wrap-around 0/360 deg."""
    buffer.append(value)
    if len(buffer) > size:
        buffer.pop(0)
    sin_sum = sum(math.sin(math.radians(a)) for a in buffer)
    cos_sum = sum(math.cos(math.radians(a)) for a in buffer)
    mean = math.degrees(math.atan2(sin_sum, cos_sum))
    if mean < 0:
        mean += 360
    return mean


# =====================================================
# KONEKTIVITAS (INDIKATOR INTERNET UNTUK LOGGING)
# =====================================================

internet_ok = False


def is_connected():
    try:
        socket.create_connection(("8.8.8.8", 53), timeout=3)
        return True
    except OSError:
        return False


def connectivity_monitor():
    """Hanya meng-update indikator internet untuk logging (net=ON/OFF).
    Koneksi & reconnect MQTT ditangani paho lewat loop_start() +
    reconnect_delay_set()."""
    global internet_ok
    while True:
        internet_ok = is_connected()
        time.sleep(CONN_CHECK_PERIOD_S)


# =====================================================
# GLOBAL STATE (THREAD-SAFE VIA LOCK)
# =====================================================

lock = threading.Lock()
anemometer_data_global = {}
wind_direction_data_global = {}
compass_data_global = {"roll": None, "pitch": None, "heading": None, "available": False}
imu_raw_global = {
    "ax": None, "ay": None, "az": None,
    "gx": None, "gy": None, "gz": None,
    "mx": None, "my": None, "mz": None,
    "available": False,
    "gyro_available": False,
}

# =====================================================
# CSV LOGGER (HANDLE PERSISTEN)
# =====================================================

FIELDNAMES = [
    "timestamp",
    "WindSpeed_ms", "WindSpeed_flag", "Beaufort_scale",
    "WindAngle_deg", "WindAngle_flag", "WindDirection",
    "Roll_deg", "Pitch_deg",
    "Heading_deg", "Heading_direction",
    "WindTrue_deg", "WindTrue_direction",
]

_csv_state = {"date": None, "file": None, "writer": None, "count": 0}


def save_to_csv(data):
    """Tulis satu baris. File dibuka sekali per hari; flush dikelompokkan
    setiap FLUSH_EVERY_N baris untuk mengurangi keausan SD card di 10 Hz."""
    date_str = time.strftime("%Y-%m-%d")

    if _csv_state["date"] != date_str:
        if _csv_state["file"]:
            try:
                _csv_state["file"].flush()
                _csv_state["file"].close()
            except Exception:
                pass
        filename = f"/home/orangepi/data/wind/wind_{date_str}.csv"
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        need_header = (not os.path.exists(filename)) or os.path.getsize(filename) == 0
        f = open(filename, "a", newline="")
        w = csv.DictWriter(f, fieldnames=FIELDNAMES)
        if need_header:
            w.writeheader()
        _csv_state.update(date=date_str, file=f, writer=w, count=0)

    _csv_state["writer"].writerow(data)
    _csv_state["count"] += 1
    if _csv_state["count"] >= FLUSH_EVERY_N:
        _csv_state["file"].flush()
        _csv_state["count"] = 0


# --- CSV terpisah untuk raw 10 DOF ---
IMU_FIELDNAMES = [
    "timestamp",
    "accel_x_g", "accel_y_g", "accel_z_g",
    "gyro_x_dps", "gyro_y_dps", "gyro_z_dps",
    "mag_x", "mag_y", "mag_z",
]

_imu_csv_state = {"date": None, "file": None, "writer": None, "count": 0}


def save_imu_to_csv(data):
    """Tulis satu baris raw IMU ke /home/orangepi/data/imu/imu_<tgl>.csv."""
    date_str = time.strftime("%Y-%m-%d")

    if _imu_csv_state["date"] != date_str:
        if _imu_csv_state["file"]:
            try:
                _imu_csv_state["file"].flush()
                _imu_csv_state["file"].close()
            except Exception:
                pass
        filename = f"/home/orangepi/data/imu/imu_{date_str}.csv"
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        need_header = (not os.path.exists(filename)) or os.path.getsize(filename) == 0
        f = open(filename, "a", newline="")
        w = csv.DictWriter(f, fieldnames=IMU_FIELDNAMES)
        if need_header:
            w.writeheader()
        _imu_csv_state.update(date=date_str, file=f, writer=w, count=0)

    _imu_csv_state["writer"].writerow(data)
    _imu_csv_state["count"] += 1
    if _imu_csv_state["count"] >= FLUSH_EVERY_N:
        _imu_csv_state["file"].flush()
        _imu_csv_state["count"] = 0


# =====================================================
# THREAD PEMBACAAN SENSOR RS485 (SATU BUS, DUA SLAVE)
# =====================================================

def read_rs485_sensors():
    """Akuisisi DUA sensor RS485 pada SATU bus half-duplex, dibaca BERURUTAN
    dari satu thread:
        - Slave ID 3 -> anemometer (wind speed)
        - Slave ID 2 -> wind direction (angle/direction)

    RS485 Modbus RTU half-duplex multi-drop hanya boleh punya satu transaksi
    (request->response) di kabel pada satu waktu, sehingga transaksi slave 3
    tuntas dulu sebelum slave 2 dimulai.
    """
    global anemometer_data_global, wind_direction_data_global
    next_t = time.monotonic()

    while True:
        try:
            a = read_anemometer() or {}
            with lock:
                anemometer_data_global = a
        except Exception as e:
            print(f"[Wind] Error reading anemometer (ID 3): {e}")

        if RS485_INTER_FRAME_DELAY > 0:
            time.sleep(RS485_INTER_FRAME_DELAY)

        try:
            w = read_wind_direction() or {}
            with lock:
                wind_direction_data_global = w
        except Exception as e:
            print(f"[Wind] Error reading wind direction (ID 2): {e}")

        next_t += RS485_POLL_PERIOD
        delay = next_t - time.monotonic()
        if delay > 0:
            time.sleep(delay)
        else:
            next_t = time.monotonic()


# =====================================================
# THREAD PEMBACAAN IMU KOMPAS (DENGAN MITIGASI)
# =====================================================

def read_compass_sensor():
    global compass_data_global, imu_raw_global

    bus = _open_bus()
    essential_ok, bus = _try_init_essential(bus)
    gyro_ok = _try_init_gyro(bus) if essential_ok else False
    last_essential_init_try = time.monotonic()
    last_gyro_init_try = time.monotonic()

    heading_buffer = []
    essential_consec_fail = 0
    gyro_consec_fail = 0
    last_log_t = 0.0
    last_gyro_log_t = 0.0
    last_health_t = time.monotonic()
    next_t = time.monotonic()

    while True:
        now = time.monotonic()

        # --- Init ulang periodik bila heading (essential) sedang tidak tersedia ---
        # Ini juga membuka ulang bus, jadi gyro perlu di-init ulang mengikuti.
        if not essential_ok and now - last_essential_init_try >= REINIT_PERIOD_S:
            essential_ok, bus = _try_init_essential(bus)
            last_essential_init_try = time.monotonic()
            if essential_ok:
                essential_consec_fail = 0
                gyro_ok = _try_init_gyro(bus)
                gyro_consec_fail = 0
                last_gyro_init_try = time.monotonic()

        # --- Init ulang periodik KHUSUS gyro (tidak menyentuh accel/mag yang sehat) ---
        elif essential_ok and not gyro_ok and now - last_gyro_init_try >= REINIT_PERIOD_S:
            gyro_ok = _try_init_gyro(bus)
            last_gyro_init_try = time.monotonic()
            if gyro_ok:
                gyro_consec_fail = 0

        if essential_ok:
            try:
                # try/except TERPISAH per chip agar log menyebut pelakunya.
                # Ini yang memungkinkan uji A/B GPS: kalau hanya fail_vcm yang
                # naik, masalahnya di magnetometer; kalau keduanya naik,
                # masalahnya di rail daya atau jalur bus secara keseluruhan.
                try:
                    ax, ay, az = get_accel(bus)
                except Exception as e:
                    _health_bump("fail_adxl", str(e))
                    raise RuntimeError(f"ADXL345: {e}")
                try:
                    mx, my, mz = get_mag_raw(bus)
                except Exception as e:
                    _health_bump("fail_vcm", str(e))
                    raise RuntimeError(f"VCM5883L: {e}")

                roll, pitch = calculate_roll_pitch(ax, ay, az)
                heading_raw = calculate_heading(mx, my, mz, roll, pitch)
                heading = circular_moving_average(heading_buffer, heading_raw,
                                                  IMU_BUFFER_SIZE)
                essential_consec_fail = 0
                _health_bump("ok")

                # Gyro dibaca TERPISAH -- kegagalannya tidak boleh membatalkan
                # heading yang barusan berhasil dihitung dari accel+mag.
                gx = gy = gz = None
                if gyro_ok:
                    try:
                        gx, gy, gz = get_gyro(bus)
                        gyro_consec_fail = 0
                    except Exception as e:
                        _health_bump("fail_itg", str(e))
                        gyro_consec_fail += 1
                        if time.monotonic() - last_gyro_log_t >= IMU_LOG_MIN_INTERVAL_S:
                            print(f"[Wind] Gyro read gagal ({gyro_consec_fail}x beruntun): "
                                  f"{e} -- heading tidak terpengaruh")
                            last_gyro_log_t = time.monotonic()
                        if gyro_consec_fail >= MAX_CONSEC_FAIL:
                            print(f"[Wind] Gyro dinyatakan HILANG -- re-init dalam "
                                  f"{REINIT_PERIOD_S:.0f}s (heading TETAP jalan)")
                            gyro_ok = False
                            last_gyro_init_try = time.monotonic()

                with lock:
                    compass_data_global = {"roll": roll, "pitch": pitch,
                                           "heading": heading, "available": True}
                    imu_raw_global = {"ax": ax, "ay": ay, "az": az,
                                      "gx": gx, "gy": gy, "gz": gz,
                                      "mx": mx, "my": my, "mz": mz,
                                      "available": True, "gyro_available": gyro_ok}

            except Exception as e:
                essential_consec_fail += 1

                # Bebaskan kunci data-protection VCM5883L (lihat docstring).
                vcm5883_flush_data(bus)

                # Log di-rate-limit: di 10 Hz, mencetak tiap kegagalan akan
                # membanjiri journal dan justru memperlambat loop.
                if time.monotonic() - last_log_t >= IMU_LOG_MIN_INTERVAL_S:
                    print(f"[Wind] Compass read gagal ({essential_consec_fail}x beruntun, "
                          f"~{essential_consec_fail * IMU_PERIOD:.1f}s): {e}")
                    last_log_t = time.monotonic()

                if essential_consec_fail >= MAX_CONSEC_FAIL:
                    print(f"[Wind] IMU esensial dinyatakan HILANG setelah "
                          f"{IMU_FAIL_TIMEOUT_S:.0f}s gagal beruntun -- "
                          f"re-init dalam {REINIT_PERIOD_S:.0f}s")
                    essential_ok = False
                    gyro_ok = False
                    last_essential_init_try = time.monotonic()
                    with lock:
                        compass_data_global = {"roll": None, "pitch": None,
                                               "heading": None, "available": False}
                        imu_raw_global = {k: None for k in imu_raw_global} | {
                            "available": False, "gyro_available": False}
                    heading_buffer.clear()
                # Kegagalan transien (< MAX_CONSEC_FAIL): JANGAN nihilkan state --
                # nilai beberapa ratus ms lalu masih sah untuk smoothing 1 s.

        # # --- Ringkasan kesehatan periodik (bahan uji A/B GPS) ---
        # if time.monotonic() - last_health_t >= IMU_HEALTH_REPORT_S:
        #     snap = _health_snapshot_and_reset()
        #     print(f"[IMU-Health] {snap['window_s']}s | reads={snap['reads']} "
        #           f"ok={snap['ok']} gagal={snap['fail_pct']}% "
        #           f"(adxl={snap['fail_adxl']} itg={snap['fail_itg']} "
        #           f"vcm={snap['fail_vcm']}) reinit={snap['reinit']}")
        #     if snap["last_error"]:
        #         print(f"[IMU-Health] error terakhir: {snap['last_error']}")
        #     if mqtt_connected:
        #         try:
        #             client.publish(MQTT_TOPIC_HEALTH,
        #                            json.dumps({"timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        #                                        **snap}), qos=0)
        #         except Exception:
        #             pass
        #     last_health_t = time.monotonic()

        next_t += IMU_PERIOD
        delay = next_t - time.monotonic()
        if delay > 0:
            time.sleep(delay)
        else:
            next_t = time.monotonic()


# =====================================================
# MAIN LOOP (PENJADWAL MONOTONIC 10 Hz)
# =====================================================

def main():
    threading.Thread(target=read_rs485_sensors, daemon=True).start()
    threading.Thread(target=read_compass_sensor, daemon=True).start()
    threading.Thread(target=connectivity_monitor, daemon=True).start()

    print(f"[Wind] Wind Monitoring Started @ {1/SAMPLE_PERIOD:.0f} Hz")
    print("[Wind] Sensors: Anemometer(ID3) + Wind Direction(ID2) @ SATU bus RS485 + IMU Compass")
    print("[Wind] True-wind correction: ENABLED | Quality Control: ENABLED")
    print(f"[Wind] Smoothing window = {SMOOTHING_WINDOW_S}s "
          f"(wind={WIND_BUFFER_SIZE} sampel, imu={IMU_BUFFER_SIZE} sampel)")
    print(f"[Wind] RS485 poll period = {RS485_POLL_PERIOD}s | "
          f"inter-frame delay = {RS485_INTER_FRAME_DELAY}s")
    print(f"[Wind] IMU period = {IMU_PERIOD}s ({1/IMU_PERIOD:.0f} Hz) | "
          f"toleransi gagal = {IMU_FAIL_TIMEOUT_S}s ({MAX_CONSEC_FAIL} sampel) | "
          f"re-init = {REINIT_PERIOD_S}s")

    wind_true_buffer = []
    sample_i = 0
    next_t = time.monotonic()

    while True:
        try:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

            with lock:
                anemometer_data = anemometer_data_global.copy()
                wind_direction_data = wind_direction_data_global.copy()
                compass_data = compass_data_global.copy()
                imu_raw = imu_raw_global.copy()

            raw_anemometer_speed = anemometer_data.get("anemometer_speed")
            raw_wind_angle = wind_direction_data.get("angle")

            speed_qc = anem_qc.clean(raw_anemometer_speed)
            angle_qc = wind_qc.clean(raw_wind_angle)

            wind_speed = speed_qc.value      # float | None
            wind_angle = angle_qc.value      # float | None

            heading = compass_data.get("heading")
            roll = compass_data.get("roll")
            pitch = compass_data.get("pitch")

            if wind_angle is not None and heading is not None:
                wind_true_raw = (wind_angle + heading) % 360
                wind_true = circular_moving_average(wind_true_buffer, wind_true_raw,
                                                    WIND_BUFFER_SIZE)
                wind_true_direction = degree_to_direction(wind_true)
            else:
                wind_true = None
                wind_true_direction = "UNKNOWN"
                wind_true_buffer.clear()

            wind_data = {
                "timestamp": timestamp,
                "WindSpeed_ms": wind_speed,
                "WindSpeed_flag": speed_qc.flag,
                "Beaufort_scale": anemometer_data.get("beaufort_scale"),
                "WindAngle_deg": wind_angle,
                "WindAngle_flag": angle_qc.flag,
                "WindDirection": wind_direction_data.get("direction"),
                "Roll_deg": round(roll, 2) if roll is not None else None,
                "Pitch_deg": round(pitch, 2) if pitch is not None else None,
                "Heading_deg": round(heading, 2) if heading is not None else None,
                "Heading_direction": degree_to_direction(heading) if heading is not None else "UNKNOWN",
                "WindTrue_deg": round(wind_true, 2) if wind_true is not None else None,
                "WindTrue_direction": wind_true_direction,
            }

            save_to_csv(wind_data)

            def _r(v, n):
                return round(v, n) if isinstance(v, (int, float)) else None

            imu_record = {
                "timestamp": timestamp,
                "accel_x_g": _r(imu_raw.get("ax"), 4),
                "accel_y_g": _r(imu_raw.get("ay"), 4),
                "accel_z_g": _r(imu_raw.get("az"), 4),
                "gyro_x_dps": _r(imu_raw.get("gx"), 3),
                "gyro_y_dps": _r(imu_raw.get("gy"), 3),
                "gyro_z_dps": _r(imu_raw.get("gz"), 3),
                "mag_x": imu_raw.get("mx"),
                "mag_y": imu_raw.get("my"),
                "mag_z": imu_raw.get("mz"),
            }
            save_imu_to_csv(imu_record)

            if mqtt_connected and (sample_i % PUBLISH_EVERY_N == 0):
                info = client.publish(MQTT_TOPIC_WIND, json.dumps(wind_data), qos=0)
                if info.rc != mqtt.MQTT_ERR_SUCCESS:
                    print(f"[Wind] publish gagal rc={info.rc}")
                info_imu = client.publish(MQTT_TOPIC_IMU, json.dumps(imu_record), qos=0)
                if info_imu.rc != mqtt.MQTT_ERR_SUCCESS:
                    print(f"[Wind] publish IMU gagal rc={info_imu.rc}")

            if sample_i % STATUS_PRINT_EVERY_N == 0:
                imu_flag = "ON" if compass_data.get("available") else "OFF"
                print(f"[Wind] Speed={wind_speed} m/s | True={wind_data['WindTrue_deg']} deg "
                      f"({wind_true_direction}) | imu={imu_flag} "
                      f"| net={'ON' if internet_ok else 'OFF'} "
                      f"| mqtt={'ON' if mqtt_connected else 'OFF'}")

        except Exception as e:
            print(f"[Wind] Error in main loop: {e}")

        sample_i += 1
        next_t += SAMPLE_PERIOD
        delay = next_t - time.monotonic()
        if delay > 0:
            time.sleep(delay)
        else:
            next_t = time.monotonic()


if __name__ == "__main__":
    main()