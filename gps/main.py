#!/usr/bin/python3
"""
GPS Acquisition Daemon - U-TEWS Buoy (Pulau Sebesi, Selat Sunda)
Sensor : 52Pi EZ-0048 / Quectel L80-R (CP2102 USB-UART) -> /dev/ttyUSB0
Output : latitude, longitude, speed (saja).

Fitur:
  - MQTT auto-reconnect (tahan putus sinyal di laut).
  - Validasi fix (mode >= 2D); titik (0,0) tidak pernah ditulis.
  - Nama file CSV harian (rollover otomatis tengah malam).
  - Throttle ke 1 Hz (sesuai update rate L80-R).
"""

import time
import csv
import json
import math
import logging
import os
from datetime import datetime, timezone

from gps import WATCH_ENABLE, WATCH_NEWSTYLE, main
import paho.mqtt.client as mqtt

# ----------------------------- KONFIGURASI ----------------------------------
MQTT_BROKER   = "77.37.63.21"
MQTT_PORT     = 1883
MQTT_USER     = "unila"
MQTT_PASSWORD = "pwdMQTT@123"      # sebaiknya pindah ke EnvironmentFile systemd
MQTT_TOPIC    = "buoyV3/gps"
MQTT_KEEPALIVE = 60

CSV_DIR        = "/home/orangepi/data/gps"
PUBLISH_PERIOD = 1.0               # detik (1 Hz, sesuai default L80-R)

# --------------------------------- LOGGING ----------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("gps-buoy")

# --------------------------------- MQTT -------------------------------------
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        log.info("MQTT terhubung ke %s:%s", MQTT_BROKER, MQTT_PORT)
    else:
        log.warning("MQTT gagal connect, rc=%s", rc)

def on_disconnect(client, userdata, rc):
    if rc != 0:
        log.warning("MQTT terputus (rc=%s); loop akan auto-reconnect.", rc)

def build_mqtt():
    client = mqtt.Client(client_id="buoy-gps", clean_session=True)
    client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.reconnect_delay_set(min_delay=1, max_delay=120)
    try:
        client.connect_async(MQTT_BROKER, MQTT_PORT, MQTT_KEEPALIVE)
    except Exception as e:
        log.warning("connect_async awal gagal (%s); loop tetap mencoba.", e)
    client.loop_start()
    return client

# --------------------------------- CSV --------------------------------------
CSV_HEADER = ["timestamp_utc", "latitude", "longitude", "speed"]

def csv_path_for_today():
    os.makedirs(CSV_DIR, exist_ok=True)
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return os.path.join(CSV_DIR, f"gps_data_{day}.csv")

def write_csv_row(row):
    path = csv_path_for_today()
    new_file = not os.path.exists(path)
    with open(path, "a", newline="") as f:
        w = csv.writer(f)
        if new_file:
            w.writerow(CSV_HEADER)
        w.writerow(row)

# ------------------------------- HELPERS ------------------------------------
def g(report, attr, default=None):
    """Ambil atribut; NaN/None -> default."""
    val = getattr(report, attr, default)
    if isinstance(val, float) and math.isnan(val):
        return default
    return val

# --------------------------------- MAIN -------------------------------------
def main():
    client = build_mqtt()
    gpsd = main(mode=WATCH_ENABLE | WATCH_NEWSTYLE)
    log.info("GPS daemon start. Menunggu fix dari L80-R (/dev/ttyUSB0)...")

    last_pub = 0.0
    have_fix_logged = False

    try:
        while True:
            report = gpsd.next()             # blocking; jangan tambah sleep

            if getattr(report, "class", "") != "TPV":
                continue

            mode = int(g(report, "mode", 0) or 0)
            lat = g(report, "lat")
            lon = g(report, "lon")
            if mode < 2 or lat is None or lon is None:
                if have_fix_logged:
                    log.warning("Fix hilang (mode=%s). Menunggu fix...", mode)
                    have_fix_logged = False
                continue
            if not have_fix_logged:
                log.info("Fix diperoleh (mode=%dD).", mode)
                have_fix_logged = True

            now = time.monotonic()
            if now - last_pub < PUBLISH_PERIOD:
                continue
            last_pub = now

            speed = g(report, "speed", 0.0)   # m/s
            ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

            data = {
                "timestamp_utc": ts,
                "latitude": lat,
                "longitude": lon,
                "speed": speed,
            }

            try:
                write_csv_row([ts, lat, lon, speed])
            except Exception as e:
                log.error("Gagal tulis CSV: %s", e)

            try:
                info = client.publish(MQTT_TOPIC, json.dumps(data), qos=0)
                if info.rc != mqtt.MQTT_ERR_SUCCESS:
                    log.warning("Publish gagal rc=%s (offline?); data tetap di CSV.", info.rc)
            except Exception as e:
                log.error("Exception saat publish: %s", e)

            log.info("lat=%.6f lon=%.6f speed=%.2f m/s", lat, lon, speed)

    except (KeyboardInterrupt, SystemExit):
        log.info("Shutdown diminta. Membersihkan...")
    finally:
        client.loop_stop()
        client.disconnect()
        log.info("Selesai.")


if __name__ == "__main__":
    main()