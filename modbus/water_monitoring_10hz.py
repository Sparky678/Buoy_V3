"""
water_monitoring_10hz_optimized.py
Optimized for stable 10 Hz sampling & low CPU usage
Rev: MQTT connect_async + auto-reconnect (pola sama dengan mppt.py/wind_monitoring.py)
"""

import csv
import json
import time
import os
import threading
import socket
from datetime import datetime

import paho.mqtt.client as mqtt
from readWaterPressure import read_sensor_data as read_water_pressure
from quality_control import water_qc


# ================= MQTT =================
MQTT_BROKER = "77.37.63.21"   # IP langsung broker (c-greenproject.org) — jangan pakai domain
MQTT_PORT = 1883
MQTT_USER = "adminvps"
MQTT_PASSWORD = "pwdMQTT@123"
MQTT_TOPIC_RAW = "buoyV3/water_pressure"

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="buoyV3_water")
client.username_pw_set(MQTT_USER, MQTT_PASSWORD)

mqtt_connected = False

def _on_connect(client, userdata, flags, reason_code, properties):
    global mqtt_connected
    if reason_code == 0:
        mqtt_connected = True
        print(f"[Water] MQTT Connected ({MQTT_BROKER}:{MQTT_PORT})")
    else:
        mqtt_connected = False
        print(f"[Water] MQTT connect gagal, reason={reason_code}")

def _on_disconnect(client, userdata, flags, reason_code, properties):
    global mqtt_connected
    mqtt_connected = False
    print(f"[Water] MQTT terputus (reason={reason_code}); reconnect otomatis berjalan...")

def connect_mqtt():
    """connect_async TIDAK blocking dan TIDAK melempar error walau jaringan
    belum siap (anti [Errno 101]); loop_start menangani reconnect di background.
    Akuisisi 10 Hz tidak pernah menunggu MQTT."""
    client.on_connect = _on_connect
    client.on_disconnect = _on_disconnect
    client.reconnect_delay_set(min_delay=1, max_delay=60)
    client.connect_async(MQTT_BROKER, MQTT_PORT, keepalive=60)
    client.loop_start()  # NON BLOCKING

# ================= CONFIG =================
SAMPLING_RATE_HZ = 10
SLEEP_TIME = 1.0 / SAMPLING_RATE_HZ
CSV_BUFFER_SIZE = 10

# ================= GLOBAL =================
lock = threading.Lock()
water_data_global = {}

csv_processed_buffer = []
csv_raw_buffer = []

# ================= CSV =================
def flush_csv_buffers():
    global csv_processed_buffer, csv_raw_buffer

    if csv_processed_buffer:
        save_batch_to_csv_processed(csv_processed_buffer)
        csv_processed_buffer.clear()

    if csv_raw_buffer:
        save_batch_to_csv_raw(csv_raw_buffer)
        csv_raw_buffer.clear()

def save_batch_to_csv_processed(data_list):
    date_str = time.strftime("%Y-%m-%d")
    filename = f"/home/orangepi/data/water/water_processed_{date_str}.csv"

    fieldnames = ["timestamp", "WaterLevel_m", "quality_flag"]
    os.makedirs(os.path.dirname(filename), exist_ok=True)

    file_exists = os.path.isfile(filename)

    with open(filename, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists or f.tell() == 0:
            writer.writeheader()
        writer.writerows(data_list)

def save_batch_to_csv_raw(data_list):
    date_str = time.strftime("%Y-%m-%d")
    filename = f"/home/orangepi/data/water/water_raw_{date_str}.csv"

    fieldnames = [
        "timestamp","MPa","kPa","water_level",
        "bar","mbar","kg/cm2","psi","mH2O","mmH2O"
    ]

    os.makedirs(os.path.dirname(filename), exist_ok=True)
    file_exists = os.path.isfile(filename)

    with open(filename, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists or f.tell() == 0:
            writer.writeheader()
        writer.writerows(data_list)

# ================= SENSOR THREAD =================
def read_water_sensor():
    global water_data_global
    while True:
        try:
            data = read_water_pressure() or {}
            with lock:
                water_data_global = data
        except Exception as e:
            print(f"[Water] Sensor error: {e}")

        time.sleep(0.02)  # ringan CPU

# ================= MAIN LOOP =================
def main():

    global csv_processed_buffer, csv_raw_buffer

    connect_mqtt()

    thread = threading.Thread(target=read_water_sensor, daemon=True)
    thread.start()

    print("[Water] Monitoring Started")
    print("[Water] Target Rate:", SAMPLING_RATE_HZ, "Hz")

    next_time = time.monotonic()
    sample_counter = 0

    total_samples = 0
    last_stats_time = time.monotonic()

    while True:
        try:
            # ===== REALTIME TIMESTAMP =====
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

            with lock:
                water_data = water_data_global.copy()

            # ===== WATER LEVEL (QC rev2) =====
            raw_water = water_data.get("water_level", None)

            # Konversi unit HANYA untuk pembacaan sah. 65535 = kode error sensor.
            if raw_water == 65535:
                conv = None              # biarkan QC menandai REJECTED/STALE
            elif raw_water is not None:
                conv = raw_water / 100
            else:
                conv = None

            qc = water_qc.clean(conv)

            # processed CSV: tulis value apa adanya (boleh None) + flag kualitas.
            # Tidak pernah ada nilai karangan 3.0/last-hold yang menyamar valid.
            processed_data = {
                "timestamp": timestamp,
                "WaterLevel_m": qc.value if qc.value is not None else "",
                "quality_flag": qc.flag,
            }

            raw_data = {
                "timestamp": timestamp,
                "MPa": water_data.get("MPa"),
                "kPa": water_data.get("kPa"),
                "water_level": water_data.get("water_level"),  # FIX TYPO
                "bar": water_data.get("bar"),
                "mbar": water_data.get("mbar"),
                "kg/cm2": water_data.get("kg/cm2"),
                "psi": water_data.get("psi"),
                "mH2O": water_data.get("mH2O"),
                "mmH2O": water_data.get("mmH2O")
            }

            csv_processed_buffer.append(processed_data)
            csv_raw_buffer.append(raw_data)

            sample_counter += 1

            # ===== BATCH CSV WRITE =====
            if sample_counter >= CSV_BUFFER_SIZE:
                flush_csv_buffers()
                sample_counter = 0

            # ===== MQTT NON BLOCKING =====
            if mqtt_connected:
                client.publish(MQTT_TOPIC_RAW, json.dumps(raw_data), qos=0)

        except Exception as e:
            print("[Water] Loop error:", e)

        # ===== REALTIME SCHEDULER (SUPER STABLE) =====
        next_time += SLEEP_TIME
        sleep_time = next_time - time.monotonic()
        if sleep_time > 0:
            time.sleep(sleep_time)

# ================= RUN =================
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[Water] Shutdown...")
        flush_csv_buffers()
        client.loop_stop()