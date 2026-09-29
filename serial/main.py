import serial
import json
import time
import csv
import os
from datetime import datetime
import paho.mqtt.client as mqtt
from collections import deque
import statistics

# --- KONFIGURASI ---

# Serial & MQTT
SERIAL_PORT = '/dev/serial/by-path/platform-fc8c0000.usb-usb-0:1:1.0-port0'
BAUD_RATE = 115200
MQTT_BROKER = "77.37.63.21"   # IP langsung broker (c-greenproject.org) - jangan pakai domain
MQTT_PORT = 1883
MQTT_USER = "adminvps"
MQTT_PASS = "pwdMQTT@123"
MQTT_TOPIC = "buoyV3/serial"

# Definisi Kolom
COLUMNS = [
    'xAcc', 'yAcc', 'zAcc',
    'xGyro', 'yGyro', 'zGyro',
    'xAngle', 'yAngle', 'zAngle',
    'temperature', 'pressure', 'depth'
]

# --- KONFIGURASI QUALITY CONTROL (QC) ---
QC_LIMITS = {
    'temperature': (-10, 100),      # Derajat Celcius
    'pressure': (90000, 120000),   # Pascal
    'depth': (0, 50),              # Meter
    'xAngle': (-360, 360),         # Diperlebar menyesuaikan output WT61PC
    'yAngle': (-360, 360),
    'zAngle': (-360, 360)
}

# --- KONFIGURASI DATA CLEANING (FILTER) ---
FILTER_WINDOW_SIZE = 5
data_buffer = {col: deque(maxlen=FILTER_WINDOW_SIZE) for col in COLUMNS}

# --- SETUP MQTT (pola connect_async + auto-reconnect, sama dengan mppt.py) ---
mqtt_connected = False

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="buoyV3_serial")

def on_connect(client, userdata, flags, reason_code, properties):
    global mqtt_connected
    if reason_code == 0:
        mqtt_connected = True
        print(f"[MQTT] Terhubung ke {MQTT_BROKER}:{MQTT_PORT}")
    else:
        mqtt_connected = False
        print(f"[MQTT] Gagal connect, reason={reason_code}")

def on_disconnect(client, userdata, flags, reason_code, properties):
    global mqtt_connected
    mqtt_connected = False
    print(f"[MQTT] Terputus (reason={reason_code}); reconnect otomatis berjalan...")

def setup_mqtt():
    """connect_async TIDAK melempar error walau jaringan belum siap;
    loop_start menjaga reconnect di background. Akuisisi CSV tidak
    pernah bergantung pada status MQTT."""
    client.username_pw_set(MQTT_USER, MQTT_PASS)
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.reconnect_delay_set(min_delay=1, max_delay=60)
    client.connect_async(MQTT_BROKER, MQTT_PORT, keepalive=60)
    client.loop_start()

# --- FUNGSI PROSES DATA ---

def get_csv_filename():
    date_str = datetime.now().strftime("%Y-%m-%d")
    directory = "/home/orangepi/data/serial"
    os.makedirs(directory, exist_ok=True)
    return f"{directory}/serial_{date_str}.csv"

def write_to_csv(data_dict):
    filename = get_csv_filename()
    fieldnames = ['TS_opi'] + COLUMNS
    file_exists = os.path.isfile(filename)

    try:
        with open(filename, mode='a', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()

            row = {k: data_dict.get(k) for k in fieldnames}
            writer.writerow(row)
    except Exception as e:
        print(f"CSV Error: {e}")

def quality_control(data_dict):
    if not data_dict: return False

    for key, limits in QC_LIMITS.items():
        if key in data_dict:
            val = data_dict[key]
            min_val, max_val = limits
            if not (min_val <= val <= max_val):
                print(f"[QC REJECT] {key}: {val} is out of bounds {limits}")
                return False
    return True

def clean_data_moving_average(new_data):
    smoothed_data = {}
    for key, val in new_data.items():
        data_buffer[key].append(val)
        if len(data_buffer[key]) > 0:
            smoothed_data[key] = round(statistics.mean(data_buffer[key]), 3)
        else:
            smoothed_data[key] = val
    return smoothed_data

def parse_serial_data(raw_line):
    try:
        raw_line = raw_line.decode('utf-8', errors='ignore').strip()
        if not raw_line: return None
        if "xacc" in raw_line.lower(): return None

        values = raw_line.split(',')
        if len(values) != len(COLUMNS):
            return None

        data = {}
        for i, col in enumerate(COLUMNS):
            data[col] = float(values[i])

        return data
    except Exception:
        return None

# --- MAIN LOOP ---

def main():
    setup_mqtt()

    try:
        ser = serial.Serial(SERIAL_PORT, BAUD_RATE, timeout=1)
        print(f"Listening on {SERIAL_PORT}...")
        time.sleep(2)
        ser.reset_input_buffer()  # PENTING: buang antrean data sampah awal agar kolom tidak bergeser
    except Exception as e:
        print(f"Serial Error: {e}")
        return

    while True:
        try:
            if ser.in_waiting > 0:
                raw_line = ser.readline()
                raw_data = parse_serial_data(raw_line)

                if raw_data:
                    if quality_control(raw_data):
                        clean_data = clean_data_moving_average(raw_data)
                        clean_data['TS_opi'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                        # CSV SELALU ditulis, terlepas dari status MQTT
                        write_to_csv(clean_data)

                        payload = json.dumps(clean_data)
                        if mqtt_connected:
                            client.publish(MQTT_TOPIC, payload, qos=0)
                            print(f"Data OK (MQTT): {payload}")
                        else:
                            print(f"Data OK (CSV only, MQTT offline): {payload}")

        except KeyboardInterrupt:
            print("\nStopping...")
            break
        except Exception as e:
            print(f"Loop Error: {e}")
            time.sleep(1)

    client.loop_stop()

if __name__ == "__main__":
    main()