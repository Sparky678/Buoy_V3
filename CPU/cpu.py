import json
import time
import os
import csv
import psutil  # Menggunakan psutil untuk mendapatkan metrik sistem yang real
import paho.mqtt.client as mqtt
from datetime import datetime

# Konfigurasi MQTT
MQTT_BROKER = "77.37.63.21"   # IP langsung broker (c-greenproject.org) - jangan pakai domain
MQTT_PORT = 1883
MQTT_TOPIC = "buoyV3/cpu"
MQTT_USERNAME = "adminvps"
MQTT_PASSWORD = "pwdMQTT@123"

# --- MQTT (pola connect_async + auto-reconnect) ---
mqtt_connected = False

mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="buoyV3_cpu")

def on_connect(client, userdata, flags, reason_code, properties):
    global mqtt_connected
    mqtt_connected = (reason_code == 0)
    print(f"[MQTT] {'Terhubung' if mqtt_connected else f'Gagal connect, reason={reason_code}'}")

def on_disconnect(client, userdata, flags, reason_code, properties):
    global mqtt_connected
    mqtt_connected = False
    print(f"[MQTT] Terputus (reason={reason_code}); reconnect otomatis berjalan...")

def setup_mqtt():
    mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_disconnect = on_disconnect
    mqtt_client.reconnect_delay_set(min_delay=1, max_delay=60)
    mqtt_client.connect_async(MQTT_BROKER, MQTT_PORT, keepalive=60)
    mqtt_client.loop_start()

# Fungsi untuk mendapatkan suhu CPU secara defensif.
# Nama zona termal berbeda antar platform: RPi = 'cpu_thermal',
# RK3588S (Orange Pi 5) = 'soc_thermal' / 'bigcore*_thermal', dll.
def get_cpu_temp():
    try:
        temps = psutil.sensors_temperatures()
        if not temps:
            return 0
        # Prioritas: cpu_thermal (RPi), soc_thermal (RK3588S), lalu zona pertama yang ada
        for key in ("cpu_thermal", "soc_thermal"):
            if key in temps and temps[key]:
                return temps[key][0].current
        first = next(iter(temps.values()))
        return first[0].current if first else 0
    except Exception:
        return 0

# Fungsi untuk mendapatkan metrik sistem
def get_system_metrics():
    cpu_usage = psutil.cpu_percent(interval=1)  # Mengambil CPU Usage real dalam 1 detik
    mem_info = psutil.virtual_memory()
    mem_gpu = 76  # Jika ada sumber data real, ganti di sini
    mem_arm = mem_info.used / (1024 * 1024)  # Convert bytes ke MB
    temp = get_cpu_temp()

    # Mengambil informasi penyimpanan
    statvfs = os.statvfs('/')
    total_space = statvfs.f_frsize * statvfs.f_blocks / 1e9
    used_space = statvfs.f_frsize * (statvfs.f_blocks - statvfs.f_bfree) / 1e9
    free_space = statvfs.f_frsize * statvfs.f_bfree / 1e9

    # Mendapatkan timestamp saat ini
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    return {
        "timestamp": timestamp,
        "cpu_usage": cpu_usage,
        "mem_gpu": mem_gpu,
        "mem_arm": mem_arm,
        "temp": temp,
        "total_space": total_space,
        "used_space": used_space,
        "free_space": free_space
    }

# Fungsi untuk menyimpan data ke CSV
def save_to_csv(data):
    filename = f"/home/orangepi/data/cpu/dataOS_{datetime.now().strftime('%Y-%m-%d')}.csv"
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    file_exists = os.path.isfile(filename)

    with open(filename, mode='a', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=data.keys())

        if not file_exists:
            writer.writeheader()  # Tulis header jika file baru

        writer.writerow(data)

# Fungsi untuk mengirim data melalui MQTT
def publish_to_mqtt(client, data):
    try:
        json_data = json.dumps(data)
        client.publish(MQTT_TOPIC, json_data, qos=0)
        print(f"Data sent: {json_data}")
    except Exception as e:
        print(f"MQTT Publish Error: {e}")

if __name__ == "__main__":
    setup_mqtt()

    try:
        # Loop akuisisi TIDAK bergantung pada status MQTT:
        # CSV selalu ditulis; publish hanya saat terhubung.
        while True:
            system_metrics = get_system_metrics()
            print(json.dumps(system_metrics, indent=2))

            # Simpan data ke CSV (selalu)
            try:
                save_to_csv(system_metrics)
            except Exception as e:
                print(f"CSV Error: {e}")

            # Kirim data ke MQTT (hanya jika terhubung)
            if mqtt_connected:
                publish_to_mqtt(mqtt_client, system_metrics)
            else:
                print("[MQTT] Offline - data tersimpan di CSV saja.")

            time.sleep(5)

    except KeyboardInterrupt:
        print("Program dihentikan.")
    finally:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()