import paho.mqtt.client as mqtt
import os

# Konfigurasi MQTT
MQTT_BROKER = "vps.isi-net.org"  # Ganti dengan alamat broker
MQTT_PORT = 1883
MQTT_TOPIC = "buoyV2/control"
MQTT_USER = "unila"
MQTT_PASS = "pwdMQTT@123"

# Fungsi callback ketika pesan diterima
def on_message(client, userdata, message):
    command = message.payload.decode('utf-8')  # Decode pesan
    print(f"Pesan diterima: {command}")
    
    # Eksekusi perintah sistem
    try:
        result = os.popen(command).read()
        print(f"Hasil: {result}")
        # Kirim hasil eksekusi kembali (opsional)
        client.publish("buoyV2/response", f"Hasil: {result}")
    except Exception as e:
        print(f"Kesalahan saat menjalankan perintah: {e}")
        client.publish("buoyV2/response", f"Kesalahan: {e}")

# Inisialisasi client MQTT
client = mqtt.Client()
client.username_pw_set(MQTT_USER, MQTT_PASS)  # Tambahkan autentikasi
client.on_message = on_message

# Koneksi ke broker
client.connect(MQTT_BROKER, MQTT_PORT)

# Berlangganan topik
client.subscribe(MQTT_TOPIC)

print("Menunggu pesan MQTT...")
client.loop_forever()

