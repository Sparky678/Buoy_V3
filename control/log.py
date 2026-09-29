import subprocess
import paho.mqtt.publish as publish

# Daftar layanan yang ingin dimonitor
services = ["gps.service", "serial.service", "modbus.service", "mppt.service", "alert.service"]

def get_service_logs(service, num_lines=5):
    """Mengambil log terakhir untuk layanan tertentu."""
    try:
        result = subprocess.run(
            ["journalctl", "-u", service, "-n", str(num_lines), "--no-pager"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        if result.returncode == 0:
            return result.stdout
        else:
            return f"Error retrieving logs for {service}: {result.stderr.strip()}"
    except Exception as e:
        return f"Exception retrieving logs for {service}: {e}"

def get_dmesg_errors():
    """Mengambil log kernel dengan kata kunci 'error' atau 'fail' (10 baris terakhir) dan menampilkan waktu."""
    try:
        result = subprocess.run(
            "dmesg -T | grep -i 'error\\|fail' | tail -n 10",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            shell=True
        )
        if result.returncode == 0:
            return result.stdout.strip()
        else:
            return f"Error retrieving dmesg errors: {result.stderr.strip()}"
    except Exception as e:
        return f"Exception retrieving dmesg errors: {e}"

def send_mqtt(topic, message, broker="localhost", username=None, password=None):
    """Mengirim pesan melalui MQTT."""
    try:
        auth = {"username": username, "password": password} if username and password else None
        publish.single(topic, message, hostname=broker, auth=auth)
        return "MQTT message sent successfully"
    except Exception as e:
        return f"Failed to send MQTT message: {e}"

if __name__ == "__main__":
    # Kumpulan hasil log
    log_data = []

    # Ambil log untuk setiap layanan
    for service in services:
        log_data.append(f"Logs for {service}:\n{get_service_logs(service)}\n{'-' * 50}")
    
    # Ambil log kernel terkait error atau fail
    kernel_logs = get_dmesg_errors()
    log_data.append(f"Kernel errors or failures:\n{kernel_logs}")
    
    # Gabungkan semua log
    final_message = "\n".join(log_data)

    # Kirim melalui MQTT
    mqtt_result = send_mqtt(
        topic="buoyV2/logs",
        message=final_message,
        broker="vps.isi-net.org",
        username="unila",
        password="pwdMQTT@123"
    )
    
    print(mqtt_result)
