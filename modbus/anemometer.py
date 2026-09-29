import minimalmodbus
import time

# =====================================================
# KONFIGURASI SENSOR ANEMOMETER (RS485, slave ID 3)
# =====================================================
PORT = '/dev/serial/by-path/platform-fc880000.usb-usb-0:1.2:1.0-port0'
SLAVE_ADDR = 3
BAUDRATE = 9600
TIMEOUT = 0.3          # detik; longgar utk respons sah, cepat menyerah saat gagal
READ_RETRIES = 3       # ID 3 menuntut idle gap lebih besar -> retry penting
RETRY_DELAY = 0.03     # jeda antar percobaan (menyediakan idle gap bus)

# Konfigurasi komunikasi Modbus RTU
try:
    sensor = minimalmodbus.Instrument(PORT, SLAVE_ADDR)
    sensor.serial.baudrate = BAUDRATE
    sensor.serial.bytesize = 8
    sensor.serial.parity = minimalmodbus.serial.PARITY_NONE
    sensor.serial.stopbits = 1
    sensor.serial.timeout = TIMEOUT
    sensor.clear_buffers_before_each_transaction = True  # penting di multi-drop
    port_found = True
except IOError as e:
    print(f"Port tidak ditemukan: {e}")
    sensor = None
    port_found = False


# Fungsi untuk membaca data dari register (dengan RETRY)
def read_sensor_data():
    """Baca wind speed + beaufort dari slave ID 3 dengan retry.

    Catatan empiris: di bus bersama, ID 3 (anemometer) justru menuntut idle gap
    LEBIH besar daripada ID 2. Tanpa retry, ID 3 mudah gagal bila dibaca tepat
    setelah transaksi ID 2 yang cepat. Retry menyelesaikan ini (uji: 100%).
    """
    if not port_found:
        print("Port tidak ditemukan, nilai diset menjadi None.")
        return {"anemometer_speed": None, "beaufort_scale": None}

    last_err = None
    for attempt in range(READ_RETRIES):
        try:
            data = sensor.read_registers(0, 2, functioncode=3)  # addr 0, qty 2
            return {
                "anemometer_speed": data[0] if len(data) > 0 else None,
                "beaufort_scale": data[1] if len(data) > 1 else None,
            }
        except Exception as e:
            last_err = e
            if attempt < READ_RETRIES - 1:
                time.sleep(RETRY_DELAY)

    print(f"[Anemometer] Gagal baca ID {SLAVE_ADDR} setelah {READ_RETRIES}x: {last_err}")
    return {"anemometer_speed": None, "beaufort_scale": None}