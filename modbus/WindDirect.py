import minimalmodbus
import time

# =====================================================
# KONFIGURASI SENSOR WIND DIRECTION (RS485, slave ID 2)
# =====================================================
PORT = '/dev/serial/by-path/platform-fc880000.usb-usb-0:1.2:1.0-port0'
SLAVE_ADDR = 2
BAUDRATE = 9600
TIMEOUT = 0.3          # detik; cukup longgar utk respons sah (~20 ms), tapi
                       # bikin percobaan GAGAL menyerah cepat -> retry murah
READ_RETRIES = 3       # terbukti dari uji: retry=3 -> success rate 100%
RETRY_DELAY = 0.03     # jeda antar percobaan; menyediakan IDLE GAP bus yang
                       # dibutuhkan slave agar mau merespons (ambang ~0.02 s)

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


# Fungsi untuk mengonversi nilai direction ke arah
def get_direction_from_value(direction):
    directions = [
        "North", "Northeast by north", "Northeast", "Northeast by east",
        "East", "Southeast by east", "Southeast", "Southeast by south",
        "South", "Southwest by south", "Southwest", "Southwest by west",
        "West", "Northwest by west", "Northwest", "Northwest by north"
    ]
    if 0 <= direction < len(directions):
        return directions[direction]
    else:
        return "Invalid direction"


# Fungsi untuk membaca data dari register (dengan RETRY)
def read_sensor_data():
    """Baca angle + direction dari slave ID 2 dengan retry.

    Di bus RS485 bersama (anemometer ID 3 + wind direction ID 2), satu slave
    sering melewatkan transaksi pertama karena idle gap bus belum cukup setelah
    transaksi slave lain. Retry dengan jeda RETRY_DELAY menyediakan gap itu;
    uji empiris menunjukkan 3x retry -> 100% sukses.
    """
    if not port_found:
        print("Port tidak ditemukan, nilai diset menjadi None.")
        return {"angle": None, "direction": "None"}

    last_err = None
    for attempt in range(READ_RETRIES):
        try:
            data = sensor.read_registers(0, 2, functioncode=3)  # addr 0, qty 2
            return {
                "angle": data[0] / 10,  # nilai angle dibagi 10
                "direction": get_direction_from_value(data[1]) if len(data) > 1 else "No data",
            }
        except Exception as e:
            last_err = e
            if attempt < READ_RETRIES - 1:
                time.sleep(RETRY_DELAY)   # beri idle gap, lalu coba lagi

    print(f"[WindDirect] Gagal baca ID {SLAVE_ADDR} setelah {READ_RETRIES}x: {last_err}")
    return {"angle": None, "direction": "None"}