import minimalmodbus
import serial
import time
import csv
import os
from datetime import datetime

# =====================================================
# KONFIGURASI
# =====================================================
# by-path TETAP walau ttyUSB berubah saat hub re-enumerate (5-1.1 = SCC/FTDI)
SCC_PORT = '/dev/serial/by-path/platform-fc800000.usb-usb-0:1.1:1.0-port0'
SCC_SLAVE = 1
SCC_BAUD = 115200

READ_INTERVAL = 60          # detik antar siklus baca
REOPEN_COOLDOWN = 5         # jeda sebelum coba re-open handle setelah gagal

# Dua register suhu EPever yang mau dibandingkan (sementara, untuk investigasi
# selisih pembacaan vs sensor BMP280 eksternal):
#   0x3110 -> "Battery Temperature": suhu yang dipakai controller untuk estimasi
#             kompensasi suhu baterai (kalau tak ada sensor RTS eksternal
#             terpasang, EPever pakai sensor internalnya sendiri sebagai fallback).
#   0x3111 -> "Device Temperature": suhu internal PCB/heatsink controller
#             (fisiknya di sisi kiri casing EPever Tracer).
# Keduanya BUKAN sensor suhu ambient/ruangan seperti BMP280 -> wajar kalau beda
# beberapa derajat dari suhu ruangan, karena posisi & selubung casing berbeda.
# Kedua register SIGNED 16-bit, skala 0.01 derajat C -> wajib signed=True saat
# dibaca supaya suhu di bawah 0 derajat C tidak terbaca salah.
BATT_TEMP_REG = 0x3110
SCC_TEMP_REG = 0x3111

# =====================================================
# MODBUS: buka & re-open handle (kunci penyelesaian Errno 5)
# =====================================================
def open_instrument():
    """Buka handle Modbus baru dari by-path. Dipanggil di awal DAN setiap kali
    handle lama basi (Errno 5) karena hub USB brown-out / re-enumerate.
    Mengembalikan instrument atau None bila device belum tersedia."""
    try:
        inst = minimalmodbus.Instrument(SCC_PORT, SCC_SLAVE)
        inst.serial.baudrate = SCC_BAUD
        inst.serial.bytesize = 8
        inst.serial.parity = serial.PARITY_NONE
        inst.serial.stopbits = 1
        inst.serial.timeout = 2
        inst.clear_buffers_before_each_transaction = True
        return inst
    except Exception as e:
        print(f"[Modbus] Gagal buka handle: {e}")
        return None


def close_instrument(inst):
    """Tutup serial port lama supaya file descriptor basi dilepas OS."""
    try:
        if inst is not None and inst.serial and inst.serial.is_open:
            inst.serial.close()
    except Exception:
        pass


def read_long_epever(inst, addr_low, addr_high, decimals=2, functioncode=4):
    """Baca register 32-bit ala EPever: word LOW di alamat lebih kecil,
    word HIGH di alamat lebih besar (mis. 0x3102 low / 0x3103 high untuk
    PV power). read_register() minimalmodbus hanya baca 1 register 16-bit,
    jadi register 32-bit (pv_power, batt_charging_current, load_power)
    HARUS digabung dari 2 register, bukan dibaca sebagai 1 register saja
    (yang sebelumnya hanya mengambil word LOW -> bisa salah bila nilai
    melebihi 655.35 pada skala 2 desimal)."""
    low = inst.read_register(addr_low, 0, functioncode=functioncode)
    high = inst.read_register(addr_high, 0, functioncode=functioncode)
    raw = (high << 16) | low
    return round(raw / (10 ** decimals), decimals)


def read_scc(inst):
    """Baca semua register SCC. Lempar exception ke pemanggil agar bisa
    membedakan error I/O (perlu re-open) vs error transient (retry biasa).

    CATATAN PENTING soal arah arus baterai (diperbaiki):
    Register 0x3105/0x3106 pada EPever adalah "Battery CHARGING Current",
    yaitu arus dari controller MENUJU baterai (selalu >= 0). Register ini
    TIDAK merepresentasikan arus yang keluar dari baterai saat baterai
    men-supply beban (load) melebihi daya dari PV, misalnya malam hari.
    Beban (load) di EPever disuplai langsung dari busbar baterai lewat
    port LOAD, dan arusnya diukur terpisah di 0x310D.

    Maka arus NET baterai (positif = mengisi/masuk ke baterai,
    negatif = mengosongkan/keluar dari baterai) dihitung sebagai:
        batt_current_net = batt_charging_current - load_current
    Field "batt_current" pada payload sekarang berisi nilai NET ini,
    sedangkan arus charging mentah dari controller tetap disertakan
    terpisah sebagai "batt_charging_current" untuk keperluan debug.
    """
    pv_voltage = inst.read_register(0x3100, 2, functioncode=4)
    pv_current = inst.read_register(0x3101, 2, functioncode=4)
    pv_power = read_long_epever(inst, 0x3102, 0x3103, decimals=2)  # 32-bit
    battery_voltage = inst.read_register(0x3104, 2, functioncode=4)
    batt_charging_current = read_long_epever(inst, 0x3105, 0x3106, decimals=2)  # 32-bit, arus MASUK ke baterai
    load_voltage = inst.read_register(0x310C, 2, functioncode=4)
    load_current = inst.read_register(0x310D, 2, functioncode=4)
    load_power = read_long_epever(inst, 0x310E, 0x310F, decimals=2)  # 32-bit
    soc = inst.read_register(0x311A, 0, functioncode=4)

    # Suhu SCC: baca DUA register sekaligus untuk perbandingan/investigasi
    # (lihat catatan register di bagian KONFIGURASI di atas).
    # signed=True WAJIB karena register ini bisa bernilai negatif.
    batt_temperature = inst.read_register(BATT_TEMP_REG, 2, functioncode=4, signed=True)
    scc_temperature = inst.read_register(SCC_TEMP_REG, 2, functioncode=4, signed=True)

    # Arus net baterai: positif = charging (masuk), negatif = discharging (keluar)
    batt_current_net = round(batt_charging_current - load_current, 2)

    return {
        "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "pv_voltage": pv_voltage,
        "pv_current": pv_current,
        "pv_power": pv_power,
        "battery_voltage": battery_voltage,
        "batt_current": batt_current_net,          # NET: + masuk / - keluar dari baterai
        "batt_charging_current": batt_charging_current,  # mentah dari controller (>=0), buat debug
        "load_voltage": load_voltage,
        "load_current": load_current,
        "load_power": load_power,
        "soc": soc,
        "batt_temperature": batt_temperature,  # 0x3110, suhu estimasi baterai (derajat C)
        "scc_temperature": scc_temperature,    # 0x3111, suhu internal device SCC (derajat C)
    }

# =====================================================
# CSV LOKAL
# =====================================================
def save_to_csv(d):
    file_name = f"/home/orangepi/data/mppt/mppt_log_{datetime.now():%Y-%m-%d}.csv"
    os.makedirs(os.path.dirname(file_name), exist_ok=True)
    file_exists = os.path.isfile(file_name)
    try:
        with open(file_name, mode='a', newline='') as file:
            writer = csv.writer(file)
            if not file_exists:
                writer.writerow(['Timestamp', 'PV_Voltage_V', 'PV_Current_A',
                                 'PV_Power_W', 'Battery_Voltage_V',
                                 'Batt_Current_Net_A', 'Batt_Charging_Current_A',
                                 'Load_Voltage_V', 'Load_Current_A',
                                 'Load_Power_W', 'SOC_Percent',
                                 'Batt_Temperature_C', 'SCC_Temperature_C'])
            writer.writerow([d["timestamp"], d["pv_voltage"], d["pv_current"],
                             d["pv_power"], d["battery_voltage"],
                             d["batt_current"], d["batt_charging_current"],
                             d["load_voltage"], d["load_current"],
                             d["load_power"], d["soc"],
                             d["batt_temperature"], d["scc_temperature"]])
    except Exception as e:
        print(f"[CSV] Gagal menyimpan: {e}")

# =====================================================
# PROGRAM UTAMA
# =====================================================
def main():
    instrument = open_instrument()

    print("-" * 40)
    print("Memulai pembacaan data SCC EPever (robust mode, lokal saja)...")
    print("Tekan Ctrl+C untuk berhenti.")
    print("-" * 40)

    try:
        while True:
            # Bila handle belum ada (device hilang saat brown-out), coba buka ulang
            if instrument is None:
                print("[Modbus] Handle tidak ada, mencoba re-open dari by-path...")
                instrument = open_instrument()
                if instrument is None:
                    time.sleep(REOPEN_COOLDOWN)
                    continue

            try:
                payload = read_scc(instrument)

                # Guard: SOC valid (0-100). Bila di luar rentang -> data sampah, skip.
                if payload["soc"] is None or not (0 <= payload["soc"] <= 100):
                    raise minimalmodbus.InvalidResponseError("SOC di luar rentang valid")

                save_to_csv(payload)
                print("  -> CSV OK")

            except (serial.SerialException, OSError) as e:
                # Errno 5 (Input/output error) & sejenisnya -> handle BASI.
                # Tutup, buang, dan paksa re-open di iterasi berikutnya.
                print(f"[Modbus] I/O error (handle basi): {e} -> re-open handle.")
                close_instrument(instrument)
                instrument = None
                time.sleep(REOPEN_COOLDOWN)
                continue

            except minimalmodbus.NoResponseError:
                print("[Modbus] SCC tidak merespons (Timeout). "
                      "Normal bila malam (SCC sleep) / cek kabel RS485.")
            except minimalmodbus.InvalidResponseError as e:
                print(f"[Modbus] Respons tidak valid: {e}")
            except Exception as e:
                # Tangkap Errno 5 yang kadang muncul sebagai Exception generik
                if 'Input/output error' in str(e) or 'Errno 5' in str(e):
                    print(f"[Modbus] I/O error generik: {e} -> re-open handle.")
                    close_instrument(instrument)
                    instrument = None
                    time.sleep(REOPEN_COOLDOWN)
                    continue
                print(f"[Modbus] Error tak terduga: {e}")

            time.sleep(READ_INTERVAL)

    except KeyboardInterrupt:
        print("\nProgram dihentikan oleh pengguna.")
    finally:
        close_instrument(instrument)


if __name__ == '__main__':
    main()