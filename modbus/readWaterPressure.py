import minimalmodbus    
import time    
from datetime import datetime    
  
# Konfigurasi komunikasi Modbus RTU    
try:  
    sensor = minimalmodbus.Instrument('/dev/serial/by-path/platform-fc880000.usb-usb-0:1.4:1.0-port0', 1)  # Port serial dan address slave    
    sensor.serial.baudrate = 9600  # Baudrate    
    sensor.serial.bytesize = 8  # Ukuran byte    
    sensor.serial.parity = minimalmodbus.serial.PARITY_NONE  # Paritas    
    sensor.serial.stopbits = 1  # Stop bit    
    sensor.serial.timeout = 1  # Timeout komunikasi    
    port_found = True  
except IOError as e:  
    print(f"Port tidak ditemukan: {e}")  
    sensor = None  
    port_found = False  
  
# Fungsi untuk membaca data dari register    
def read_sensor_data():    
    if not port_found:  
        print("Port tidak ditemukan, nilai diset menjadi None.")  
        return {    
            "MPa": None,    
            "kPa": None,    
            "water_level": None,    
            "bar": None,    
            "mbar": None,    
            "kg/cm2": None,    
            "psi": None,    
            "mH2O": None,    
            "mmH2O": None    
        }  
    try:    
        # Membaca register    
        data = sensor.read_registers(2, 9, functioncode=3)  # Address 2, quantity 9    
        labeled_data = {    
            "MPa": data[0],    
            "kPa": data[1],    
            "water_level": data[2],    
            "bar": data[3],    
            "mbar": data[4],    
            "kg/cm2": data[5],    
            "psi": data[6],    
            "mH2O": data[7],    
            "mmH2O": data[8]    
        }    
        return labeled_data    
    except Exception as e:    
        print(f"Kesalahan membaca sensor: {e}")    
        return {    
            "MPa": None,    
            "kPa": None,    
            "water_level": None,    
            "bar": None,    
            "mbar": None,    
            "kg/cm2": None,    
            "psi": None,    
            "mH2O": None,    
            "mmH2O": None    
        }  # Jika ada error, kembalikan dictionary dengan None    
    
