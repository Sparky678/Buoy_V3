import os
import datetime

# Folder tempat file log disimpan
LOG_FOLDER = "/home/pi/data/gps"

# Format nama file log 
LOG_FILENAME_PREFIXES = ["gps_data_"]
LOG_FILENAME_SUFFIX = ".csv"
LOG_DATE_FORMAT = "%Y-%m-%d"

# Jumlah hari yang diizinkan untuk menyimpan file log
DAYS_TO_KEEP = 60

def delete_old_logs():
    """Menghapus file log yang lebih tua dari DAYS_TO_KEEP."""
    now = datetime.datetime.now()
    cutoff_date = now - datetime.timedelta(days=DAYS_TO_KEEP)
    
    if not os.path.exists(LOG_FOLDER):
        print(f"Folder {LOG_FOLDER} tidak ditemukan.")
        return
    
    for filename in os.listdir(LOG_FOLDER):
        if not any(filename.startswith(prefix) and filename.endswith(LOG_FILENAME_SUFFIX) for prefix in LOG_FILENAME_PREFIXES):
            continue  # Lewati file yang tidak sesuai format
        
        try:
            for prefix in LOG_FILENAME_PREFIXES:
                if filename.startswith(prefix):
                    date_str = filename[len(prefix):-len(LOG_FILENAME_SUFFIX)]
                    file_date = datetime.datetime.strptime(date_str, LOG_DATE_FORMAT)
                    
                    # Hapus file jika lebih tua dari cutoff_date
                    if file_date < cutoff_date:
                        file_path = os.path.join(LOG_FOLDER, filename)
                        os.remove(file_path)
                        print(f"Deleted: {file_path}")
                    break  # Keluar dari loop setelah menemukan prefix yang cocok
        except ValueError:
            continue  # Lewati file yang tidak sesuai format tanggal

if __name__ == "__main__":
    delete_old_logs()

