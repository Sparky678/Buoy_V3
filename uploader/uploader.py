#!/usr/bin/env python3
"""
uploader.py — Store-and-forward uploader U-TEWS Buoy-V3 (Orange Pi 5)

TAHAP 2: pembaca CSV + checkpoint + pengiriman MQTT QoS 1 (store-and-forward).

Konsep
------
Script akuisisi (wind, water, serial, gps, mppt, cpu) tetap berjalan real-time
dan menulis CSV harian seperti biasa. CSV itulah "buffer lokal".
Uploader ini bangun setiap PERIOD_S detik, lalu untuk tiap stream:

  1. Membaca baris BARU sejak checkpoint terakhir (byte offset per file).
     Hanya sampai newline terakhir -> baris yang masih setengah ditulis
     oleh script akuisisi TIDAK ikut diambil (diambil di siklus berikutnya).
  2. Memecah baris menjadi chunk (maks MAX_ROWS_PER_CHUNK baris).
  3. Menyerahkan tiap chunk ke "sink".
       - DryRunSink (default): hanya mencatat, tidak mengirim.
       - MqttSink (--mqtt): publish QoS 1 + gzip, tunggu PUBACK dari broker.
  4. Checkpoint HANYA dimajukan setelah sink melapor sukses, lalu disimpan
     secara atomik (tulis file sementara -> fsync -> os.replace).
     Bila sink gagal, checkpoint diam di tempat dan data dicoba lagi
     di siklus berikutnya -> tidak ada data yang hilang.

Program ini hanya MEMBACA CSV (tidak pernah menulis/mengubah CSV), sehingga
tidak mengganggu script akuisisi 10 Hz maupun utews_realtime.py.

Pemakaian
---------
  python3 uploader.py                 # jalan terus, siklus tiap PERIOD_S
  python3 uploader.py --once          # satu siklus lalu keluar (untuk uji)
  python3 uploader.py --period 60     # override periode (detik)
  python3 uploader.py --save-batches  # simpan batch dry-run ke file JSON
  python3 uploader.py --mqtt          # KIRIM ke broker MQTT (QoS 1)
  python3 uploader.py --mqtt --topic-prefix batch/ --once   # uji paralel, topik terpisah
  python3 uploader.py --status        # tampilkan checkpoint & sisa data, lalu keluar

  Uji dengan data lama (mis. data kolam 10 hari) — pakai state-dir terpisah:
  python3 uploader.py --data-dir ~/uji/data --state-dir ~/uji/state \
                      --backfill-days 30 --drain
"""

import argparse
import csv
import gzip
import io
import json
import logging
import math
import os
import re
import signal
import sys
import threading
import time
from datetime import date, datetime, timedelta

# =============================================================================
# KONFIGURASI
# =============================================================================
DATA_DIR = os.environ.get("UPLOADER_DATA_DIR", "/home/orangepi/data")
STATE_DIR = os.environ.get("UPLOADER_STATE_DIR", "/home/orangepi/data/uploader")
CHECKPOINT_FILE = "checkpoint.json"

DEVICE_ID = "buoyV3"

PERIOD_S = 300                 # periode pengambilan data (5 menit)
ALIGN_TO_CLOCK = True          # siklus jatuh di jam bulat: 10:00, 10:05, ...
RUN_AT_STARTUP = True          # langsung satu siklus saat start (kuras backlog setelah reboot)

# --- MQTT (dipakai mulai tahap 2) ---
# Catatan jaringan: DNS di lokasi buoy meresolusi c-greenproject.org ke IP
# block-page Fortinet -> pakai IP langsung, seperti script akuisisi lain.
MQTT_BROKER = os.environ.get("UPLOADER_MQTT_BROKER", "77.37.63.21")
MQTT_PORT = int(os.environ.get("UPLOADER_MQTT_PORT", "1883"))
MQTT_USER = os.environ.get("UPLOADER_MQTT_USER", "adminvps")
MQTT_PASSWORD = os.environ.get("UPLOADER_MQTT_PASSWORD", "pwdMQTT@123")
MQTT_CLIENT_ID = "buoyV3_uploader"
MQTT_KEEPALIVE = 60
MQTT_CONNECT_TIMEOUT = 20      # detik menunggu CONNACK di awal siklus
MQTT_PUBACK_TIMEOUT = 30       # detik menunggu PUBACK per paket
MQTT_MAX_INFLIGHT = 1          # 1 = urut & sederhana; checkpoint selalu rapi
TOPIC_PREFIX = os.environ.get("UPLOADER_TOPIC_PREFIX", "")   # mis. "batch/" untuk uji paralel

MAX_ROWS_PER_CHUNK = 500       # baris per paket (1 paket = 1 pesan MQTT di tahap 2)
MAX_BYTES_PER_STREAM = 8 * 1024 * 1024   # batas baca per stream per siklus
                                         # (mencegah backlog panjang dikirim sekaligus)
BACKFILL_DAYS = 1              # saat PERTAMA kali melihat sebuah file: file yang
                               # tanggalnya lebih tua dari (hari ini - BACKFILL_DAYS)
                               # dianggap arsip lama dan tidak dikirim.

# Pemetaan stream -> folder & prefix nama file CSV (nama: <prefix>YYYY-MM-DD.csv)
STREAMS = [
    {"name": "wind",           "topic": "buoyV3/wind",           "subdir": "wind",   "prefix": "wind_"},
    {"name": "imu",            "topic": "buoyV3/imu",            "subdir": "imu",    "prefix": "imu_"},
    {"name": "water_pressure", "topic": "buoyV3/water_pressure", "subdir": "water",  "prefix": "water_raw_"},
    {"name": "serial",         "topic": "buoyV3/serial",         "subdir": "serial", "prefix": "serial_"},
    {"name": "gps",            "topic": "buoyV3/gps",            "subdir": "gps",    "prefix": "gps_data_"},
    {"name": "scc",            "topic": "buoyV3/scc",            "subdir": "mppt",   "prefix": "mppt_log_"},
    {"name": "cpu",            "topic": "buoyV3/cpu",            "subdir": "cpu",    "prefix": "dataOS_"},
]

log = logging.getLogger("uploader")


# =============================================================================
# CHECKPOINT (penanda data yang sudah diambil)
# =============================================================================
class Checkpoint:
    """Menyimpan byte offset terakhir yang sudah sukses dikirim, per file.

    Format checkpoint.json:
    {
      "version": 1,
      "files": {
        "wind/wind_2026-09-11.csv": {"offset": 184320, "inode": 123, "updated": "..."},
        ...
      }
    }
    Offset menunjuk ke awal baris berikutnya yang BELUM terkirim.
    """

    def __init__(self, path):
        self.path = path
        self.files = {}
        self._load()

    def _load(self):
        if not os.path.exists(self.path):
            log.info("[CKPT] Belum ada checkpoint, mulai baru: %s", self.path)
            return
        try:
            with open(self.path, "r") as f:
                data = json.load(f)
            self.files = data.get("files", {})
            log.info("[CKPT] Checkpoint dimuat (%d file)", len(self.files))
        except Exception as e:
            # Checkpoint rusak: jangan crash. Simpan salinan untuk investigasi.
            bad = self.path + ".corrupt-" + datetime.now().strftime("%Y%m%d%H%M%S")
            try:
                os.replace(self.path, bad)
            except OSError:
                pass
            log.error("[CKPT] Checkpoint rusak (%s) -> dipindah ke %s, mulai baru", e, bad)
            self.files = {}

    def has(self, key):
        return key in self.files

    def get(self, key):
        return self.files.get(key)

    def set(self, key, offset, inode):
        self.files[key] = {
            "offset": int(offset),
            "inode": int(inode),
            "updated": datetime.now().isoformat(timespec="seconds"),
        }

    def prune(self, existing_keys):
        """Buang entri untuk file yang sudah tidak ada di disk."""
        gone = [k for k in self.files if k not in existing_keys]
        for k in gone:
            del self.files[k]
        if gone:
            log.info("[CKPT] %d entri file yang sudah terhapus dibuang", len(gone))
        return len(gone)

    def save(self):
        """Simpan atomik: file sementara -> fsync -> os.replace.
        Listrik mati di tengah penulisan tidak akan merusak checkpoint lama."""
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        tmp = self.path + ".tmp"
        with open(tmp, "w") as f:
            json.dump({"version": 1, "files": self.files}, f, indent=1, sort_keys=True)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.path)


# =============================================================================
# PENCARIAN FILE
# =============================================================================
def list_stream_files(stream, data_dir):
    """Kembalikan [(tanggal, path, key)] terurut dari tanggal paling lama."""
    folder = os.path.join(data_dir, stream["subdir"])
    if not os.path.isdir(folder):
        return []
    pat = re.compile(r"^" + re.escape(stream["prefix"]) + r"(\d{4}-\d{2}-\d{2})\.csv$")
    out = []
    for fname in os.listdir(folder):
        m = pat.match(fname)
        if not m:
            continue
        try:
            d = datetime.strptime(m.group(1), "%Y-%m-%d").date()
        except ValueError:
            continue
        out.append((d, os.path.join(folder, fname), f"{stream['subdir']}/{fname}"))
    out.sort()
    return out


# =============================================================================
# PEMBACAAN CSV
# =============================================================================
def read_header(path):
    """Baca baris header. Kembalikan (fields, offset_akhir_header) atau
    (None, 0) bila header belum lengkap tertulis."""
    with open(path, "rb") as f:
        line = f.readline()
    if not line.endswith(b"\n"):
        return None, 0
    text = line.decode("utf-8", errors="replace").lstrip("\ufeff")
    fields = next(csv.reader([text]))
    fields = [c.strip() for c in fields]
    if not fields or not any(fields):
        return None, 0
    return fields, len(line)


def read_complete_lines(path, offset, max_bytes):
    """Baca dari `offset` maksimal `max_bytes` byte, dipotong di newline
    terakhir. Kembalikan (bytes_blok, offset_akhir). Blok kosong berarti
    belum ada baris lengkap baru."""
    with open(path, "rb") as f:
        f.seek(0, os.SEEK_END)
        size = f.tell()
        if offset >= size:
            return b"", offset
        f.seek(offset)
        buf = f.read(min(max_bytes, size - offset))
        cut = buf.rfind(b"\n")
        if cut == -1:
            if offset + len(buf) >= size:
                # Hanya ada potongan baris di ujung file: tunggu siklus berikut.
                return b"", offset
            # Satu baris lebih panjang dari max_bytes (tidak wajar): baca
            # terus sampai ketemu newline supaya tidak macet selamanya.
            while True:
                more = f.read(64 * 1024)
                if not more:
                    return b"", offset
                buf += more
                cut = buf.rfind(b"\n")
                if cut != -1:
                    break
        block = buf[:cut + 1]
        return block, offset + len(block)


def split_lines_with_offsets(block, base_offset):
    """Pecah blok menjadi [(start, end, bytes_baris)]."""
    lines = []
    pos = 0
    n = len(block)
    while pos < n:
        nl = block.find(b"\n", pos)
        if nl == -1:
            break
        lines.append((base_offset + pos, base_offset + nl + 1, block[pos:nl + 1]))
        pos = nl + 1
    return lines


_INT_RE = re.compile(r"^[+-]?\d+$")


def convert_value(v):
    """String CSV -> tipe JSON yang wajar.
    ''/None/nan -> null, bilangan bulat -> int, desimal -> float, lainnya string."""
    v = v.strip()
    if v == "" or v.lower() in ("none", "null", "nan"):
        return None
    if _INT_RE.match(v):
        try:
            return int(v)
        except ValueError:
            pass
    try:
        x = float(v)
        return x if math.isfinite(x) else None
    except ValueError:
        return v


def parse_rows(lines, fields):
    """Parse baris CSV. Baris rusak (jumlah kolom beda) atau header ganda
    dilewati. Kembalikan (rows, n_bad)."""
    text = b"".join(l[2] for l in lines).decode("utf-8", errors="replace")
    rows, n_bad = [], 0
    header_stripped = [f.strip() for f in fields]
    for rec in csv.reader(io.StringIO(text)):
        if not rec or (len(rec) == 1 and rec[0].strip() == ""):
            continue                                  # baris kosong
        if [c.strip() for c in rec] == header_stripped:
            continue                                  # header tertulis ulang
        if len(rec) != len(fields):
            n_bad += 1
            continue
        rows.append([convert_value(c) for c in rec])
    return rows, n_bad


# =============================================================================
# BATCH
# =============================================================================
def build_batch(stream, key, fields, rows, start, end):
    """Satu chunk = satu paket. batch_id unik dipakai database untuk
    membuang duplikat bila paket yang sama terkirim dua kali."""
    fname = os.path.basename(key)
    return {
        "device": DEVICE_ID,
        "stream": stream["name"],
        "batch_id": f"{DEVICE_ID}:{stream['name']}:{fname}:{start}-{end}",
        "file": fname,
        "offset_start": start,
        "offset_end": end,
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "n_rows": len(rows),
        "fields": fields,
        "rows": rows,
    }


def encode_batch(batch):
    """JSON ringkas -> gzip. Dipakai dry-run (ukur ukuran) dan MQTT (tahap 2)."""
    raw = json.dumps(batch, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return raw, gzip.compress(raw, compresslevel=6)


# =============================================================================
# SINK
# =============================================================================
class DryRunSink:
    """Tahap 1: tidak mengirim ke mana-mana. Mencatat ringkasan paket dan
    (opsional) menyimpannya ke file JSON agar isinya bisa diperiksa."""

    def __init__(self, save_dir=None):
        self.save_dir = save_dir
        self.total_raw = 0
        self.total_gz = 0

    def send(self, topic, batch):
        raw, gz = encode_batch(batch)
        self.total_raw += len(raw)
        self.total_gz += len(gz)
        log.info("[DRY] %-22s %4d baris | %7.1f KB json -> %6.1f KB gzip | %s",
                 topic, batch["n_rows"], len(raw) / 1024, len(gz) / 1024, batch["batch_id"])
        if self.save_dir:
            d = os.path.join(self.save_dir, batch["stream"])
            os.makedirs(d, exist_ok=True)
            fn = f"{batch['file'][:-4]}_{batch['offset_start']}-{batch['offset_end']}.json"
            with open(os.path.join(d, fn), "w") as f:
                json.dump(batch, f, ensure_ascii=False)
        return True


class MqttSink:
    """Tahap 2: kirim paket ke broker MQTT dengan QoS 1.

    send() baru mengembalikan True setelah broker membalas PUBACK. Kalau
    koneksi putus atau PUBACK tidak datang dalam MQTT_PUBACK_TIMEOUT detik,
    send() mengembalikan False -> checkpoint tidak maju -> data dicoba lagi
    di siklus berikutnya (tetap aman di CSV).

    Koneksi dibuka di awal siklus dan ditutup di akhir siklus, supaya uploader
    tidak menahan socket selama 5 menit menganggur.
    """

    def __init__(self, broker=MQTT_BROKER, port=MQTT_PORT, user=MQTT_USER,
                 password=MQTT_PASSWORD, client_id=MQTT_CLIENT_ID,
                 topic_prefix=TOPIC_PREFIX, compress=True):
        import paho.mqtt.client as mqtt          # import di sini: dry-run tak perlu paho
        self._mqtt = mqtt
        self.broker, self.port = broker, port
        self.user, self.password = user, password
        self.client_id = client_id
        self.topic_prefix = topic_prefix
        self.compress = compress
        self.client = None
        self._connected = threading.Event()
        self._acks = set()
        self._lock = threading.Lock()
        self._ack_event = threading.Event()
        self._sengaja_tutup = False      # bedakan disconnect normal vs putus mendadak
        self.total_raw = 0
        self.total_sent = 0
        self.n_batches = 0

    # -- callback --
    def _on_connect(self, c, userdata, flags, reason_code, properties=None):
        if reason_code == 0:
            self._connected.set()
            log.info("[MQTT] Tersambung ke %s:%s", self.broker, self.port)
        else:
            log.error("[MQTT] Gagal connect, rc=%s", reason_code)

    def _on_disconnect(self, c, userdata, *a):
        self._connected.clear()
        if self._sengaja_tutup:
            log.info("[MQTT] Koneksi ditutup (siklus selesai)")
        else:
            log.warning("[MQTT] Koneksi terputus")

    def _on_publish(self, c, userdata, mid, *a):
        with self._lock:
            self._acks.add(mid)
        self._ack_event.set()

    # -- siklus hidup --
    def open(self):
        if self.client is not None and self._connected.is_set():
            return True
        self.close()
        c = self._mqtt.Client(self._mqtt.CallbackAPIVersion.VERSION2,
                              client_id=self.client_id, clean_session=True)
        c.username_pw_set(self.user, self.password)
        c.max_inflight_messages_set(MQTT_MAX_INFLIGHT)
        c.on_connect = self._on_connect
        c.on_disconnect = self._on_disconnect
        c.on_publish = self._on_publish
        self._connected.clear()
        self._acks.clear()
        self._sengaja_tutup = False
        try:
            c.connect(self.broker, self.port, keepalive=MQTT_KEEPALIVE)
        except Exception as e:
            log.error("[MQTT] Tidak bisa menghubungi broker: %s", e)
            return False
        c.loop_start()
        self.client = c
        if not self._connected.wait(MQTT_CONNECT_TIMEOUT):
            log.error("[MQTT] Tidak ada CONNACK dalam %ss", MQTT_CONNECT_TIMEOUT)
            self.close()
            return False
        return True

    def close(self):
        if self.client is not None:
            self._sengaja_tutup = True
            try:
                self.client.loop_stop()
                self.client.disconnect()
            except Exception:
                pass
            self.client = None
        self._connected.clear()

    # -- kirim --
    def send(self, topic, batch):
        if self.client is None or not self._connected.is_set():
            if not self.open():
                return False
        raw, gz = encode_batch(batch)
        payload = gz if self.compress else raw
        full_topic = self.topic_prefix + topic
        try:
            info = self.client.publish(full_topic, payload, qos=1, retain=False)
        except Exception as e:
            log.error("[MQTT] publish error: %s", e)
            return False
        if info.rc != self._mqtt.MQTT_ERR_SUCCESS:
            log.error("[MQTT] publish ditolak, rc=%s", info.rc)
            return False

        # Tunggu PUBACK. Putus koneksi di tengah tunggu -> gagal (paket
        # dikirim ulang di siklus berikutnya; batch_id dipakai untuk dedup).
        deadline = time.monotonic() + MQTT_PUBACK_TIMEOUT
        while True:
            with self._lock:
                if info.mid in self._acks:
                    self._acks.discard(info.mid)
                    break
            if not self._connected.is_set():
                log.warning("[MQTT] Putus sebelum PUBACK (%s)", batch["batch_id"])
                return False
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                log.warning("[MQTT] PUBACK timeout %ss (%s)",
                            MQTT_PUBACK_TIMEOUT, batch["batch_id"])
                return False
            self._ack_event.clear()
            self._ack_event.wait(min(0.5, remaining))

        self.total_raw += len(raw)
        self.total_sent += len(payload)
        self.n_batches += 1
        log.info("[MQTT] %-24s %4d baris | %7.1f KB -> %6.1f KB terkirim | %s",
                 full_topic, batch["n_rows"], len(raw) / 1024,
                 len(payload) / 1024, batch["batch_id"])
        return True


# =============================================================================
# SIKLUS
# =============================================================================
class SinkDown(Exception):
    """Sink gagal: hentikan siklus ini, coba lagi di siklus berikutnya."""


def process_file(stream, path, key, ckpt, sink, budget):
    """Ambil data baru dari satu file. Kembalikan (rows_sent, bytes_used)."""
    st = os.stat(path)
    entry = ckpt.get(key)
    offset = entry["offset"] if entry else 0

    if entry and (entry.get("inode") != st.st_ino or st.st_size < offset):
        log.warning("[%s] %s diganti/terpotong (inode/ukuran berubah) -> baca ulang dari awal",
                    stream["name"], key)
        offset = 0

    fields, header_end = read_header(path)
    if fields is None:
        return 0, 0                                   # header belum lengkap
    if offset < header_end:
        offset = header_end
        ckpt.set(key, offset, st.st_ino)              # header dilewati

    rows_sent, used = 0, 0
    while used < budget:
        block, new_off = read_complete_lines(path, offset, budget - used)
        if not block:
            break
        lines = split_lines_with_offsets(block, offset)
        used += len(block)

        for i in range(0, len(lines), MAX_ROWS_PER_CHUNK):
            chunk = lines[i:i + MAX_ROWS_PER_CHUNK]
            c_start, c_end = chunk[0][0], chunk[-1][1]
            rows, n_bad = parse_rows(chunk, fields)
            if n_bad:
                log.warning("[%s] %s: %d baris dilewati (jumlah kolom != header %d)",
                            stream["name"], key, n_bad, len(fields))
            if rows:
                batch = build_batch(stream, key, fields, rows, c_start, c_end)
                ok = False
                try:
                    ok = sink.send(stream["topic"], batch)
                except Exception as e:
                    log.error("[%s] sink error: %s", stream["name"], e)
                if not ok:
                    ckpt.save()                       # simpan kemajuan sejauh ini
                    raise SinkDown(stream["name"])
                rows_sent += len(rows)
            # Chunk sukses (atau isinya rusak semua) -> checkpoint maju.
            ckpt.set(key, c_end, st.st_ino)
            ckpt.save()
        offset = new_off
    return rows_sent, used


def run_cycle(streams, ckpt, sink, data_dir, today=None):
    """Satu siklus pengambilan untuk semua stream. Kembalikan ringkasan."""
    today = today or date.today()
    oldest_new = today - timedelta(days=BACKFILL_DAYS)
    t0 = time.monotonic()
    summary = {}
    existing = set()
    aborted = False

    opener = getattr(sink, "open", None)
    if opener is not None and not opener():
        log.warning("[CYCLE] Sink tidak siap (broker tak terjangkau) -> "
                    "siklus dilewati, data tetap aman di CSV")
        return {"rows": {}, "aborted": True, "duration_s": 0.0}

    for s in streams:
        files = list_stream_files(s, data_dir)
        existing.update(k for _, _, k in files)
        if aborted:
            continue
        budget = MAX_BYTES_PER_STREAM
        sent = 0
        for d, path, key in files:
            if not ckpt.has(key) and d < oldest_new:
                # Arsip lama yang belum pernah dilacak: tandai "sudah", jangan kirim.
                ckpt.set(key, os.path.getsize(path), os.stat(path).st_ino)
                ckpt.save()
                log.info("[%s] %s arsip lama (< %s) -> ditandai, tidak dikirim",
                         s["name"], key, oldest_new)
                continue
            if budget <= 0:
                break
            try:
                n, used = process_file(s, path, key, ckpt, sink, budget)
            except SinkDown:
                log.warning("[CYCLE] Sink gagal di stream '%s' -> siklus dihentikan, "
                            "data tetap di CSV & dicoba lagi siklus berikutnya", s["name"])
                aborted = True
                break
            except FileNotFoundError:
                continue
            except Exception as e:
                log.exception("[%s] Error membaca %s: %s", s["name"], key, e)
                continue
            sent += n
            budget -= used
        summary[s["name"]] = sent
        if budget <= 0:
            log.info("[%s] batas %d MB/siklus tercapai, sisa backlog lanjut siklus berikutnya",
                     s["name"], MAX_BYTES_PER_STREAM // (1024 * 1024))

    closer = getattr(sink, "close", None)
    if closer is not None:
        closer()

    if ckpt.prune(existing):
        ckpt.save()
    dt = time.monotonic() - t0
    total = sum(summary.values())
    log.info("[CYCLE] selesai %.2f s | %d baris | %s%s", dt, total,
             ", ".join(f"{k}={v}" for k, v in summary.items()),
             " | DIHENTIKAN (sink gagal)" if aborted else "")
    return {"rows": summary, "aborted": aborted, "duration_s": dt}


def pending_bytes(streams, ckpt, data_dir):
    """Byte yang belum diambil per stream: {nama: (byte, jumlah_file)}.
    File arsip lama yang belum pernah dilacak juga dihitung."""
    out = {}
    for s in streams:
        pend, nfiles = 0, 0
        for _, path, key in list_stream_files(s, data_dir):
            size = os.path.getsize(path)
            e = ckpt.get(key)
            off = e["offset"] if e else 0
            if size > off:
                pend += size - off
                nfiles += 1
        out[s["name"]] = (pend, nfiles)
    return out


def pending_report(streams, ckpt, data_dir):
    """Teks ringkasan untuk --status."""
    return "\n".join(f"  {name:<15} belum diambil: {b / 1024:9.1f} KB di {n} file"
                     for name, (b, n) in pending_bytes(streams, ckpt, data_dir).items())


def drain(streams, ckpt, sink, data_dir, max_cycles=100000):
    """Uji backlog: jalankan siklus berturut-turut (tanpa menunggu periode)
    sampai tidak ada lagi baris lengkap yang bisa diambil."""
    t0 = time.monotonic()
    totals = {s["name"]: 0 for s in streams}
    last_pending = None
    for i in range(1, max_cycles + 1):
        r = run_cycle(streams, ckpt, sink, data_dir)
        for k, v in r["rows"].items():
            totals[k] += v
        if r["aborted"]:
            log.warning("[DRAIN] berhenti: sink gagal")
            break
        pending = sum(b for b, _ in pending_bytes(streams, ckpt, data_dir).values())
        if pending == 0 or pending == last_pending:
            break          # habis, atau sisa hanya potongan baris yang belum lengkap
        last_pending = pending
    dt = time.monotonic() - t0
    log.info("[DRAIN] %d siklus, %.1f s | total baris: %s", i, dt,
             ", ".join(f"{k}={v}" for k, v in totals.items()))
    return totals


# =============================================================================
# MAIN LOOP
# =============================================================================
def next_tick(period, align):
    now = time.time()
    if align:
        return (math.floor(now / period) + 1) * period
    return now + period


def _report_totals(sink):
    if isinstance(sink, DryRunSink):
        log.info("Total dry-run: %.1f KB json -> %.1f KB gzip",
                 sink.total_raw / 1024, sink.total_gz / 1024)
    elif isinstance(sink, MqttSink):
        hemat = (1 - sink.total_sent / sink.total_raw) * 100 if sink.total_raw else 0
        log.info("Total MQTT: %d paket | %.1f KB json -> %.1f KB terkirim (hemat %.0f%%)",
                 sink.n_batches, sink.total_raw / 1024, sink.total_sent / 1024, hemat)


def main():
    global BACKFILL_DAYS, MAX_BYTES_PER_STREAM
    ap = argparse.ArgumentParser(description="U-TEWS Buoy-V3 store-and-forward uploader (tahap 1: dry-run)")
    ap.add_argument("--once", action="store_true", help="jalankan satu siklus lalu keluar")
    ap.add_argument("--period", type=int, default=PERIOD_S, help="periode siklus (detik)")
    ap.add_argument("--data-dir", default=DATA_DIR)
    ap.add_argument("--state-dir", default=STATE_DIR)
    ap.add_argument("--save-batches", action="store_true",
                    help="simpan batch dry-run ke <state-dir>/dryrun_batches/")
    ap.add_argument("--status", action="store_true", help="tampilkan sisa data lalu keluar")
    ap.add_argument("--backfill-days", type=int, default=BACKFILL_DAYS,
                    help="file yang lebih tua dari N hari (dan belum pernah dilacak) "
                         "dianggap arsip dan tidak dikirim. Untuk uji data lama: mis. 30")
    ap.add_argument("--max-mb", type=float, default=MAX_BYTES_PER_STREAM / (1024 * 1024),
                    help="batas baca per stream per siklus (MB)")
    ap.add_argument("--drain", action="store_true",
                    help="uji backlog: siklus berturut-turut sampai semua data habis, lalu keluar")
    ap.add_argument("--mqtt", action="store_true",
                    help="AKTIFKAN pengiriman MQTT (tanpa ini: dry-run, tidak mengirim apa pun)")
    ap.add_argument("--broker", default=MQTT_BROKER)
    ap.add_argument("--port", type=int, default=MQTT_PORT)
    ap.add_argument("--topic-prefix", default=TOPIC_PREFIX,
                    help="awalan topik, mis. 'batch/' -> batch/buoyV3/wind (untuk uji paralel)")
    ap.add_argument("--no-compress", action="store_true",
                    help="kirim JSON apa adanya tanpa gzip (untuk uji/debug di sisi server)")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    BACKFILL_DAYS = args.backfill_days
    MAX_BYTES_PER_STREAM = int(args.max_mb * 1024 * 1024)

    ckpt = Checkpoint(os.path.join(args.state_dir, CHECKPOINT_FILE))

    if args.status:
        print(pending_report(STREAMS, ckpt, args.data_dir))
        return 0

    if args.mqtt:
        sink = MqttSink(broker=args.broker, port=args.port,
                        topic_prefix=args.topic_prefix,
                        compress=not args.no_compress)
        mode = f"MQTT -> {args.broker}:{args.port}"
        if args.topic_prefix:
            mode += f" (prefix '{args.topic_prefix}')"
        if args.no_compress:
            mode += " [tanpa gzip]"
    else:
        save_dir = os.path.join(args.state_dir, "dryrun_batches") if args.save_batches else None
        sink = DryRunSink(save_dir=save_dir)
        mode = "DRY-RUN (tidak mengirim)"

    log.info("Uploader start | data=%s | state=%s | periode=%ds | chunk=%d baris | mode=%s",
             args.data_dir, args.state_dir, args.period, MAX_ROWS_PER_CHUNK, mode)

    if args.once or args.drain:
        if args.drain:
            drain(STREAMS, ckpt, sink, args.data_dir)
        else:
            run_cycle(STREAMS, ckpt, sink, args.data_dir)
        _report_totals(sink)
        return 0

    stop = threading.Event()

    def _stop(signum, _frame):
        log.info("Sinyal %s diterima, berhenti setelah siklus berjalan selesai...", signum)
        stop.set()

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    if RUN_AT_STARTUP:
        run_cycle(STREAMS, ckpt, sink, args.data_dir)

    while not stop.is_set():
        t_next = next_tick(args.period, ALIGN_TO_CLOCK)
        log.info("Siklus berikutnya: %s", datetime.fromtimestamp(t_next).strftime("%H:%M:%S"))
        if stop.wait(max(0.0, t_next - time.time())):
            break
        run_cycle(STREAMS, ckpt, sink, args.data_dir)

    if hasattr(sink, "close"):
        sink.close()
    ckpt.save()
    _report_totals(sink)
    log.info("Uploader berhenti. Checkpoint tersimpan.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
