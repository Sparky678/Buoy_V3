#!/usr/bin/env python3
"""
test_uploader.py — Uji otomatis uploader.py tahap 1 (tanpa hardware, tanpa MQTT).

Menjalankan:  python3 test_uploader.py
Semua uji memakai folder sementara; tidak menyentuh /home/orangepi/data.
"""
import csv
import os
import random
import shutil
import tempfile
import threading
import time
import logging
from datetime import date, timedelta

import uploader as up

logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")

WIND_FIELDS = ["timestamp", "WindSpeed_ms", "WindSpeed_flag", "Beaufort_scale",
               "WindAngle_deg", "WindAngle_flag", "WindDirection", "Roll_deg", "Pitch_deg",
               "Heading_deg", "Heading_direction", "WindTrue_deg", "WindTrue_direction"]

TODAY = date(2026, 9, 11)


class CollectSink:
    """Sink uji: mengumpulkan baris; bisa dipaksa gagal."""
    def __init__(self):
        self.rows = []          # (stream, timestamp)
        self.batches = []
        self.fail_after = None  # gagal setelah N paket sukses
        self.fail_prob = 0.0

    def send(self, topic, batch):
        if self.fail_after is not None and len(self.batches) >= self.fail_after:
            return False
        if self.fail_prob and random.random() < self.fail_prob:
            return False
        up.encode_batch(batch)  # pastikan bisa di-encode (JSON valid)
        self.batches.append(batch)
        ts_idx = 0
        for r in batch["rows"]:
            self.rows.append((batch["stream"], r[ts_idx]))
        return True


def wind_row(i, d=TODAY):
    ts = f"{d} 10:{(i // 600) % 60:02d}:{(i // 10) % 60:02d}.{(i % 10) * 100:03d}#{i}"
    return {"timestamp": ts, "WindSpeed_ms": round(3 + (i % 7) * 0.1, 2), "WindSpeed_flag": "OK",
            "Beaufort_scale": 2, "WindAngle_deg": 145.0, "WindAngle_flag": "OK",
            "WindDirection": "SE", "Roll_deg": None, "Pitch_deg": -1.5, "Heading_deg": 12.3,
            "Heading_direction": "N", "WindTrue_deg": 157.3, "WindTrue_direction": "SSE"}


def write_wind(data_dir, rows, d=TODAY, header=True):
    path = os.path.join(data_dir, "wind", f"wind_{d}.csv")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    new = not os.path.exists(path)
    with open(path, "a", newline="") as f:           # sama seperti wind_monitoring.py
        w = csv.DictWriter(f, fieldnames=WIND_FIELDS)
        if new and header:
            w.writeheader()
        w.writerows(rows)
    return path


class Env:
    def __init__(self):
        self.root = tempfile.mkdtemp(prefix="uptest_")
        self.data = os.path.join(self.root, "data")
        self.state = os.path.join(self.root, "state")
        os.makedirs(self.data)

    def ckpt(self):
        return up.Checkpoint(os.path.join(self.state, "checkpoint.json"))

    def cycle(self, sink, today=TODAY, ckpt=None):
        return up.run_cycle(up.STREAMS, ckpt or self.ckpt(), sink, self.data, today=today)

    def close(self):
        shutil.rmtree(self.root)


def ids(sink, stream="wind"):
    return [int(ts.split("#")[1]) for s, ts in sink.rows if s == stream]


# ----------------------------------------------------------------------------- tests
def test_basic_and_idempotent():
    e = Env(); s = CollectSink()
    write_wind(e.data, [wind_row(i) for i in range(1234)])
    e.cycle(s)
    assert ids(s) == list(range(1234)), "semua baris harus terkirim berurutan"
    assert len(s.batches) == 3, f"1234 baris = 3 paket (500/500/234), dapat {len(s.batches)}"
    e.cycle(s)
    assert len(ids(s)) == 1234, "siklus tanpa data baru tidak boleh mengirim ulang"
    b = s.batches[0]
    assert b["fields"] == WIND_FIELDS and b["rows"][0][7] is None and b["rows"][0][3] == 2
    assert isinstance(b["rows"][0][1], float) and b["rows"][0][2] == "OK"
    e.close()


def test_partial_line():
    e = Env(); s = CollectSink()
    path = write_wind(e.data, [wind_row(i) for i in range(10)])
    with open(path, "ab") as f:                       # baris setengah tertulis
        f.write(b"2026-09-11 10:00:01.000#10,3.1,OK,2,14")
    e.cycle(s)
    assert ids(s) == list(range(10)), "baris setengah jadi tidak boleh diambil"
    with open(path, "ab") as f:                       # baris dilengkapi
        f.write(b"5.0,OK,SE,,-1.5,12.3,N,157.3,SSE\r\n")
    e.cycle(s)
    assert ids(s) == list(range(11)), "baris yang sudah lengkap diambil di siklus berikutnya"
    e.close()


def test_sink_failure_then_recover():
    e = Env(); s = CollectSink()
    write_wind(e.data, [wind_row(i) for i in range(1600)])
    s.fail_after = 2                                  # paket ke-3 gagal
    r = e.cycle(s)
    assert r["aborted"] and ids(s) == list(range(1000))
    s.fail_after = None
    e.cycle(s)
    assert ids(s) == list(range(1600)), "setelah pulih: lanjut tepat dari paket yang gagal"
    e.close()


def test_restart_persists():
    e = Env(); s = CollectSink()
    write_wind(e.data, [wind_row(i) for i in range(300)])
    e.cycle(s, ckpt=e.ckpt())
    write_wind(e.data, [wind_row(i) for i in range(300, 450)])
    e.cycle(s, ckpt=e.ckpt())                         # objek checkpoint baru = restart
    assert ids(s) == list(range(450))
    e.close()


def test_day_rollover_and_old_archive():
    e = Env(); s = CollectSink()
    old = TODAY - timedelta(days=5)
    write_wind(e.data, [wind_row(i, old) for i in range(50)], d=old)       # arsip lama
    write_wind(e.data, [wind_row(i) for i in range(100)])                   # hari ini
    e.cycle(s)
    assert ids(s) == list(range(100)), "arsip lama tidak ikut dikirim"
    write_wind(e.data, [wind_row(i) for i in range(100, 120)])              # sisa hari ini
    tmr = TODAY + timedelta(days=1)
    write_wind(e.data, [wind_row(i, tmr) for i in range(120, 150)], d=tmr)  # hari baru
    e.cycle(s, today=tmr)
    assert ids(s) == list(range(150)), "sisa file kemarin + file baru terkirim berurutan"
    e.close()


def test_mixed_columns_mppt():
    e = Env(); s = CollectSink()
    p = os.path.join(e.data, "mppt", f"mppt_log_{TODAY}.csv")
    os.makedirs(os.path.dirname(p))
    with open(p, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["Timestamp", "PV_Voltage_V", "PV_Current_A", "PV_Power_W",
                    "Battery_Voltage_V", "Batt_Current_A", "SOC_Percent"])
        w.writerow([f"{TODAY} 10:00:00#0", 18.2, 1.1, 20.0, 12.9, 0.8, 87])
        w.writerow([f"{TODAY} 10:01:00#1"] + [1] * 12)                      # 13 kolom (mppt_lokal)
        w.writerow([f"{TODAY} 10:02:00#2", 18.0, 1.0, 18.0, 12.9, 0.7, 88])
    e.cycle(s)
    assert ids(s, "scc") == [0, 2], "baris 13 kolom dilewati, tidak crash"
    e.close()


def test_truncated_file():
    e = Env(); s = CollectSink()
    path = write_wind(e.data, [wind_row(i) for i in range(100)])
    e.cycle(s)
    os.remove(path)
    write_wind(e.data, [wind_row(i) for i in range(1000, 1010)])           # file dibuat ulang
    e.cycle(s)
    assert ids(s)[-10:] == list(range(1000, 1010)), "file baru dibaca dari awal"
    e.close()


def test_backlog_budget():
    e = Env(); s = CollectSink()
    write_wind(e.data, [wind_row(i) for i in range(20000)])
    old = up.MAX_BYTES_PER_STREAM
    up.MAX_BYTES_PER_STREAM = 1024 * 1024             # 1 MB/siklus
    try:
        cycles = 0
        while len(ids(s)) < 20000:
            e.cycle(s); cycles += 1
            assert cycles < 20
    finally:
        up.MAX_BYTES_PER_STREAM = old
    assert ids(s) == list(range(20000)) and cycles > 1, f"backlog dicicil dalam {cycles} siklus"
    e.close()


def test_concurrent_writer_10hz_stress():
    """Penulis 10 Hz meniru wind_monitoring (handle persisten, flush tiap ~10 baris,
    kadang flush di tengah baris) + uploader siklus cepat + sink kadang gagal.
    Syarat: setiap baris diterima TEPAT SEKALI dan berurutan."""
    e = Env(); s = CollectSink(); s.fail_prob = 0.15
    random.seed(1)
    N = 6000
    path = os.path.join(e.data, "wind", f"wind_{TODAY}.csv")
    os.makedirs(os.path.dirname(path))
    done = threading.Event()

    def writer():
        with open(path, "a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=WIND_FIELDS)
            w.writeheader()
            for i in range(N):
                w.writerow(wind_row(i))
                if random.random() < 0.1:
                    f.flush()
                if random.random() < 0.02:            # flush di tengah baris berikutnya
                    f.write("2026-09-11 99:99"); f.flush(); time.sleep(0.001)
                    f.write(":99.000#x,")              # akan membuat baris rusak -> dilewati
                    f.write(",".join([""] * 11) + "\r\n")
                if i % 50 == 0:
                    time.sleep(0.001)
        done.set()

    t = threading.Thread(target=writer); t.start()
    ck = e.ckpt()
    while not done.is_set():
        up.run_cycle(up.STREAMS, ck, s, e.data, today=TODAY)
        time.sleep(0.003)
    t.join()
    s.fail_prob = 0.0
    up.run_cycle(up.STREAMS, ck, s, e.data, today=TODAY)
    got = ids(s)
    assert got == list(range(N)), f"hilang/duplikat: {N - len(set(got))} hilang, {len(got) - len(set(got))} duplikat"
    e.close()


if __name__ == "__main__":
    tests = [v for k, v in list(globals().items()) if k.startswith("test_")]
    for fn in tests:
        t0 = time.time()
        fn()
        print(f"LULUS  {fn.__name__:<40} {time.time() - t0:5.2f} s")
    print(f"\nSemua {len(tests)} uji lulus.")
