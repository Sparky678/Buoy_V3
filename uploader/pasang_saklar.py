#!/usr/bin/env python3
"""
pasang_saklar.py — Memasang saklar REALTIME_PUBLISH di script akuisisi Buoy-V3.

CARA KERJANYA
-------------
Skrip ini TIDAK menyentuh baris publish satu per satu. Sebagai gantinya, tepat
setelah objek MQTT client dibuat, disisipkan beberapa baris yang membuat
fungsi publish milik client itu menjadi "tidak melakukan apa-apa" ketika
saklar dimatikan:

    REALTIME_PUBLISH = os.environ.get("BUOY_REALTIME_PUBLISH", "1") != "0"
    if not REALTIME_PUBLISH:
        class _NoPub:                 # meniru hasil publish yang sukses
            rc = 0
            mid = 0
            def is_published(self): return True
            def wait_for_publish(self, timeout=None): return None
        client.publish = lambda *a, **k: _NoPub()

Dengan cara ini, semua kode setelahnya (pengecekan info.rc, print status,
dan sebagainya) tetap berjalan normal tanpa error. Yang berubah hanya:
pesan tidak jadi dikirim.

Penulisan CSV, akuisisi sensor, dan alert tsunami TIDAK disentuh sama sekali.

  - Tanpa pengaturan apa pun      -> publish real-time TETAP JALAN (aman).
  - BUOY_REALTIME_PUBLISH=0       -> publish real-time mati, pengiriman ke
                                     database diserahkan ke uploader.py.

PEMAKAIAN
---------
    python3 pasang_saklar.py --cek      # lihat rencana perubahan, tanpa mengubah
    python3 pasang_saklar.py --pasang   # terapkan + buat cadangan .bak
    python3 pasang_saklar.py --batal    # kembalikan dari cadangan .bak
"""
import argparse
import os
import re
import shutil
import sys

ROOT = os.path.expanduser("~/Buoy-V3")

TARGETS = [
    "modbus/wind_monitoring.py",
    "modbus/water_monitoring_10hz.py",
    "CPU/cpu.py",
    "gps/gps.py",
    "scc/mppt.py",
    "serial/serial.py",
]

CLIENT_RE = re.compile(r'^(?P<indent>[ \t]*)(?P<var>[A-Za-z_]\w*)\s*=\s*mqtt\.Client\(')
MARKER = "REALTIME_PUBLISH"

BLOK = '''{i}{marker} = os.environ.get("BUOY_REALTIME_PUBLISH", "1") != "0"
{i}if not {marker}:
{i}    # Saklar OFF: pengiriman ke database ditangani uploader.py (store-and-forward).
{i}    # publish() diganti fungsi kosong yang tetap melaporkan "sukses",
{i}    # supaya kode di bawahnya (cek rc, print status) tidak error.
{i}    class _NoPub:
{i}        rc = 0
{i}        mid = 0
{i}        def is_published(self):
{i}            return True
{i}        def wait_for_publish(self, timeout=None):
{i}            return None
{i}    {var}.publish = lambda *a, **k: _NoPub()
'''


def eol(text):
    return "\r\n" if "\r\n" in text else "\n"


def pastikan_import_os(lines, nl):
    if any(re.match(r'^\s*import os\b', l) for l in lines):
        return lines, False
    last = 0
    for i, l in enumerate(lines[:120]):
        if re.match(r'^\s*(import|from)\s+\w', l):
            last = i
    return lines[:last + 1] + ["import os" + nl] + lines[last + 1:], True


def proses(path, cek):
    with open(path, "r", newline="") as f:
        text = f.read()
    if MARKER in text:
        return "sudah terpasang", 0
    nl = eol(text)
    lines = text.splitlines(keepends=True)
    lines, tambah_os = pastikan_import_os(lines, nl)

    out, n = [], 0
    i = 0
    while i < len(lines):
        l = lines[i]
        out.append(l)
        m = CLIENT_RE.match(l)
        if m and not l.lstrip().startswith("#"):
            # lewati baris lanjutan bila pemanggilan Client(...) multi-baris
            depth = l.count("(") - l.count(")")
            while depth > 0 and i + 1 < len(lines):
                i += 1
                out.append(lines[i])
                depth += lines[i].count("(") - lines[i].count(")")
            blok = BLOK.format(i=m.group("indent"), var=m.group("var"), marker=MARKER)
            out.append(nl)
            out.extend(x + nl for x in blok.splitlines())
            n += 1
        i += 1

    if n == 0:
        return "TIDAK ADA mqtt.Client() ditemukan", -1

    baru = "".join(out)
    try:
        compile(baru, path, "exec")
    except SyntaxError as e:
        return f"GAGAL: hasil tidak valid ({e})", -1

    if not cek:
        shutil.copy2(path, path + ".bak")
        with open(path, "w", newline="") as f:
            f.write(baru)
    ket = f"{n} client dipasangi saklar"
    if tambah_os:
        ket += " (+ import os)"
    return ket, n


def main():
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--cek", action="store_true", help="tampilkan rencana, jangan ubah apa pun")
    g.add_argument("--pasang", action="store_true", help="terapkan perubahan")
    g.add_argument("--batal", action="store_true", help="kembalikan dari cadangan .bak")
    ap.add_argument("--root", default=ROOT)
    a = ap.parse_args()

    gagal = False
    for rel in TARGETS:
        path = os.path.join(a.root, rel)
        if not os.path.exists(path):
            print(f"  {rel:<35} TIDAK DITEMUKAN")
            gagal = True
            continue
        if a.batal:
            bak = path + ".bak"
            if os.path.exists(bak):
                shutil.move(bak, path)
                print(f"  {rel:<35} dikembalikan dari cadangan")
            else:
                print(f"  {rel:<35} tidak ada cadangan")
            continue
        msg, n = proses(path, cek=a.cek)
        print(f"  {rel:<35} {msg}")
        if n < 0:
            gagal = True

    if a.cek:
        print("\nIni hanya simulasi, tidak ada file yang diubah.")
        print("Jalankan dengan --pasang untuk menerapkannya.")
    elif a.pasang:
        print("\nSelesai. Cadangan asli disimpan sebagai *.bak di folder yang sama.")
        print("Publish real-time MASIH AKTIF. Untuk mematikannya, jalankan script")
        print("dengan BUOY_REALTIME_PUBLISH=0 (lihat panduan).")
    return 1 if gagal else 0


if __name__ == "__main__":
    sys.exit(main())
