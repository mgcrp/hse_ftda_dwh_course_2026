#!/usr/bin/env python3
"""block_map.py - рисует карту блоков HDFS: какой блок на какой DataNode лежит.

Под капотом просто парсит `hdfs fsck <path> -files -blocks -locations`.

    python3 block_map.py /data/blocks_demo/pg4300_1m.txt
    python3 block_map.py /data/small_files --max-files 5
    hdfs fsck /data/x -files -blocks -locations | python3 block_map.py -   # из stdin
"""

import argparse
import re
import socket
import subprocess
import sys

FILE_RE = re.compile(r"^(/\S*) (\d+) bytes,.*?(\d+) block\(s\)")
REPL_RE = re.compile(r"replication=(\d+)")
BLOCK_RE = re.compile(r"^\s*(\d+)\.\s+\S*?(blk_\d+)_\d+\s+len=(\d+)(?:\s+Live_repl=(\d+))?")
DN_RE = re.compile(r"DatanodeInfoWithStorage\[([^:,\]]+):\d+")
DEFAULT_BLOCKSIZE = 128 * 1024 * 1024
BAR_WIDTH = 20


def human(n):
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return ("%d %s" % (n, unit)) if unit == "B" else ("%.1f %s" % (n, unit))
        n /= 1024.0


def resolve(ip):
    try:
        return socket.gethostbyaddr(ip)[0].split(".")[0]
    except OSError:
        return ip


def default_blocksize():
    try:
        out = subprocess.run(["hdfs", "getconf", "-confKey", "dfs.blocksize"],
                             stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                             universal_newlines=True, check=True).stdout.strip()
        suffix = {"k": 1024, "m": 1024 ** 2, "g": 1024 ** 3}
        if out and out[-1].lower() in suffix:
            return int(out[:-1]) * suffix[out[-1].lower()]
        return int(out)
    except (OSError, ValueError, subprocess.CalledProcessError):
        return DEFAULT_BLOCKSIZE


def parse(lines):
    files = []
    for line in lines:
        m = FILE_RE.match(line)
        if m:
            repl = REPL_RE.search(line)
            files.append({"path": m.group(1), "size": int(m.group(2)),
                          "repl": int(repl.group(1)) if repl else None, "blocks": []})
            continue
        m = BLOCK_RE.match(line)
        if m and files:
            files[-1]["blocks"].append({
                "id": m.group(2), "len": int(m.group(3)),
                "live": int(m.group(4)) if m.group(4) else None,
                "nodes": DN_RE.findall(line),
            })
    return files


def bar(value, full):
    filled = max(1, int(round(BAR_WIDTH * min(value, full) / float(full)))) if value else 0
    return "█" * filled + "·" * (BAR_WIDTH - filled)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("path", help="путь в HDFS (файл или директория) или '-' для чтения вывода fsck из stdin")
    ap.add_argument("--max-files", type=int, default=10, help="сколько файлов рисовать подробно (default 10)")
    args = ap.parse_args()

    if args.path == "-":
        lines = sys.stdin.read().splitlines()
    else:
        proc = subprocess.run(["hdfs", "fsck", args.path, "-files", "-blocks", "-locations"],
                              stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, universal_newlines=True)
        lines = proc.stdout.splitlines()

    files = parse(lines)
    if not files:
        print("Не нашёл ни одного файла в выводе fsck. Проверьте путь и права.")
        print("\n".join(lines[-15:]))
        sys.exit(1)

    # DataNode'ы -> короткие имена dn1, dn2, ...
    ips = sorted({ip for f in files for b in f["blocks"] for ip in b["nodes"]})
    names = {ip: resolve(ip) for ip in ips}
    ips.sort(key=lambda ip: names[ip])
    alias = {ip: "dn%d" % (i + 1) for i, ip in enumerate(ips)}
    per_dn = {ip: 0 for ip in ips}
    fallback_bs = None

    total_blocks = sum(len(f["blocks"]) for f in files)
    logical = sum(f["size"] for f in files)
    raw = sum(b["len"] * len(b["nodes"]) for f in files for b in f["blocks"])

    for fi, f in enumerate(files):
        for b in f["blocks"]:
            for ip in b["nodes"]:
                per_dn[ip] += 1
        if fi >= args.max_files:
            continue

        if len(f["blocks"]) > 1:
            bs = f["blocks"][0]["len"]
        else:
            if fallback_bs is None:
                fallback_bs = default_blocksize() if args.path != "-" else DEFAULT_BLOCKSIZE
            bs = fallback_bs

        repl = "replication=%d" % f["repl"] if f["repl"] else ""
        print("\n📄 %s   %s   %d block(s)   blocksize≈%s   %s"
              % (f["path"], human(f["size"]), len(f["blocks"]), human(bs), repl))
        if not f["blocks"]:
            print("   (пустой файл - блоков нет, только запись в NameNode)")
            continue
        header = "   %3s  %-16s %-*s %10s   " % ("#", "block", BAR_WIDTH, "заполненность блока", "size")
        print(header + "  ".join("%-4s" % alias[ip] for ip in ips))
        for i, b in enumerate(f["blocks"]):
            cells = "  ".join("%-4s" % ("●" if ip in b["nodes"] else "·") for ip in ips)
            warn = ""
            if f["repl"] and len(b["nodes"]) < f["repl"]:
                warn = "  ⚠ реплик %d из %d" % (len(b["nodes"]), f["repl"])
            print("   %3d  %-16s %s %10s   %s%s" % (i, b["id"], bar(b["len"], bs), human(b["len"]), cells, warn))

    if len(files) > args.max_files:
        print("\n... и ещё %d файл(ов) не показаны (--max-files)" % (len(files) - args.max_files))

    print("\nDataNodes:")
    top = max(per_dn.values()) if per_dn else 0
    for ip in ips:
        n = per_dn[ip]
        width = int(round(30.0 * n / top)) if top else 0
        print("   %-4s %-28s %-15s %s %d блок(ов)" % (alias[ip], names[ip], ip, "█" * width, n))

    print("\nИтого: файлов %d, блоков %d (в среднем %s на блок)"
          % (len(files), total_blocks, human(logical / total_blocks) if total_blocks else "-"))
    print("       данных %s, на дисках кластера с репликами %s (x%.1f)"
          % (human(logical), human(raw), raw / float(logical) if logical else 0))


if __name__ == "__main__":
    main()
