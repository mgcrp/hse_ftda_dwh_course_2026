#!/usr/bin/env python3
"""block_boundary.py - показывает, что HDFS режет файл на блоки строго по байтам,
не глядя на строки и слова.

Печатает по N байт до и после каждой границы блока, граница помечена ‖.

    python3 block_boundary.py /data/blocks_demo/pg4300_1m.txt
    python3 block_boundary.py /data/blocks_demo/pg4300_1m.txt --context 150
"""

import argparse
import subprocess
import sys

MARK = "\033[7m‖\033[0m" if sys.stdout.isatty() else "‖"


def show(chunk):
    return chunk.decode("utf-8", errors="replace").replace("\r", "")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("path", help="текстовый файл в HDFS")
    ap.add_argument("--context", type=int, default=100, help="сколько байт показывать по обе стороны границы")
    args = ap.parse_args()

    stat = subprocess.run(["hdfs", "dfs", "-stat", "%o %b", args.path],
                          stdout=subprocess.PIPE, universal_newlines=True, check=True).stdout.split()
    blocksize, size = int(stat[0]), int(stat[1])
    n_blocks = (size + blocksize - 1) // blocksize
    print("%s: %d байт, blocksize = %d байт -> %d блок(ов)" % (args.path, size, blocksize, n_blocks))
    if n_blocks < 2:
        print("Файл помещается в один блок - границ нет. Загрузите его с меньшим dfs.blocksize.")
        return

    data = subprocess.run(["hdfs", "dfs", "-cat", args.path], stdout=subprocess.PIPE, check=True).stdout
    for i in range(1, n_blocks):
        off = i * blocksize
        before = data[max(0, off - args.context):off]
        after = data[off:off + args.context]
        print("\n" + "=" * 70)
        print("Граница блока %d | блока %d  (смещение %d байт)" % (i - 1, i, off))
        print("=" * 70)
        print(show(before) + MARK + show(after))
        if before[-1:] != b"\n":
            print("\n-> строка разрезана: её начало в блоке %d, конец - в блоке %d" % (i - 1, i))
        left = before.split()[-1:] if before and not before[-1:].isspace() else []
        right = after.split()[:1] if after and not after[:1].isspace() else []
        if left and right:
            print("\n-> слово разрезано: '%s' лежит в блоке %d, '%s' - в блоке %d"
                  % (show(left[0]), i - 1, show(right[0]), i))


if __name__ == "__main__":
    main()
