#!/usr/bin/env python3
"""counters.py - вытаскивает самые наглядные счётчики из лога `hadoop jar ...`
и ставит несколько запусков рядом.

    hadoop jar ... 2>&1 | tee wc.log
    hadoop jar ... -combiner ... 2>&1 | tee wc_combiner.log
    python3 counters.py wc.log wc_combiner.log
"""

import os
import re
import sys

COUNTERS = [
    ("Launched map tasks", "сколько mapper-ов (≈ сплитов/блоков)"),
    ("Data-local map tasks", "mapper запущен там же, где лежит блок"),
    ("Rack-local map tasks", "mapper в той же стойке"),
    ("Launched reduce tasks", "сколько reducer-ов (= файлов part-*)"),
    ("Map input records", "строк прочитано"),
    ("Map output records", "пар (word, 1) от mapper-ов"),
    ("Combine input records", "пар вошло в combiner"),
    ("Combine output records", "пар вышло из combiner-а"),
    ("Reduce shuffle bytes", "байт передано по сети в shuffle"),
    ("Reduce input records", "пар дошло до reducer-ов"),
    ("Reduce input groups", "уникальных ключей (слов)"),
    ("Reduce output records", "строк в результате"),
]
LINE_RE = re.compile(r"^\s+(.+?)=(\d+)\s*$")


def load(path):
    values = {}
    with open(path, errors="replace") as f:
        for line in f:
            m = LINE_RE.match(line)
            if m:
                values[m.group(1)] = int(m.group(2))
    return values


def main():
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    runs = [(os.path.basename(p), load(p)) for p in sys.argv[1:]]
    width = max(12, max(len(name) for name, _ in runs))
    print("%-24s " % "counter" + " ".join("%*s" % (width, name) for name, _ in runs) + "   что это")
    print("-" * (26 + (width + 1) * len(runs) + 40))
    for counter, hint in COUNTERS:
        if not any(counter in vals for _, vals in runs):
            continue
        cells = " ".join("%*s" % (width, "{:,}".format(vals[counter]).replace(",", " ") if counter in vals else "-")
                         for _, vals in runs)
        print("%-24s %s   %s" % (counter, cells, hint))


if __name__ == "__main__":
    main()
