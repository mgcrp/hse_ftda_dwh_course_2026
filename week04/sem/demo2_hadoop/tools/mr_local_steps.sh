#!/usr/bin/env bash
# mr_local_steps.sh - MapReduce "на пальцах" обычным пайпом Unix, по шагам.
#
#   Map  ->  Shuffle & Sort  ->  Reduce
#   mapper.py | sort | reducer.py
#
# Запуск (из папки demo2_hadoop):
#   bash tools/mr_local_steps.sh
#   bash tools/mr_local_steps.sh "свой текст для подсчёта"

set -euo pipefail
cd "$(dirname "$0")/../map_reduce_example"

TEXT="${1:-to be or not to be
that is the question
to sleep perchance to dream}"

step() { printf '\n\033[1;36m=== %s ===\033[0m\n' "$1"; }

step "0. Вход (каждая строка - запись для mapper-а)"
echo "$TEXT"

step "1. MAP: mapper.py превращает каждое слово в пару (word, 1)"
echo "$TEXT" | python3 mapper.py

step "2. SHUFFLE & SORT: Hadoop сортирует пары по ключу - одинаковые слова оказываются рядом"
echo "$TEXT" | python3 mapper.py | sort -k1,1

step "3. REDUCE: reducer.py идёт по отсортированному потоку и суммирует подряд идущие ключи"
echo "$TEXT" | python3 mapper.py | sort -k1,1 | python3 reducer.py

step "Что будет БЕЗ shuffle/sort? reducer видит 'to' несколько раз вразнобой и считает неправильно"
echo "$TEXT" | python3 mapper.py | python3 reducer.py

step "PARTITION: при 2 reducer-ах каждый ключ уходит ровно в один из них (hash(key) mod 2)"
echo "$TEXT" | python3 mapper.py | python3 -c '
import sys, zlib
for line in sys.stdin:
    word = line.split("\t", 1)[0]
    print("reducer-%d\t%s" % (zlib.crc32(word.encode()) % 2, line.rstrip()))
' | sort -k1,1 -k2,2

step "COMBINER: reducer.py можно запустить прямо на выходе mapper-а, чтобы гнать по сети меньше пар"
echo "до combiner-а:    $(echo "$TEXT" | python3 mapper.py | wc -l | tr -d ' ') пар"
echo "после combiner-а: $(echo "$TEXT" | python3 mapper.py | sort -k1,1 | python3 reducer.py | wc -l | tr -d ' ') пар"

step "То же самое на настоящей книге (pg4300.txt): топ-10 слов"
time (python3 mapper.py < pg4300.txt | sort -k1,1 | python3 reducer.py | sort -k2,2nr | head -10)
