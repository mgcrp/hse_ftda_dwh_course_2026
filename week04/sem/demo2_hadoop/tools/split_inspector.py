#!/usr/bin/env python3
"""split_inspector.py - маппер-"шпион" для map-only job (mapreduce.job.reduces=0).

Ничего не считает, а в конце печатает ОДНУ строку про свой input split:
    task | host | file | start | length | lines | first line | last line

Так видно, что:
  * map task-ов столько же, сколько сплитов (≈ блоков);
  * сплит начинается ровно на границе блока (start = k * blocksize);
  * но первая строка сплита - это первая ЦЕЛАЯ строка после границы,
    а последняя строка "дочитывается" из следующего блока (LineRecordReader);
  * task обычно запускается на той же ноде, где лежит блок (data locality).
"""

import os
import socket
import sys


def env(*names):
    for name in names:
        if name in os.environ:
            return os.environ[name]
    return "?"


def preview(line, width=35):
    text = line.decode("utf-8", errors="replace").strip().replace("\t", " ")
    return text if len(text) <= width else text[:width - 1] + "…"


lines = 0
first = last = b""
for line in sys.stdin.buffer:
    if lines == 0:
        first = line
    last = line
    lines += 1

attempt = env("mapreduce_task_attempt_id", "mapred_task_id")
task = "_".join(attempt.split("_")[-3:-1]) if attempt != "?" else "?"   # m_000003
path = env("mapreduce_map_input_file", "map_input_file")

print("\t".join([
    task,
    socket.gethostname().split(".")[0],
    path.rsplit("/", 1)[-1],
    "start=" + env("mapreduce_map_input_start", "map_input_start"),
    "length=" + env("mapreduce_map_input_length", "map_input_length"),
    "lines=%d" % lines,
    "first=[%s]" % preview(first),
    "last=[%s]" % preview(last),
]))
