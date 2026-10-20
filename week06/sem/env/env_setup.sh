#!/bin/sh
# Ставит окружение для демо 6-й недели и запускает jupyter.
# Можно запускать откуда угодно: sh week06/sem/env/env_setup.sh
set -e
cd "$(dirname "$0")"

python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
python -m ipykernel install --user --name=sem6_kernel --display-name "Python (sem6)"

echo
echo "Окружение готово: $(pwd)/.venv"
echo "Jupyter сейчас займёт этот терминал. В других терминалах (consumer, producer) сначала:"
echo "  source $(pwd)/.venv/bin/activate"
echo

cd ..
jupyter notebook
