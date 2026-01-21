#!/usr/bin/env bash
SCRIPT_DIR=$(dirname "${BASH_SOURCE[0]}")
cd "$SCRIPT_DIR" || exit 1

if [ -f ./.env ]; then
    source ./.env
fi

ops=${1:-8000}

ldir=$(realpath ~/logs)
mdir=$(realpath ~/metrics)

if [ ! -d ./.venv ]; then
	echo "Virtual environment not found. Creating..."
	python3.13 -m venv ./.venv
else
	echo "Using existing virtual environment."
fi
source ./.venv/bin/activate
pip install --upgrade pip
if [ -f ./requirements.txt ]; then
	pip install -r ./requirements.txt
else
	echo "requirements.txt not found. Skipping package installation."
fi

for w in 1 2 4 8 16; do
	for b in 1 10 50 100 500; do
		for p in "bigint" "uuidv4" "uuidv7"; do
            echo "Running with pktype=$p, workers=$w, batchsize=$b, operations=$ops"
            ./.venv/bin/python ./src/main.py --logdir $ldir --metricsdir $mdir --operations $ops --workers $w --batchsize $b --pktype $p
		done
	done
done

deactivate