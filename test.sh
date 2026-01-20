#!/usr/bin/env bash
SCRIPT_DIR=$(dirname "${BASH_SOURCE[0]}")
cd "$SCRIPT_DIR" || exit 1
pwd

ops=${1:-1000}

ldir=$(realpath ~/logs)
mdir=$(realpath ~/metrics)

source ./.venv/bin/activate

for w in 1 2 4 8 16; do
	for b in 1 2 10 25 50 100 250 500 1000; do
		for p in "bigint" "uuidv4" "uuidv7"; do
            echo "Running with workers=$w, batchsize=$b, pktype=$p"
            ./.venv/bin/python ./src/main.py --logdir $ldir --metricsdir $mdir --operations $ops --workers $w --batchsize $b --pktype $p
		done
	done
done

deactivate