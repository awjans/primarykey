#!/bin/bash

ldir=$(realpath ~/logs)
mdir=$(realpath ~/metrics)

for w in 1 2 4 8 16; do
	for b in 1 2 10 25 50 100 250 500 1000; do
		for p in "bigint" "uuidv4" "uuidv7"; do
            echo "Running with workers=$w, batchsize=$b, pktype=$p"
            /home/andrea/src/primarykey/.venv/bin/python /home/andrea/src/primarykey/src/main.py --logdir $ldir --metricsdir $mdir --operations $1 --workers $w --batchsize $b --pktype $p
		done
	done
done