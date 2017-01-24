#!/bin/bash

xpoints="10 20 40 80 160 320 640 1280 2560 5120 10480"

models[${#models[*]}]="8,12 --tx-writes=1,3 --db-size=1000 --think-time=100,200 --tx-timeout=20000 --run-name=SSN+RC/S --db-model=ssi4 --si-relax-reads=2 --si-relax-writes=yes "
models[${#models[*]}]="32,48 --tx-writes=4,12 --db-size=4000 --think-time=400,800 --tx-timeout=80000 --run-name=SSN+RC/L --db-model=ssi4 --si-relax-reads=2 --si-relax-writes=yes"

models[${#models[*]}]="8,12 --tx-writes=1,3 --db-size=1000 --think-time=100,200 --tx-timeout=20000 --run-name=SSI/S --db-model=pg_ssi2"
models[${#models[*]}]="32,48 --tx-writes=4,12 --db-size=4000 --think-time=400,800 --tx-timeout=80000 --run-name=SSI/L --db-model=pg_ssi2"

globals="--tx-name=Writer --nclients=30 --tx-size=1 --tx-writes=0 --think-time=8,12 --tx-name=Reader --tx-timeout=160000 --nclients=1 --duration=30000"

./run.sh safe-snaps $xpoints -- tx-size "${models[@]}" -- $globals
