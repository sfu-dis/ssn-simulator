#!/bin/bash

xpoints="0 1 2 5 10 15 20 30 40 50 60 70 80 85 90 95 98 99 100"

models[${#models[*]}]="2pl --run-name=2PL"
models[${#models[*]}]="2pl_ssn --run-name=SSN+RCL --wr-blocks=yes"
models[${#models[*]}]="si --allow-cycles=yes --run-name=SI"
models[${#models[*]}]="pg_ssi2 --run-name=SSI"
models[${#models[*]}]="ssi4 --run-name=SSN+SI"
models[${#models[*]}]="ssi4 --si-relax-reads=2 --si-relax-writes=yes --run-name=SSN+RC"

globals="--db-size=100000 --tx-size=100 --think-time=900,1200 --nclients=30 --tx-timeout=160000 --duration=50000"

./run.sh tx-writes $xpoints -- db-model "${models[@]}" -- -- $globals
