#!/bin/bash
xpoints="100 180 320 560 1000 1800 3200 5600 10000 18000 32000 56000 100000"

models[${#models[*]}]="nocc --run-name=RC --allow-cycles=yes"
models[${#models[*]}]="nocc --run-name=RCL --allow-cycles=yes --wr-blocks=yes"
models[${#models[*]}]="si --run-name=SI --allow-cycles=yes"
models[${#models[*]}]="2pl --run-name=2PL"
models[${#models[*]}]="2pl_ssn --run-name=SSN+RCL --wr-blocks=yes"
models[${#models[*]}]="pg_ssi2 --run-name=SSI"
models[${#models[*]}]="ssi4 --run-name=SSN+SI"
models[${#models[*]}]="ssi4 --run-name=SSN+RC --si-relax-reads=2 --si-relax-writes=yes"

# For reproducing the RC+SSN trace with a cycle (Figure 4d in the paper):
#models[${#models[*]}]="ssi4 --run-name=SSN+RC --si-relax-reads=2 --si-relax-writes=yes --replay-file=rc-cycle-trace --verbose=True"

globals="--duration=20000 --nclients=30"
#globals="--duration=2000" # for reproducing the RC+SSN cycle trace

./run.sh db-size $xpoints -- db-model "${models[@]}" -- $globals
