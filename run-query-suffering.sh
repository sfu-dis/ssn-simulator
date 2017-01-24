#!/bin/bash

xpoints="0 1 2 5 10 20 30 40 50"

models[${#models[*]}]="2pl --run-name=2PL"
models[${#models[*]}]="2pl_ssn --run-name=SSN+RCL --wr-blocks=yes"
models[${#models[*]}]="pg_ssi2 --run-name=SSI"
models[${#models[*]}]="ssi4 --si-relax-reads=2 --si-relax-writes=yes --run-name=SSN+RC"

globals="--db-size=3000 --duration=500000 --dscale=.5 --doffset=10 --tx-size=0 --tx-writes=0 --think-time=1000 --tx-name=Nop --nclients=1 --tx-size=8,12 --tx-writes=1,4 --think-time=150,200 --tx-name=Writer --nclients=10 --tx-timeout=1500 --tx-size=100,200 --tx-writes=0 --think-time=1000,1500 --tx-name=Reader"

./run.sh nclients $xpoints -- db-model "${models[@]}" -- $globals
