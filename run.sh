#!/bin/bash

tmp=$(mktemp)
trap "rm -f $tmp" EXIT

function runcmd() {
    echo "$@" >&2
    $@
}

echo '------------------------------------------------------------------------'
echo -n "$0"
for arg in "$@"; do
    echo -n " '$arg'"
done
echo
echo '------------------------------------------------------------------------'
echo

function usage() {
    echo "Usage: $0 <x-arg> <x1> [<x2> ...] -- <model-arg> <model1> [<model2> ...] [-- <early-param1> ... [-- <late-param1> ...]]"
    exit 1
}

if [ -z "$1" ]; then
    usage
else
    xarg="$1"
    shift
fi

while [ -n "$1" -a "$1" != "--" ]; do
    echo "x-point found: $1"
    xpoints[${#xpoints[*]}]="$1"
    shift
done

if [ "$1" == '--' ]; then
    echo "swallowing --"
    shift
else
    usage
fi

if [ -z "$1" ]; then
    usage
else
    marg="$1"
    shift
fi

while [ -n "$1" -a "$1" != "--" ]; do
    echo "Model found: $1"
    models[${#models[*]}]="$1"
    shift
done

if [ "$1" == '--' ]; then
    echo "swallowing --"
    shift
fi

while [ -n "$1" -a "$1" != "--" ]; do
    echo "Early param found: $1"
    eparms[${#eparms[*]}]="$1"
    shift
done


if [ "$1" == '--' ]; then
    echo "swallowing --"
    shift
fi

echo "Late params: $*"

for x in "${xpoints[@]}"; do
    # run the first model, slurping up the RNG seed
    echo
    echo '------------------------------------------------------------------------'
    seed_arg=
    for m in "${models[@]}"; do
        #echo "Model: $m"
        runcmd python3 main.py --dep-tracker=safe $seed_arg "${eparms[@]}" --$marg=$m --$xarg=$x "$@" |& tee $tmp
        echo
        
        seed=$(cat $tmp | grep '\(Simulator configuration\|seed\)' |
            grep -A1 'Simulator configuration' | grep seed | awk '{print $2}')
        echo found seed $seed
        if [ -n "$seed_arg" ]; then
            if [ "$seed_arg" != "--seed=$seed" ]; then
                echo "Warning: seed mismatch: $seed_arg vs --seed=$seed" >&2
            fi
        else
            seed_arg="--seed=$seed"
        fi
    done
done


    
