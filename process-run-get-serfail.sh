#!/bin/bash

function usage() {
    echo "Usage: $0 <input-file> <x-arg> <watch> <run1> [<run2> ...]"
    exit 1
}

if [ -f "$1" ]; then
    ifile="$1"
    shift
else
    usage
fi

if [ -n "$1" ]; then
    xarg="$1"
    shift
else
    usage
fi

if [ -n "$1" ]; then
    watch="$1"
    shift
else
    usage
fi

echo "x-axis: $xarg"
cat "$ifile" |
    grep "\(Simulator configuration\|\<$xarg\>\)" |
    grep -A1 "Simulator configuration" |
    grep "\<$xarg\>" |
    awk '{print $2}' |
    sort -nu |
    tee /dev/clipboard
read

echo "Total:"
cat "$ifile" |
    grep "\(run_name\s*$1\>\|\: Total\>\)" |
    grep -A1 "run_name\s*$1" |
    grep ": Total\>" |
    awk '{print $1}' |
    tr -d ':' |
    tee /dev/clipboard
read

for m in "$@"; do
    echo "Run $m:"
    cat "$ifile" |
        grep "\(run_name\s*$m\>\|\Total transactions\>\)" |
        grep -A1 "run_name\s*$m" |
        grep "Total" |
        awk '{print $6}' |
        tee /dev/clipboard
    read
done
