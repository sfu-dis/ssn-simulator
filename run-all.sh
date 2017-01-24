#!/bin/bash
echo "db-size"
./run-db-size.sh &> $1/db-size.txt

echo "rw-ratio"
./run-rw-ratio.sh &> $1/rw-ratio.txt

echo "query-suffering"
./run-query-suffering.sh &> $1/query-suffering.txt

echo "snapkill"
./run-snapkill.sh &> $1/snapkill.txt
