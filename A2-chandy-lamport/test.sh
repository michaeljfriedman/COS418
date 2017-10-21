#!/bin/bash
# test.sh
# Author: Michael Friedman
#
# Runs tests for this assignment many times to check that they always pass.
# Reports any failures.

if [ $# -ne 1 ]; then
  echo "Usage: ./test.sh NUM-TRIALS"
  exit
fi
T=$1

pass="true"
for t in $(seq $T)
do
  echo "Running trial $t..."
  outfile=out$t.tmp
  errfile=err$t.tmp

  # Run tests
  go test > $outfile 2> $errfile
  cat $outfile | grep "FAIL" > fail-out.tmp
  cat $errfile | grep "FAIL" > fail-err.tmp

  # Check for failures
  if [ -s fail-out.tmp ] || [ -s fail-err.tmp ]; then
    pass="false"
    echo "[!] Fail on trial $t. Stdout in $outfile, stderr in $errfile"
  else
    rm -f $outfile $errfile
  fi

  # Clean up
  rm -f fail-out.tmp fail-err.tmp
done

if [ "$pass" == "true" ]; then
  echo "PASS"
fi
