#!/bin/bash
# test-many.sh
# Author: Michael Friedman
#
# Runs tests for Raft leader election.

if [ $# -ne 1 ] || [ "$1" == "--help" ]; then
  echo "Usage: ./test-many.sh NUM-TRIALS"
  exit
fi

T=$1

#----------------------------

pass="true"
for t in $(seq $T)
do
  echo "Running trial $t..."
  outfile=out$t.tmp
  errfile=err$t.tmp

  # Run test
  go test -run Election > $outfile 2> $errfile

  # Check for failures and warnings
  cat $outfile | grep "FAIL" > fail-out.tmp
  cat $errfile | grep "FAIL" > fail-err.tmp
  cat $outfile | grep "warning" > warning-out.tmp
  cat $errfile | grep "warning" > warning-err.tmp
  if [ -s fail-out.tmp ] || [ -s fail-err.tmp ]; then
    pass="false"
    echo "[!] Fail on trial $t. Stdout in $outfile, stderr in $errfile"
  elif [ -s warning-out.tmp ] || [ -s warning-err.tmp ]; then
    echo "[!] Warning on trial $t. Stdout in $outfile, stderr in $errfile"
  else
    rm -f $outfile $errfile
  fi

  # Clean up
  rm -rf fail-out.tmp fail-err.tmp warning-out.tmp warning-err.tmp
done

if [ "$pass" == "true" ]; then
  echo "PASS"
fi
