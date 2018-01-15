#!/bin/bash
# test-regression.sh
# Author: Michael Friedman
#
# Runs all tests I have been passing so far. Run this to catch which tests
# fail after making future changes.

# All the known passing tests. Update this when you pass more tests.
tests=(
  TestBasic
  TestConcurrent
  TestUnreliable
  TestUnreliableOneKey
  TestOnePartition
  TestManyPartitionsOneClient
  TestManyPartitionsManyClients
)

#----------------------------

# Check usage
if [ $# -ne 1 ] || [ "$1" == "--help" ]; then
  echo "Usage: ./test-regression.sh NUM-TRIALS"
  exit
fi

trials=$1

#----------------------------

# Run trials
pass="true"
for trial in $(seq $trials); do

  echo "TRIAL $trial"

  # Run each test
  for test in ${tests[@]}; do
    echo "Running $test..."
    outfile=$test-$trial.out
    errfile=$test-$trial.err

    # Run test
    go test -run $test > $outfile 2> $errfile

    # Check for failures and warnings
    failout=fail.out
    failerr=fail.err
    warningout=warning.out
    warningerr=warning.err
    cat $outfile | grep "FAIL" > $failout
    cat $errfile | grep "FAIL" > $failerr
    cat $outfile | grep "warning" > $warningout
    cat $errfile | grep "warning" > $warningerr
    if [ -s $failout ] || [ -s $failerr ]; then
      pass="false"
      echo "[!] Failed $test. Stdout in $outfile, stderr in $errfile"
    elif [ -s $warningout ] || [ -s $warningerr ]; then
      echo "[!] Warning from $test. Stdout in $outfile, stderr in $errfile"
    else
      rm -f $outfile $errfile
    fi

    # Clean up
    rm -rf $failout $failerr $warningout $warningerr
  done

  echo

  if [ "$pass" == "false" ]; then
    echo "FAIL during trial $trial"
    break
  fi

done

if [ "$pass" == "true" ]; then
  echo "PASS"
fi
