#!/bin/bash
# test-regression.sh
# Author: Michael Friedman
#
# Runs all tests I have been passing so far. Run this to catch which tests
# fail after making future changes.

# All the known passing tests. Update this when you pass more tests.
tests=(
  TestInitialElection
  TestReElection
  TestBasicAgree
  TestFailAgree
  TestFailNoAgree
  TestConcurrentStarts
  TestRejoin
  TestBackup
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
    outfile=out-$test-$trial.tmp
    errfile=err-$test-$trial.tmp

    # Run test
    go test -run $test > $outfile 2> $errfile

    # Check for failures and warnings
    cat $outfile | grep "FAIL" > fail-out.tmp
    cat $errfile | grep "FAIL" > fail-err.tmp
    cat $outfile | grep "warning" > warning-out.tmp
    cat $errfile | grep "warning" > warning-err.tmp
    if [ -s fail-out.tmp ] || [ -s fail-err.tmp ]; then
      pass="false"
      echo "[!] Failed $test. Stdout in $outfile, stderr in $errfile"
    elif [ -s warning-out.tmp ] || [ -s warning-err.tmp ]; then
      echo "[!] Warning from $test. Stdout in $outfile, stderr in $errfile"
    else
      rm -f $outfile $errfile
    fi

    # Clean up
    rm -rf fail-out.tmp fail-err.tmp warning-out.tmp warning-err.tmp
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
