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
)

# Run tests
pass="true"
for test in ${tests[@]}
do
  echo "Running $test..."
  outfile=out-$test.tmp
  errfile=err-$test.tmp

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

if [ "$pass" == "true" ]; then
  echo "PASS"
else
  echo "FAIL"
fi
