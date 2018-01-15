#!/bin/bash
# test.sh
# Author: Michael Friedman
#
# Runs a given test, printing stdout to terminal, and if it fails, stdout is
# saved to a file [TEST-NAME].out and stderr to [TEST-NAME].err.

# Check usage
if [ $# -ne 1 ]; then
  echo "Usage: ./test.sh TEST-NAME"
  exit 1
fi

test_name=$1

#-------------------------------------------------------------------------------

outfile=$test_name.out
errfile=$test_name.err

# Run test
go test -run $test_name 2> $errfile | tee $outfile

# Check if it passed, and remove the stdout/stderr files if it did.
failout=fail.out
failerr=fail.err
warningout=warning.out
warningerr=warning.err
cat $outfile | grep "FAIL" > $failout
cat $errfile | grep "FAIL" > $failerr
cat $outfile | grep "warning" > $warningout
cat $errfile | grep "warning" > $warningerr
if [ ! -s $failout ] && [ ! -s $failerr ] && [ ! -s $warningout ] && [ ! -s $warningerr ]; then
  rm -f $outfile $errfile
fi

# Clean up
rm -f $failout $failerr $warningout $warningerr
