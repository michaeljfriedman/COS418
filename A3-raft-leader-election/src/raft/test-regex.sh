#!/bin/bash
# test-v2.sh
# Author: Michael Friedman

# Check usage
usage="$(basename $0) NUM-TRIALS [TEST-REGEX]"
help_message='Runs a test for some number of trials. The TEST-REGEX is interpreted
the same as the -run argument to "go test". If none is provided, runs
all the tests as a plain "go test".'
if [ "$1" == "-h" ] || [ "$1" == "--help" ]; then
  echo "$usage"
  echo
  echo "$help_message"
  exit
elif [ $# -lt 1 ]; then
  echo "$usage"
  echo "Missing argument"
  exit 1
fi

trials=$1
test_regex=$2
ncores=$(getconf _NPROCESSORS_ONLN)

#----------------------------

# Runs test $1. Give the trial number as $2. If the test passes, reports
# nothing and deletes the stdout/stderr. If it fails, reports the failure and
# saves the stdout and stderr to files.
run_test() {
  test_regex=$1
  trial=$2

  outfile=out-$trial.txt
  errfile=err-$trial.txt

  # Run test
  if [ -z "$test_regex" ]; then
    go test > $outfile 2> $errfile
  else
    go test -run $test_regex > $outfile 2> $errfile
  fi
  test_status=$?

  # Check for failures and warnings
  if [ $test_status -ne 0 ] || grep "FAIL" $outfile > /dev/null 2> /dev/null || grep "FAIL" $errfile > /dev/null 2> /dev/null; then
    echo "[!] Failed on trial $trial. Stdout in $outfile, stderr in $errfile"
  elif grep "warning" $outfile > /dev/null 2> /dev/null || grep "warning" $errfile > /dev/null 2> /dev/null; then
    echo "[!] Warning on trial $trial. Stdout in $outfile, stderr in $errfile"
  else
    rm -f $outfile $errfile
  fi
}

# Runs a partition of the trials. Give the test name as $1, and the range of
# trials to run as $2 and $3 (start and end indices, inclusive on both ends).
run_partition() {
  test_regex=$1
  start=$2
  end=$3

  for trial in $(seq $start $end); do
    run_test $test_regex $trial
  done
}

# Run trials of the test command, utilizing all cores to run concurrently
if [ -z "$test_regex" ]; then
  test_str="all"
else
  test_str="$test_regex"
fi
echo "Tests to run: $test_str"
echo "Running $trials trials across $ncores cores..."

trials_per_core=$(($trials / $ncores))
extra_trials=$(($trials % $ncores))

# Special case - if running fewer trials than cores, don't need extra cores
if [ $trials -lt $ncores ]; then
  ncores=$trials
fi

trial=1
worker_pids=""
for i in $(seq $ncores); do
  start=$trial
  end=$(($trial+$trials_per_core-1))

  # Allocate extra trials if applicable
  if [ $extra_trials -gt 0 ]; then
    end=$(($end+1))
    extra_trials=$(($extra_trials-1))
  fi

  # Start worker
  run_partition $test_regex $start $end &
  worker_pids="$worker_pids $!"

  trial=$(($end+1))
done

# Kill all workers if this script is killed
trap "kill $worker_pids; exit" INT TERM

# Wait for all workers to finish
for pid in $worker_pids; do
  wait $pid
done

# Check if all passed by by checking if out/err files were left behind
if ! ls out-*.txt err-*.txt > /dev/null 2> /dev/null; then
  echo "PASS"
fi
