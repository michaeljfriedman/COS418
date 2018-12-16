#!/bin/bash
# test.sh
# Author: Michael Friedman

# All the tests you wish to run. Uncomment the ones you want to add.
tests=(
  # TestInitialElection
  # TestReElection
  # TestBasicAgree
  # TestFailAgree
  # TestFailNoAgree
  # TestConcurrentStarts
  # TestRejoin
  TestBackup
  # TestCount
  # TestPersist1
  # TestPersist2
  # TestPersist3
  # TestFigure8
  # TestUnreliableAgree
  # TestFigure8Unreliable
  # TestReliableChurn
  # TestUnreliableChurn
)

#----------------------------

# Check usage
usage="$(basename $0) NUM-TRIALS"
help_message='Runs tests for some number of trials. (Uncomment test names in
this script to run them.)'
if [ $# -ne 1 ]; then
  echo "$usage"
  echo "Missing argument"
  exit 1
elif [ "$1" == "-h" ] || [ "$1" == "--help" ]; then
  echo "$usage"
  echo
  echo "$help_message"
  exit
fi

trials=$1
ncores=$(getconf _NPROCESSORS_ONLN)

#----------------------------

# Runs test $1. Give the trial number as $2. If the test passes, reports
# nothing and deletes the stdout/stderr. If it fails, reports the failure and
# saves the stdout and stderr to files.
run_test() {
  test=$1
  trial=$2

  outfile=out-$test-$trial.txt
  errfile=err-$test-$trial.txt

  # Run test
  go test -run $test > $outfile 2> $errfile
  test_status=$?

  # Check for failures and warnings
  if [ $test_status -ne 0 ] || grep "FAIL" $outfile > /dev/null 2> /dev/null || grep "FAIL" $errfile > /dev/null 2> /dev/null; then
    echo "[!] Failed $test on trial $trial. Stdout in $outfile, stderr in $errfile"
  elif grep "warning" $outfile > /dev/null 2> /dev/null || grep "warning" $errfile > /dev/null 2> /dev/null; then
    echo "[!] Warning from $test on trial $trial. Stdout in $outfile, stderr in $errfile"
  else
    rm -f $outfile $errfile
  fi
}

# Runs a partition of the trials. Give the test name as $1, and the range of
# trials to run as $2 and $3 (start and end indices, inclusive on both ends).
run_partition() {
  test=$1
  start=$2
  end=$3

  for trial in $(seq $start $end); do
    run_test $test $trial
  done
}

# Run trials of each test, utilizing all cores to run concurrently
tests_str=""
for test in ${tests[@]}; do
  tests_str="$tests_str $test"
done
echo "Tests to run:$tests_str"
echo "$trials trials of each test across $ncores cores"

for test in ${tests[@]}; do
  echo "Running $test..."

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
    run_partition $test $start $end &
    worker_pids="$worker_pids $!"

    trial=$(($end+1))
  done

  # Kill all workers if this script is killed
  trap "kill $worker_pids; exit" INT TERM

  # Wait for all workers to finish
  for pid in $worker_pids; do
    wait $pid
  done
done

# Check if all passed by by checking if out/err files were left behind
if ! ls out-*.txt err-*.txt > /dev/null 2> /dev/null; then
  echo "PASS"
fi
