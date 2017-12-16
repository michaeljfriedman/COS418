#!/bin/bash
# filter-log.sh
# Author: Michael Friedman
#
# Filter a log by stream. Streams are defined in raft.go at the top. Prints
# to stdout.

if [ $# -ne 2 ] || [ "$1" == "--help" ]; then
  echo "Usage: ./filter-log FILENAME STREAM"
  exit
fi

filename=$1
stream=$2

#-------------------------------------------------------------------------------

grep --color "\[$stream\]" $filename
