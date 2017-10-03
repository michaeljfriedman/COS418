# test.ps1
# Author: Michael Friedman
#
# Runs all tests for the distributed part of the assignment.

echo "----- Testing basic (w/o failures) -----"
go test -v -run TestBasic
echo

echo "----- Testing with failures -----"
go test -v -run Failure
echo
