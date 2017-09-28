# test-wc.ps1
# Author: Michael Friedman
#
# Translation of test-wc.sh into PS.

go run wc.go master sequential pg-*.txt
sort -n -k2 mrtmp.wcseq | tail -10 | diff - mr-testout.txt > diff.out
if ((Test-Path diff.out) -and ((Get-Item diff.out).length -gt 0)) {
  Write-Output "Failed test. Output should be as in mr-testout.txt. Your output differs as follows (from diff.out):"
  Get-Content diff.out
} else {
  Write-Output "Passed test"
}
