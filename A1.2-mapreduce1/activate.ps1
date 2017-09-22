# activate.ps1
# Author: Michael Friedman
#
# Sets up Go environment for working on this assignment.

if (($args.Length -eq 1) -and ($args[0] == "--help")) {
  Write-Output "Usage: .\activate.ps1"
  exit
}

$env:GOPATH = (Get-Location)
Write-Output "GOPATH = $env:GOPATH"
