#Requires -Version 5.1
<#
.SYNOPSIS
    Thin wrapper: runs tests and writes HTML report (delegates to run-fptoken-tests.ps1).

.DESCRIPTION
    Equivalent to:
      .\scripts\run-fptoken-tests.ps1 -HtmlReport -ExcludePerfTag [-SkipCompile] [-ExtraJvmArgs ...]
    With -IncludePerf, performance-tagged tests are not excluded and -Perf JVM flag is set.

.PARAMETER IncludePerf
    Do not exclude @Tag("performance"); set -Dfptoken.runPerfTests=true for enabled perf tests.

.PARAMETER SkipCompile
    Forwarded: use existing bin/ and bin-test/ without recompiling.

.PARAMETER JvmArgs
    Extra JVM arguments (forwarded as -ExtraJvmArgs).

.EXAMPLE
    .\scripts\run-tests-html-report.ps1

.EXAMPLE
    .\scripts\run-tests-html-report.ps1 -IncludePerf -JvmArgs @('-Dfptoken.runPerfTests=true')
#>
[CmdletBinding()]
param(
    [switch] $IncludePerf,
    [switch] $SkipCompile,
    [string[]] $JvmArgs = @()
)

$ErrorActionPreference = "Stop"
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path

$delegate = @{
    HtmlReport       = $true
    ExcludePerfTag   = (-not $IncludePerf)
    SkipCompile      = $SkipCompile
}
if ($IncludePerf) {
    $delegate["Perf"] = $true
}
if ($JvmArgs -and $JvmArgs.Count -gt 0) {
    $delegate["ExtraJvmArgs"] = $JvmArgs
}

& (Join-Path $scriptDir "run-fptoken-tests.ps1") @delegate
