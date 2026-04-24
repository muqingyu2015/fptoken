#Requires -Version 5.1
<#
.SYNOPSIS
    Run JUnit tests and generate a single-page HTML report from Console Launcher XML output.

.DESCRIPTION
    1) Runs lib/junit-platform-console-standalone-*.jar with --reports-dir -> build/test-results/junit-xml/
    2) Parses TEST-*.xml and writes build/test-results/junit-html/index.html

    Compile main sources to bin/ and tests to bin-test/ first (e.g. Eclipse or javac).

.PARAMETER ProjectRoot
    Repository root. Default: parent of this script directory.

.PARAMETER IncludePerf
    If set, do not exclude @Tag("performance") tests (default: exclude performance).

.PARAMETER JvmArgs
    Extra args for java, e.g. @('-Dfptoken.runPerfTests=true') when using -IncludePerf.

.EXAMPLE
    .\scripts\run-tests-html-report.ps1

.EXAMPLE
    .\scripts\run-tests-html-report.ps1 -IncludePerf -JvmArgs @('-Dfptoken.runPerfTests=true')
#>
[CmdletBinding()]
param(
    [string] $ProjectRoot,
    [switch] $IncludePerf,
    [string[]] $JvmArgs = @()
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($ProjectRoot)) {
    $scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
    $ProjectRoot = (Resolve-Path (Join-Path $scriptDir "..")).Path
}

function Find-ConsoleStandaloneJar([string] $libDir) {
    $jar = Get-ChildItem -Path $libDir -Filter "junit-platform-console-standalone-*.jar" -File |
        Sort-Object Name -Descending |
        Select-Object -First 1
    if (-not $jar) {
        throw "Missing lib/junit-platform-console-standalone-*.jar"
    }
    return $jar.FullName
}

function Escape-Html([string] $text) {
    if ($null -eq $text) { return [string]::Empty }
    $x = $text.Replace("&", "&amp;").Replace("<", "&lt;").Replace(">", "&gt;")
    return $x.Replace([string][char] 34, "&quot;")
}

function Convert-JUnitXmlDirectoryToHtml([string] $xmlDir, [string] $htmlFile) {
    $files = @(Get-ChildItem -Path $xmlDir -Filter "TEST-*.xml" -File)
    if ($files.Count -eq 0) {
        throw "No TEST-*.xml under: $xmlDir"
    }

    $totalTests = 0
    $totalFailures = 0
    $totalErrors = 0
    $totalSkipped = 0
    $totalTime = 0.0
    $sections = New-Object System.Collections.Generic.List[string]
    $suiteIndex = 0

    foreach ($f in $files) {
        [xml]$doc = Get-Content -LiteralPath $f.FullName -Raw -Encoding UTF8
        $ts = $doc.testsuite
        if (-not $ts) { continue }

        $suiteIndex++
        $name = [string]$ts.name
        $tests = [int]$ts.tests
        $failures = [int]$ts.failures
        $errors = [int]$ts.errors
        $skipped = [int]$ts.skipped
        $timeStr = [string]$ts.time
        $time = 0.0
        $timeNorm = if ($timeStr) { $timeStr -replace ",", "." } else { "0" }
        if (-not [double]::TryParse($timeNorm, [ref]$time)) { $time = 0.0 }

        $totalTests += $tests
        $totalFailures += $failures
        $totalErrors += $errors
        $totalSkipped += $skipped
        $totalTime += $time

        $sb = New-Object System.Text.StringBuilder
        [void]$sb.AppendLine("<h2 id=`"suite-$suiteIndex`">" + (Escape-Html $name) + "</h2>")
        [void]$sb.AppendLine("<p class=`"meta`">XML: " + (Escape-Html $f.Name) +
            " &mdash; tests=$tests failures=$failures errors=$errors skipped=$skipped time=${time}s</p>")
        [void]$sb.AppendLine("<table><thead><tr><th>Class</th><th>Method</th><th>Time (s)</th><th>Result</th></tr></thead><tbody>")

        foreach ($tc in $ts.SelectNodes("testcase")) {
            $cn = [string]$tc.classname
            $mn = [string]$tc.name
            $tRaw = [string]$tc.time
            $tVal = 0.0
            $tNorm2 = if ($tRaw) { $tRaw -replace ",", "." } else { "0" }
            if (-not [double]::TryParse($tNorm2, [ref]$tVal)) { $tVal = 0.0 }

            $status = "passed"
            $rowClass = "pass"
            $failNode = $tc.SelectSingleNode("failure")
            $errNode = $tc.SelectSingleNode("error")
            $skipNode = $tc.SelectSingleNode("skipped")
            if ($null -ne $failNode) {
                $status = "failure: " + [string]$failNode.message
                $rowClass = "fail"
            } elseif ($null -ne $errNode) {
                $status = "error: " + [string]$errNode.message
                $rowClass = "error"
            } elseif ($null -ne $skipNode) {
                $status = "skipped"
                $rowClass = "skip"
            }

            [void]$sb.AppendLine("<tr class=`"$rowClass`"><td>" + (Escape-Html $cn) + "</td><td>" + (Escape-Html $mn) +
                "</td><td>$tVal</td><td>" + (Escape-Html $status) + "</td></tr>")
        }
        [void]$sb.AppendLine("</tbody></table>")
        $sections.Add($sb.ToString())
    }

    $generated = (Get-Date).ToUniversalTime().ToString("yyyy-MM-dd HH:mm:ss UTC")
    $htmlDir = Split-Path -Parent $htmlFile
    if (-not (Test-Path $htmlDir)) {
        New-Item -ItemType Directory -Path $htmlDir -Force | Out-Null
    }

    $summaryRows = @"
<tr><td>Total tests</td><td>$totalTests</td></tr>
<tr><td>Failures</td><td class="fail">$totalFailures</td></tr>
<tr><td>Errors</td><td class="error">$totalErrors</td></tr>
<tr><td>Skipped</td><td class="skip">$totalSkipped</td></tr>
<tr><td>Total time (s)</td><td>$([math]::Round($totalTime, 3))</td></tr>
"@

    $body = ($sections -join "`n")
    $html = @"
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>JUnit report</title>
  <style>
    body { font-family: Segoe UI, system-ui, sans-serif; margin: 1.5rem; color: #1a1a1a; }
    h1 { font-size: 1.5rem; }
    h2 { font-size: 1.15rem; margin-top: 2rem; border-bottom: 1px solid #ccc; padding-bottom: 0.25rem; }
    table { border-collapse: collapse; width: 100%; margin: 0.75rem 0 2rem; font-size: 0.9rem; }
    th, td { border: 1px solid #ddd; padding: 0.35rem 0.5rem; text-align: left; vertical-align: top; }
    th { background: #f4f4f4; }
    tr.pass td:last-child { color: #0a6; }
    tr.fail td:last-child, tr.error td:last-child { color: #b00; font-weight: 600; }
    tr.skip td:last-child { color: #666; }
    .meta { color: #555; font-size: 0.85rem; }
    .summary { max-width: 28rem; }
  </style>
</head>
<body>
  <h1>JUnit test report</h1>
  <p class="meta">Generated: $generated &mdash; XML dir: $(Escape-Html $xmlDir)</p>
  <h2>Summary</h2>
  <table class="summary"><tbody>$summaryRows</tbody></table>
  $body
</body>
</html>
"@

    $utf8NoBom = New-Object System.Text.UTF8Encoding $false
    [System.IO.File]::WriteAllText($htmlFile, $html, $utf8NoBom)
}

$libDir = Join-Path $ProjectRoot "lib"
$binMain = Join-Path $ProjectRoot "bin"
$binTest = Join-Path $ProjectRoot "bin-test"
if (-not (Test-Path $binMain)) {
    throw "Missing bin/ directory. Compile main sources first."
}
if (-not (Test-Path $binTest)) {
    throw "Missing bin-test/ directory. Compile test sources first."
}

$jar = Find-ConsoleStandaloneJar $libDir
$xmlDir = Join-Path $ProjectRoot "build/test-results/junit-xml"
$htmlFile = Join-Path $ProjectRoot "build/test-results/junit-html/index.html"

if (Test-Path $xmlDir) {
    Remove-Item -LiteralPath $xmlDir -Recurse -Force
}
New-Item -ItemType Directory -Path $xmlDir -Force | Out-Null

$classPath = "$binMain;$binTest"
$junitArgs = @(
    "execute",
    "--class-path", $classPath,
    "--scan-classpath",
    "--reports-dir", $xmlDir,
    "--details", "tree"
)
if (-not $IncludePerf) {
    $junitArgs += @("-T", "performance")
}

Write-Host "JUnit: $jar"
Write-Host "XML reports: $xmlDir"
& java @JvmArgs -jar $jar @junitArgs
$exit = $LASTEXITCODE

Convert-JUnitXmlDirectoryToHtml $xmlDir $htmlFile
Write-Host ""
Write-Host "HTML report: $htmlFile"

if ($exit -ne 0) {
    exit $exit
}
