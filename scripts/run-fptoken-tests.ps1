#Requires -Version 5.1
<#
.SYNOPSIS
    Compile main + test sources and run JUnit 5 for package cn.lxdb.plugins.muqingyu.fptoken.tests.

.DESCRIPTION
    Single entry point for local and CI-style runs. Optional HTML report under build/test-results/.

.PARAMETER HtmlReport
    After tests, write JUnit XML to build/test-results/junit-xml/ and a summary HTML page to
    build/test-results/junit-html/index.html .

.PARAMETER ExcludePerfTag
    Pass -T performance to the Console Launcher (exclude tests tagged performance). Recommended with -HtmlReport for faster reports.

.PARAMETER SkipCompile
    Skip clean + javac; requires existing bin/ and bin-test/ (e.g. already built in IDE).

.PARAMETER Perf / Scale / Budget / Stress / Soak
    Forwarded as JVM -Dfptoken.run* system properties.

.PARAMETER ExtraJvmArgs
    Additional JVM arguments (e.g. custom -D properties).

.EXAMPLE
    .\scripts\run-fptoken-tests.ps1

.EXAMPLE
    .\scripts\run-fptoken-tests.ps1 -Perf

.EXAMPLE
    .\scripts\run-fptoken-tests.ps1 -HtmlReport -ExcludePerfTag

.EXAMPLE
    .\scripts\run-fptoken-tests.ps1 -HtmlReport -ExcludePerfTag -SkipCompile
#>
param(
    [switch] $Perf,
    [switch] $Scale,
    [switch] $Budget,
    [switch] $Stress,
    [switch] $Soak,
    [switch] $HtmlReport,
    [switch] $ExcludePerfTag,
    [switch] $SkipCompile,
    [string[]] $ExtraJvmArgs = @()
)

# 中文速查：-Perf / -Scale / -Budget / -Stress / -Soak → 对应 -Dfptoken.run*；
#   -HtmlReport → 写 build/test-results 下 JUnit XML 与汇总 HTML；
#   -ExcludePerfTag → 排除 @Tag("performance")；-SkipCompile → 不清理、不 javac。

$ErrorActionPreference = "Stop"
$root = Split-Path $PSScriptRoot -Parent

$lib = Join-Path $root "lib"
New-Item -ItemType Directory -Force -Path $lib | Out-Null

function Ensure-MavenJar([string] $fileName, [string] $url) {
    $p = Join-Path $lib $fileName
    if (-not (Test-Path $p)) {
        Write-Host "Downloading $fileName ..."
        Invoke-WebRequest -Uri $url -OutFile $p
    }
}

$junitJar = Join-Path $lib "junit-platform-console-standalone-1.10.2.jar"
Ensure-MavenJar "junit-platform-console-standalone-1.10.2.jar" `
    "https://repo1.maven.org/maven2/org/junit/platform/junit-platform-console-standalone/1.10.2/junit-platform-console-standalone-1.10.2.jar"

Ensure-MavenJar "junit-jupiter-api-5.10.2.jar" `
    "https://repo1.maven.org/maven2/org/junit/jupiter/junit-jupiter-api/5.10.2/junit-jupiter-api-5.10.2.jar"
Ensure-MavenJar "junit-jupiter-engine-5.10.2.jar" `
    "https://repo1.maven.org/maven2/org/junit/jupiter/junit-jupiter-engine/5.10.2/junit-jupiter-engine-5.10.2.jar"
Ensure-MavenJar "junit-platform-commons-1.10.2.jar" `
    "https://repo1.maven.org/maven2/org/junit/platform/junit-platform-commons/1.10.2/junit-platform-commons-1.10.2.jar"
Ensure-MavenJar "junit-platform-engine-1.10.2.jar" `
    "https://repo1.maven.org/maven2/org/junit/platform/junit-platform-engine/1.10.2/junit-platform-engine-1.10.2.jar"
Ensure-MavenJar "junit-platform-launcher-1.10.2.jar" `
    "https://repo1.maven.org/maven2/org/junit/platform/junit-platform-launcher/1.10.2/junit-platform-launcher-1.10.2.jar"
Ensure-MavenJar "opentest4j-1.3.0.jar" `
    "https://repo1.maven.org/maven2/org/opentest4j/opentest4j/1.3.0/opentest4j-1.3.0.jar"
Ensure-MavenJar "apiguardian-api-1.1.2.jar" `
    "https://repo1.maven.org/maven2/org/apiguardian/apiguardian-api/1.1.2/apiguardian-api-1.1.2.jar"

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

$bin = Join-Path $root "bin"
$binTest = Join-Path $root "bin-test"

if (-not $SkipCompile) {
    New-Item -ItemType Directory -Force -Path $bin | Out-Null
    New-Item -ItemType Directory -Force -Path $binTest | Out-Null
    Remove-Item -Path (Join-Path $bin "*") -Recurse -Force -ErrorAction SilentlyContinue
    Remove-Item -Path (Join-Path $binTest "*") -Recurse -Force -ErrorAction SilentlyContinue

    $mainFiles = @(Get-ChildItem -Path (Join-Path $root "src\cn") -Recurse -Filter "*.java" | ForEach-Object { $_.FullName })
    $testFiles = @(Get-ChildItem -Path (Join-Path $root "src\test\java") -Recurse -Filter "*.java" | ForEach-Object { $_.FullName })

    if ($mainFiles.Count -eq 0) {
        throw "No main sources under src\cn"
    }
    if ($testFiles.Count -eq 0) {
        throw "No test sources under src\test\java"
    }

    Write-Host "Compiling main sources -> $bin"
    & javac -encoding UTF-8 -d $bin -sourcepath (Join-Path $root "src") @mainFiles
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

    Write-Host "Compiling test sources -> $binTest"
    $compileCp = "$bin" + [IO.Path]::PathSeparator + $junitJar
    & javac -encoding UTF-8 -d $binTest -classpath $compileCp -sourcepath (Join-Path $root "src\test\java") @testFiles
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
} else {
    if (-not (Test-Path $bin)) {
        throw "SkipCompile: missing bin/"
    }
    if (-not (Test-Path $binTest)) {
        throw "SkipCompile: missing bin-test/"
    }
}

$runCp = "$bin" + [IO.Path]::PathSeparator + $binTest
Write-Host "Running tests (package cn.lxdb.plugins.muqingyu.fptoken.tests)..."
$jvmArgs = @()
if ($Perf) {
    $jvmArgs += "-Dfptoken.runPerfTests=true"
}
if ($Scale) {
    $jvmArgs += "-Dfptoken.runScaleTests=true"
}
if ($Budget) {
    $jvmArgs += "-Dfptoken.runBudgetPerfTests=true"
}
if ($Stress) {
    $jvmArgs += "-Dfptoken.runStressTests=true"
}
if ($Soak) {
    $jvmArgs += "-Dfptoken.runSoakTests=true"
}
if ($ExtraJvmArgs -and $ExtraJvmArgs.Count -gt 0) {
    $jvmArgs += $ExtraJvmArgs
}

$detailsMode = if ($HtmlReport) { "summary" } else { "tree" }
$junitArgs = @(
    "execute",
    "--class-path", $runCp,
    "--select-package", "cn.lxdb.plugins.muqingyu.fptoken.tests",
    "--details", $detailsMode
)
if ($ExcludePerfTag) {
    $junitArgs += @("-T", "performance")
}

$xmlDir = $null
if ($HtmlReport) {
    $xmlDir = Join-Path $root "build\test-results\junit-xml"
    if (Test-Path $xmlDir) {
        Remove-Item -LiteralPath $xmlDir -Recurse -Force
    }
    New-Item -ItemType Directory -Path $xmlDir -Force | Out-Null
    $junitArgs += @("--reports-dir", $xmlDir)
}

& java @jvmArgs -jar $junitJar @junitArgs
$exit = $LASTEXITCODE

if ($HtmlReport) {
    $htmlFile = Join-Path $root "build\test-results\junit-html\index.html"
    Convert-JUnitXmlDirectoryToHtml $xmlDir $htmlFile
    Write-Host ""
    Write-Host "HTML report: $htmlFile"
}

exit $exit
