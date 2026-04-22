# 编译主代码与 JUnit5 测试，并执行 cn.lxdb.plugins.muqingyu.fptoken.tests 包。
# 用法:
#   .\scripts\run-fptoken-tests.ps1              # 默认：功能 + 场景 + 正确性（约 40+ 用例）
#   .\scripts\run-fptoken-tests.ps1 -Perf        # 加 -Dfptoken.runPerfTests=true（旧版性能类）
#   .\scripts\run-fptoken-tests.ps1 -Scale         # 加 -Dfptoken.runScaleTests=true（大规模扩展场景）
#   .\scripts\run-fptoken-tests.ps1 -Budget        # 加 -Dfptoken.runBudgetPerfTests=true（P-002～P-005 预算烟测）
#   .\scripts\run-fptoken-tests.ps1 -Stress        # 加 -Dfptoken.runStressTests=true（极限压力用例）
#   .\scripts\run-fptoken-tests.ps1 -Soak          # 加 -Dfptoken.runSoakTests=true（浸泡稳定性用例）
# 可组合: -Perf -Scale -Budget -Stress -Soak
# 可选 JVM 属性示例:
#   -Dfptoken.perf.records=10000 -Dfptoken.indexBudgetMs=60000 -Dfptoken.e2eBudgetMs=120000

param(
    [switch] $Perf,
    [switch] $Scale,
    [switch] $Budget,
    [switch] $Stress,
    [switch] $Soak
)

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

# 命令行跑测试用（fat jar）
$junitJar = Join-Path $lib "junit-platform-console-standalone-1.10.2.jar"
Ensure-MavenJar "junit-platform-console-standalone-1.10.2.jar" `
    "https://repo1.maven.org/maven2/org/junit/platform/junit-platform-console-standalone/1.10.2/junit-platform-console-standalone-1.10.2.jar"

# Eclipse / IDE 编译测试源码用（Jupiter API 版本号 5.x 与 Platform 1.10.2 配套，勿用 1.10.2）
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

$bin = Join-Path $root "bin"
$binTest = Join-Path $root "bin-test"
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
& java @jvmArgs -jar $junitJar execute --class-path $runCp --select-package cn.lxdb.plugins.muqingyu.fptoken.tests
exit $LASTEXITCODE
