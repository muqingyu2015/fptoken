#Requires -Version 5.1
<#
.SYNOPSIS
  根据 Eclipse .classpath 中的 lib 条目，校验 lib/ 下 jar 是否齐全（不自动下载，仅报告缺失）。

.DESCRIPTION
  将 .classpath 里 path=".../lib/xxx.jar" 与 lib/ 目录对照。
  若你从 LXDB 工程复制依赖，请保证与 .classpath 列表一致。
#>
$ErrorActionPreference = "Stop"
$root = Split-Path $PSScriptRoot -Parent
$classpath = Join-Path $root ".classpath"
[xml]$doc = Get-Content -LiteralPath $classpath -Encoding UTF8
$expected = @($doc.classpath.classpathentry |
    Where-Object { $_.path -match '[/\\]lib[/\\][^/\\]+\.jar$' } |
    ForEach-Object { Split-Path $_.path -Leaf } |
    Sort-Object -Unique)
$present = @(Get-ChildItem -Path (Join-Path $root "lib") -Filter "*.jar" -File |
    ForEach-Object { $_.Name } | Sort-Object -Unique)
$missing = @($expected | Where-Object { $_ -notin $present })
$extra = @($present | Where-Object { $_ -notin $expected })
Write-Host "Expected jars (from .classpath): $($expected.Count)"
Write-Host "Present in lib/: $($present.Count)"
if ($missing.Count -gt 0) {
    Write-Host "MISSING:" -ForegroundColor Red
    $missing | ForEach-Object { Write-Host "  $_" }
    exit 1
}
if ($extra.Count -gt 0) {
    Write-Host "Extra (not in .classpath): $($extra.Count)" -ForegroundColor Yellow
}
Write-Host "lib/ matches .classpath." -ForegroundColor Green
exit 0
