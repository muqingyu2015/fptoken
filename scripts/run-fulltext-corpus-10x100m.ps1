# Build >=10 CSV shards, 100M rows each (~100GB+ total). Resumable.
# Requires: py -3, pip install datasets huggingface_hub, network for The Stack

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$OutDir = Join-Path $Root "sample-data\corpus"
$Log = Join-Path $OutDir "generate.log"

New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

py -3 -m pip install datasets huggingface_hub -q

Write-Host "Logging to $Log"
Write-Host "Output: $OutDir"

py -3 (Join-Path $Root "scripts\generate-fulltext-corpus.py") `
  --s3-codesearchnet `
  --codesearchnet `
  --stack `
  --clone-repos `
  --local "$Root" "D:\开源项目源码" `
  --rows-per-file 100000000 `
  --min-parts 10 `
  --out-dir $OutDir `
  2>&1 | Tee-Object -FilePath $Log
