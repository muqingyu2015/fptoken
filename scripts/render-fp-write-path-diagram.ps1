# 将 docs/fp-token-write-path-detailed.mmd 渲染为高分辨率 PNG
# 需要：npx 缓存中的 @mermaid-js/mermaid-cli，以及本机 Edge/Chrome
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
$Mmd = Join-Path $Root "docs\fp-token-write-path-detailed.mmd"
$Out = Join-Path $Root "docs\fp-token-write-path-detailed.png"

$edgeCandidates = @(
    "${env:ProgramFiles}\Microsoft\Edge\Application\msedge.exe",
    "${env:ProgramFiles(x86)}\Microsoft\Edge\Application\msedge.exe",
    "${env:ProgramFiles}\Google\Chrome\Application\chrome.exe"
)
$browser = $edgeCandidates | Where-Object { Test-Path $_ } | Select-Object -First 1
if (-not $browser) {
    throw "未找到 Edge/Chrome，无法渲染 Mermaid PNG"
}

$npxMmdc = Get-ChildItem -Path "$env:LOCALAPPDATA\npm-cache\_npx" -Recurse -Filter "mmdc.cmd" -ErrorAction SilentlyContinue |
    Select-Object -First 1 -ExpandProperty FullName
if (-not $npxMmdc) {
    Write-Host "首次运行将安装 @mermaid-js/mermaid-cli …"
    npx -y @mermaid-js/mermaid-cli@11.4.0 -v | Out-Null
    $npxMmdc = Get-ChildItem -Path "$env:LOCALAPPDATA\npm-cache\_npx" -Recurse -Filter "mmdc.cmd" -ErrorAction SilentlyContinue |
        Select-Object -First 1 -ExpandProperty FullName
}

$env:PUPPETEER_EXECUTABLE_PATH = $browser
Push-Location $Root
try {
    & $npxMmdc -i $Mmd -o $Out -w 4800 -b white -s 2
    $fi = Get-Item $Out
    Write-Host "OK: $Out ($($fi.Length) bytes)"
} finally {
    Pop-Location
}
