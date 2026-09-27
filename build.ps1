# Cross-compilation script for kafka-unwedge
$ErrorActionPreference = "Stop"

$targets = @(
    @{ os = "linux"; arch = "amd64"; out = "kafka-unwedge-linux-amd64" },
    @{ os = "linux"; arch = "arm64"; out = "kafka-unwedge-linux-arm64" },
    @{ os = "darwin"; arch = "amd64"; out = "kafka-unwedge-darwin-amd64" },
    @{ os = "darwin"; arch = "arm64"; out = "kafka-unwedge-darwin-arm64" },
    @{ os = "windows"; arch = "amd64"; out = "kafka-unwedge-windows-amd64.exe" }
)

$distDir = Join-Path $PSScriptRoot "dist"
if (-not (Test-Path $distDir)) {
    New-Item -ItemType Directory -Path $distDir -Force | Out-Null
}

Write-Host "Building static binaries in $distDir..." -ForegroundColor Cyan

foreach ($target in $targets) {
    $outPath = Join-Path $distDir $target.out
    Write-Host "Compiling $($target.os)/$($target.arch) -> $($target.out)..."
    $env:GOOS = $target.os
    $env:GOARCH = $target.arch
    $env:CGO_ENABLED = "0"

    go build -trimpath -ldflags="-s -w" -o $outPath ./cmd/kafka-unwedge
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Build failed for $($target.os)/$($target.arch)"
    }

    $size = (Get-Item $outPath).Length
    $sizeMB = [math]::Round($size / 1MB, 2)
    Write-Host "  Success: $size bytes ($sizeMB MB)" -ForegroundColor Green
}

Write-Host "All targets compiled successfully." -ForegroundColor Green
