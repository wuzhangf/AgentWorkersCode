param(
    [ValidateSet('Debug','Release')][string]$Profile = 'Debug',
    [string]$LlvmMingwHome,
    [switch]$Test
)
$ErrorActionPreference = 'Stop'
$cargo = Get-Command cargo -ErrorAction SilentlyContinue
if (-not $cargo) { $cargoPath = Join-Path $env:USERPROFILE '.cargo\bin\cargo.exe' } else { $cargoPath = $cargo.Source }
$manifest = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..\Cargo.toml'))
$arguments = @()
if ($LlvmMingwHome) {
    $toolchain = '1.99.1-x86_64-pc-windows-gnu'
    $rustup = Join-Path (Split-Path $cargoPath) 'rustup.exe'
    $previousOutputEncoding = [Console]::OutputEncoding
    [Console]::OutputEncoding = [System.Text.Encoding]::UTF8
    try {
        $sysroot = (& $rustup run $toolchain rustc --print sysroot).Trim()
    } finally {
        [Console]::OutputEncoding = $previousOutputEncoding
    }
    if ($LASTEXITCODE -ne 0) { throw 'PINNED_GNU_TOOLCHAIN_REQUIRED' }
    $env:PATH = (Join-Path $LlvmMingwHome 'bin') + ';' + $env:PATH
    $env:CC = 'x86_64-w64-mingw32-clang'
    $env:AR = 'llvm-ar'
    $env:CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER = Join-Path $sysroot 'lib\rustlib\x86_64-pc-windows-gnu\bin\rust-lld.exe'
    $arguments += '+' + $toolchain
}
if ($Test) { $arguments += 'test' } else { $arguments += 'build' }
$arguments += @('--locked','--manifest-path',$manifest)
if ($Profile -eq 'Release') { $arguments += '--release' }
& $cargoPath @arguments
if ($LASTEXITCODE -ne 0) { throw 'NATIVE_WORKER_BUILD_FAILED' }
