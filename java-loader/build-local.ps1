param(
    [string]$JdbcJar = (Join-Path $env:USERPROFILE '.m2\repository\com\microsoft\sqlserver\mssql-jdbc\12.8.1.jre8\mssql-jdbc-12.8.1.jre8.jar')
)

$ErrorActionPreference = 'Stop'
if (-not (Test-Path -LiteralPath $JdbcJar -PathType Leaf)) {
    throw "JDBC driver not found: $JdbcJar. Pass -JdbcJar with a local SQL Server JDBC JAR for your Java version."
}
$javacPath = (Get-Command javac -ErrorAction Stop).Source
$jarPath = Join-Path (Split-Path -Parent $javacPath) 'jar.exe'
if (-not (Test-Path -LiteralPath $jarPath)) {
    throw "JDK jar.exe not found next to javac: $jarPath"
}
$targetPath = Join-Path $PSScriptRoot 'target'
$classPath = Join-Path $targetPath 'classes'
$libPath = Join-Path $targetPath 'lib'
New-Item -ItemType Directory -Force $classPath, $libPath | Out-Null
$sources = @(Get-ChildItem -LiteralPath (Join-Path $PSScriptRoot 'src\main\java') -Filter '*.java' -Recurse |
    Select-Object -ExpandProperty FullName)
& $javacPath -encoding UTF-8 -source 8 -target 8 -d $classPath $sources
if ($LASTEXITCODE -ne 0) {
    throw "Compilation failed with exit code $LASTEXITCODE"
}
Copy-Item -LiteralPath $JdbcJar -Destination (Join-Path $libPath 'mssql-jdbc.jar') -Force
$outputJar = Join-Path $targetPath 'diagnostic-data-loader.jar'
& $jarPath cfm $outputJar (Join-Path $PSScriptRoot 'manifest.mf') -C $classPath .
if ($LASTEXITCODE -ne 0) {
    throw "Packaging failed with exit code $LASTEXITCODE"
}
Write-Output "Built $outputJar"
Write-Output 'Keep target/lib/mssql-jdbc.jar alongside the application JAR when moving it.'
