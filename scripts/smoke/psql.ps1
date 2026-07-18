param(
    [string]$HostName = "127.0.0.1",
    [int]$Port = 58432,
    [string]$User = "postgres",
    [string]$Database = "pgkinetic",
    [string]$Password = "postgres",
    [string]$SslMode = "disable",
    [string]$GssEncMode = "disable",
    [int]$ConnectTimeoutSeconds = 10
)

$ErrorActionPreference = "Stop"
$env:PGPASSWORD = $Password
$env:PGSSLMODE = $SslMode
$env:PGGSSENCMODE = $GssEncMode
$env:PGCONNECT_TIMEOUT = $ConnectTimeoutSeconds

$result = psql -h $HostName -p $Port -U $User -d $Database -Atc "select count(*) from accounts;"

if ($result.Trim() -ne "2") {
    throw "expected account count 2, got '$result'"
}

Write-Host "psql smoke passed on ${HostName}:${Port}"
