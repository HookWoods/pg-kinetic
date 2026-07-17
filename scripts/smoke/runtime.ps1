param(
    [string]$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
)

$ErrorActionPreference = "Stop"

function Get-FreeTcpPort {
    $listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, 0)
    try {
        $listener.Start()
        return $listener.LocalEndpoint.Port
    } finally {
        $listener.Stop()
    }
}

function Get-PgKineticCommandSpec {
    $binaryPath = Join-Path $RepoRoot "target\debug\pg-kinetic.exe"
    if (Test-Path $binaryPath) {
        return [pscustomobject]@{
            FilePath  = $binaryPath
            Arguments = @()
        }
    }

    return [pscustomobject]@{
        FilePath  = "cargo"
        Arguments = @("run", "--quiet", "-p", "pg-kinetic", "--")
    }
}

function Start-PgKineticProxy {
    param([string]$ConfigPath)

    $spec = Get-PgKineticCommandSpec
    return Start-Process -FilePath $spec.FilePath -ArgumentList ($spec.Arguments + @("--config-file", $ConfigPath)) -PassThru -NoNewWindow -WorkingDirectory $RepoRoot
}

function Write-RuntimeConfig {
    param(
        [string]$Path,
        [int]$ListenPort,
        [int]$BackendPort,
        [int]$AdminPort
    )

    $contents = @"
[connection]
listen_addr = "127.0.0.1:$ListenPort"
backend_addr = "127.0.0.1:$BackendPort"

[runtime.lifecycle]
startup_grace_ms = 1000
shutdown_grace_ms = 1000
readiness_fail_during_drain = true
pre_stop_drain_enabled = true
pre_stop_drain_endpoint = "/drain"
startup_backend_checks_enabled = false
termination_grace_period_seconds = 5

[runtime.node]
node_id = "runtime-smoke"

[runtime.engine]
runtime_engine = "tokio_current_thread"

[runtime.production]
control_plane_enabled = true
mirroring_enabled = false
adaptive_enabled = false

[drain]
drain_timeout_ms = 1000

[admin]
admin_addr = "127.0.0.1:$AdminPort"
admin_require_tls = false
admin_allowed_user = "postgres"
admin_query_timeout_ms = 3000
admin_max_clients = 8

[tls]
client_tls_mode = "disable"
backend_tls_mode = "disable"

[auth]
auth_mode = "trust"
"@

    Set-Content -Path $Path -Value $contents -NoNewline
}

function Convert-ToBigEndianBytes {
    param([int]$Value)

    $bytes = [System.BitConverter]::GetBytes($Value)
    if ([System.BitConverter]::IsLittleEndian) {
        [System.Array]::Reverse($bytes)
    }

    return $bytes
}

function New-PgStartupPacket {
    param([string]$User)

    $packet = New-Object System.Collections.Generic.List[byte]
    $packet.AddRange([byte[]](Convert-ToBigEndianBytes -Value 196608))
    $packet.AddRange([byte[]]([System.Text.Encoding]::UTF8.GetBytes("user")))
    $packet.Add(0)
    $packet.AddRange([byte[]]([System.Text.Encoding]::UTF8.GetBytes($User)))
    $packet.Add(0)
    $packet.AddRange([byte[]]([System.Text.Encoding]::UTF8.GetBytes("database")))
    $packet.Add(0)
    $packet.AddRange([byte[]]([System.Text.Encoding]::UTF8.GetBytes("pgkinetic")))
    $packet.Add(0)
    $packet.Add(0)

    $body = $packet.ToArray()
    $message = New-Object System.Collections.Generic.List[byte]
    $message.AddRange([byte[]](Convert-ToBigEndianBytes -Value ($body.Length + 4)))
    $message.AddRange([byte[]]$body)
    return $message.ToArray()
}

function New-PgQueryPacket {
    param([string]$Sql)

    $body = New-Object System.Collections.Generic.List[byte]
    $body.AddRange([byte[]]([System.Text.Encoding]::UTF8.GetBytes($Sql)))
    $body.Add(0)

    $message = New-Object System.Collections.Generic.List[byte]
    $message.Add([byte][char]'Q')
    $bodyBytes = $body.ToArray()
    $message.AddRange([byte[]](Convert-ToBigEndianBytes -Value ($bodyBytes.Length + 4)))
    $message.AddRange([byte[]]$bodyBytes)
    return $message.ToArray()
}

function New-PgTerminatePacket {
    $message = New-Object System.Collections.Generic.List[byte]
    $message.Add([byte][char]'X')
    $message.AddRange([byte[]](Convert-ToBigEndianBytes -Value 4))
    return $message.ToArray()
}

function Read-PgResponse {
    param(
        [System.IO.Stream]$Stream,
        [int]$TimeoutMs = 3000
    )

    $buffer = New-Object byte[] 4096
    $responseBytes = New-Object System.Collections.Generic.List[byte]
    $deadline = (Get-Date).AddMilliseconds($TimeoutMs)
    $quietSince = $null

    while ((Get-Date) -lt $deadline) {
        if ($Stream.DataAvailable) {
            $read = $Stream.Read($buffer, 0, $buffer.Length)
            if ($read -le 0) {
                break
            }

            for ($index = 0; $index -lt $read; $index++) {
                $responseBytes.Add($buffer[$index])
            }
            $quietSince = $null
            continue
        }

        if ($responseBytes.Count -gt 0) {
            if ($null -eq $quietSince) {
                $quietSince = Get-Date
            } elseif (((Get-Date) - $quietSince).TotalMilliseconds -ge 150) {
                break
            }
        }

        Start-Sleep -Milliseconds 50
    }

    return [System.Text.Encoding]::UTF8.GetString($responseBytes.ToArray())
}

function Open-AdminSession {
    param(
        [int]$Port,
        [string]$User
    )

    $client = [System.Net.Sockets.TcpClient]::new()
    $client.Connect("127.0.0.1", $Port)
    $stream = $client.GetStream()
    $startupPacket = New-PgStartupPacket -User $User
    $stream.Write($startupPacket, 0, $startupPacket.Length)
    $null = Read-PgResponse -Stream $stream
    return [pscustomobject]@{
        Client = $client
        Stream = $stream
    }
}

function Invoke-AdminSessionQuery {
    param(
        [pscustomobject]$Session,
        [string]$Sql
    )

    $queryPacket = New-PgQueryPacket -Sql $Sql
    $Session.Stream.Write($queryPacket, 0, $queryPacket.Length)
    return Read-PgResponse -Stream $Session.Stream
}

function Close-AdminSession {
    param([pscustomobject]$Session)

    try {
        $terminatePacket = New-PgTerminatePacket
        $Session.Stream.Write($terminatePacket, 0, $terminatePacket.Length)
        [void](Read-PgResponse -Stream $Session.Stream -TimeoutMs 500)
    } catch {
    } finally {
        $Session.Stream.Dispose()
        $Session.Client.Dispose()
    }
}

function Wait-ForAdminResponse {
    param(
        [int]$Port,
        [string]$Sql,
        [string]$Needle,
        [string]$User = "postgres"
    )

    $deadline = (Get-Date).AddSeconds(30)
    while ((Get-Date) -lt $deadline) {
        $session = $null
        $response = $null
        try {
            $session = Open-AdminSession -Port $Port -User $User
            $response = Invoke-AdminSessionQuery -Session $session -Sql $Sql
            if ($response.Contains($Needle)) {
                return [pscustomobject]@{
                    Session  = $session
                    Response = $response
                }
            }
        } catch {
        } finally {
            if ($session -and -not $response) {
                $session.Stream.Dispose()
                $session.Client.Dispose()
            }
        }

        Start-Sleep -Milliseconds 200
    }

    throw "admin query '$Sql' did not return '$Needle' on port $Port"
}

function Wait-ForSessionResponse {
    param(
        [pscustomobject]$Session,
        [string]$Sql,
        [string]$Needle
    )

    $deadline = (Get-Date).AddSeconds(30)
    while ((Get-Date) -lt $deadline) {
        try {
            $response = Invoke-AdminSessionQuery -Session $Session -Sql $Sql
            if ($response.Contains($Needle)) {
                return $response
            }
        } catch {
        }

        Start-Sleep -Milliseconds 200
    }

    throw "session query '$Sql' did not return '$Needle'"
}

function Assert-Contains {
    param(
        [string]$Text,
        [string]$Needle,
        [string]$Message
    )

    if (-not $Text.Contains($Needle)) {
        throw "$Message`nExpected to find: $Needle`nActual: $Text"
    }
}

function Assert-NotContains {
    param(
        [string]$Text,
        [string]$Needle,
        [string]$Message
    )

    if ($Text.Contains($Needle)) {
        throw "$Message`nDid not expect to find: $Needle`nActual: $Text"
    }
}

Add-Type @"
using System;
using System.Runtime.InteropServices;

public static class ConsoleControl {
    private delegate bool HandlerRoutine(uint ctrlType);
    private static readonly HandlerRoutine Handler = ctrlType => true;

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool SetConsoleCtrlHandler(HandlerRoutine handlerRoutine, bool add);

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool GenerateConsoleCtrlEvent(uint ctrlEvent, uint processGroupId);

    public static void IgnoreCtrlC() {
        SetConsoleCtrlHandler(Handler, true);
    }

    public static bool SendCtrlC() {
        return GenerateConsoleCtrlEvent(0, 0);
    }
}
"@

$tempRoot = Join-Path $env:TEMP "pg-kinetic-runtime-smoke"
New-Item -ItemType Directory -Force -Path $tempRoot | Out-Null
$configPath = Join-Path $tempRoot "runtime.toml"
$listenPort = Get-FreeTcpPort
$backendPort = Get-FreeTcpPort
$adminPort = Get-FreeTcpPort
Write-RuntimeConfig -Path $configPath -ListenPort $listenPort -BackendPort $backendPort -AdminPort $adminPort

$commandSpec = Get-PgKineticCommandSpec
$preflight = & $commandSpec.FilePath @($commandSpec.Arguments + @("preflight", "--config", $configPath, "--format", "json")) 2>&1
if ($LASTEXITCODE -ne 0) {
    throw "preflight failed:`n$($preflight -join [Environment]::NewLine)"
}

$preflightText = ($preflight -join [Environment]::NewLine)
Assert-Contains -Text $preflightText -Needle '"ok":true' -Message "preflight should succeed"

[ConsoleControl]::IgnoreCtrlC()
$proxy = Start-PgKineticProxy -ConfigPath $configPath

try {
    $runtimeState = Wait-ForAdminResponse -Port $adminPort -Sql "SHOW RUNTIME;" -Needle "tokio_current_thread"
    $runtimeResponse = $runtimeState.Response
    Assert-Contains -Text $runtimeResponse -Needle "runtime-smoke" -Message "runtime should report the configured node id"
    Assert-Contains -Text $runtimeResponse -Needle "tokio_current_thread" -Message "runtime should report the selected engine"
    Assert-Contains -Text $runtimeResponse -Needle "starting" -Message "runtime should expose the lifecycle state"
    Assert-Contains -Text $runtimeResponse -Needle "not_ready" -Message "runtime should expose the readiness state"
} finally {
    if ($runtimeState.Session) {
        Close-AdminSession -Session $runtimeState.Session
    }

    if (-not $proxy.HasExited) {
        Stop-Process -Id $proxy.Id -Force
        $proxy.WaitForExit()
    }
}

Write-Host "runtime smoke passed"
