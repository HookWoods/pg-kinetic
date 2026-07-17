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

function Write-MirroringConfig {
    param(
        [string]$Path,
        [int]$ListenPort,
        [int]$BackendPort,
        [int]$AdminPort,
        [int]$MirrorTargetPort
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
startup_backend_checks_enabled = false
termination_grace_period_seconds = 5

[runtime.node]
node_id = "mirroring-smoke"

[runtime.engine]
runtime_engine = "tokio_default"

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

[mirror]
mirroring_enabled = true
mirror_mode = "read_only"
mirror_timeout_ms = 50
mirror_max_in_flight = 8

[mirror.target]
address = "127.0.0.1:$MirrorTargetPort"
isolated = true

[mirror.sampling]
mirror_sample_rate = 0.25

[mirror.safety]
mirror_require_isolated_target = true
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

$tempRoot = Join-Path $env:TEMP "pg-kinetic-mirroring-smoke"
New-Item -ItemType Directory -Force -Path $tempRoot | Out-Null
$configPath = Join-Path $tempRoot "mirroring.toml"
$listenPort = Get-FreeTcpPort
$backendPort = Get-FreeTcpPort
$adminPort = Get-FreeTcpPort
$mirrorTargetPort = Get-FreeTcpPort
Write-MirroringConfig -Path $configPath -ListenPort $listenPort -BackendPort $backendPort -AdminPort $adminPort -MirrorTargetPort $mirrorTargetPort

$commandSpec = Get-PgKineticCommandSpec
$preflight = & $commandSpec.FilePath @($commandSpec.Arguments + @("preflight", "--config", $configPath, "--format", "json")) 2>&1
if ($LASTEXITCODE -ne 0) {
    throw "preflight failed:`n$($preflight -join [Environment]::NewLine)"
}

$preflightText = ($preflight -join [Environment]::NewLine)
Assert-Contains -Text $preflightText -Needle '"ok":true' -Message "configured mirror preflight should succeed"

$proxy = Start-PgKineticProxy -ConfigPath $configPath

try {
    $mirrorState = Wait-ForAdminResponse -Port $adminPort -Sql "SHOW MIRRORING;" -Needle "off"
    $mirrorResponse = $mirrorState.Response
    Assert-Contains -Text $mirrorResponse -Needle "sample_rate" -Message "mirror view should show the sample rate"
    Assert-Contains -Text $mirrorResponse -Needle "0.000" -Message "mirror view should report the default disabled sample rate"
    Assert-Contains -Text $mirrorResponse -Needle "mode" -Message "mirror view should expose the mode column"
} finally {
    if ($mirrorState.Session) {
        Close-AdminSession -Session $mirrorState.Session
    }

    if (-not $proxy.HasExited) {
        Stop-Process -Id $proxy.Id -Force
        $proxy.WaitForExit()
    }
}

Write-Host "mirroring smoke passed"
