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

function Write-ShardingPreviewConfig {
    param(
        [string]$Path,
        [bool]$ShardingEnabled
    )

    $contents = @"
[sharding]
sharding_enabled = $($ShardingEnabled.ToString().ToLowerInvariant())
multi_shard_policy = "first_match"
route_map_reload_strict = true
route_preview_enabled = true

[[sharding.route_maps]]
[sharding.route_maps.scope]
kind = "schema_table"
schema = "public"
table = "orders"

[sharding.route_maps.strategy]
kind = "list"

[[sharding.route_maps.targets]]
kind = "primary"
shard_id = "tenant-a"

[[sharding.route_maps.targets]]
kind = "replicas"
shard_id = "tenant-b"
"@

    Set-Content -Path $Path -Value $contents -NoNewline
}

function Invoke-RoutePreview {
    param(
        [string]$ConfigPath,
        [string]$Sql
    )

    $binaryPath = Get-PgKineticBinaryPath
    if ($binaryPath) {
        $output = & $binaryPath route-preview `
            --config $ConfigPath `
            --database "billing" `
            --user "reporter" `
            --sql $Sql 2>&1
    } else {
        $output = & cargo run --quiet -p pg-kinetic -- route-preview `
            --config $ConfigPath `
            --database "billing" `
            --user "reporter" `
            --sql $Sql 2>&1
    }

    if ($LASTEXITCODE -ne 0) {
        throw "route-preview failed:`n$($output -join [Environment]::NewLine)"
    }

    return ($output -join [Environment]::NewLine).Trim()
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

function Get-PgKineticBinaryPath {
    $candidate = Join-Path $RepoRoot "target\debug\pg-kinetic.exe"
    if (Test-Path $candidate) {
        return $candidate
    }

    return $null
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

function Invoke-AdminQuery {
    param(
        [int]$Port,
        [string]$User,
        [string]$Sql
    )

    $client = [System.Net.Sockets.TcpClient]::new()
    try {
        $client.Connect("127.0.0.1", $Port)
        $stream = $client.GetStream()
        try {
            $startupPacket = New-PgStartupPacket -User $User
            $stream.Write($startupPacket, 0, $startupPacket.Length)
            $null = Read-PgResponse -Stream $stream

            $queryPacket = New-PgQueryPacket -Sql $Sql
            $stream.Write($queryPacket, 0, $queryPacket.Length)
            return Read-PgResponse -Stream $stream
        } finally {
            $stream.Dispose()
        }
    } finally {
        $client.Dispose()
    }
}

function Start-Proxy {
    param(
        [string]$ConfigPath,
        [string]$StdOutLog,
        [string]$StdErrLog
    )

    $binaryPath = Get-PgKineticBinaryPath
    if ($binaryPath) {
        return Start-Process -FilePath $binaryPath -WindowStyle Hidden -PassThru -RedirectStandardOutput $StdOutLog -RedirectStandardError $StdErrLog -WorkingDirectory $RepoRoot -ArgumentList @(
            "--config-file",
            $ConfigPath
        )
    }

    return Start-Process -FilePath "cargo" -WindowStyle Hidden -PassThru -RedirectStandardOutput $StdOutLog -RedirectStandardError $StdErrLog -WorkingDirectory $RepoRoot -ArgumentList @(
        "run",
        "--quiet",
        "-p",
        "pg-kinetic",
        "--",
        "--config-file",
        $ConfigPath
    )
}

function Wait-For-Admin {
    param(
        [int]$Port,
        [string]$User
    )

    $deadline = (Get-Date).AddSeconds(60)
    while ((Get-Date) -lt $deadline) {
        try {
            $result = Invoke-AdminQuery -Port $Port -User $User -Sql "SHOW SHARDS;"
            if ($result) {
                return $result.Trim()
            }
        } catch {
        }

        Start-Sleep -Milliseconds 250
    }

    throw "admin SHOW SHARDS did not become ready on port $Port"
}

$tempRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("pg-kinetic-sharding-smoke-" + [guid]::NewGuid().ToString("n"))
New-Item -ItemType Directory -Path $tempRoot | Out-Null

$proxyProcess = $null
$scriptSucceeded = $false
try {
    $disabledPreviewConfig = Join-Path $tempRoot "disabled-preview.toml"
    $enabledPreviewConfig = Join-Path $tempRoot "enabled-preview.toml"
    Write-ShardingPreviewConfig -Path $disabledPreviewConfig -ShardingEnabled $false
    Write-ShardingPreviewConfig -Path $enabledPreviewConfig -ShardingEnabled $true

    $disabledPreview = Invoke-RoutePreview -ConfigPath $disabledPreviewConfig -Sql "select * from public.orders where tenant_id = 'tenant-a'"
    Assert-Contains -Text $disabledPreview -Needle '"ok":true' -Message "disabled preview failed"
    Assert-Contains -Text $disabledPreview -Needle '"shard_id":null' -Message "disabled preview should not assign a shard"
    Assert-Contains -Text $disabledPreview -Needle '"shard_reason":"no_match"' -Message "disabled preview should report no shard match"

    $knownPreview = Invoke-RoutePreview -ConfigPath $enabledPreviewConfig -Sql "select * from public.orders where tenant_id = 'tenant-a'"
    Assert-Contains -Text $knownPreview -Needle '"ok":true' -Message "known shard preview failed"
    Assert-Contains -Text $knownPreview -Needle '"shard_id":"tenant-a"' -Message "known shard preview should assign tenant-a"
    Assert-Contains -Text $knownPreview -Needle '"reason":"list_match"' -Message "known shard preview should match the list route"

    $unknownPreview = Invoke-RoutePreview -ConfigPath $enabledPreviewConfig -Sql "select * from public.orders where tenant_id = 'tenant-z'"
    Assert-Contains -Text $unknownPreview -Needle '"ok":true' -Message "unknown shard preview failed"
    Assert-Contains -Text $unknownPreview -Needle '"shard_id":null' -Message "unknown shard preview should not assign a shard"
    Assert-Contains -Text $unknownPreview -Needle '"shard_reason":"no_match"' -Message "unknown shard preview should report no match"

    $adminPort = Get-FreeTcpPort
    $listenPort = Get-FreeTcpPort
    $backendPort = Get-FreeTcpPort
    $adminConfigPath = Join-Path $tempRoot "admin.toml"
    $adminConfig = @"
[connection]
listen_addr = "127.0.0.1:$listenPort"
backend_addr = "127.0.0.1:$backendPort"

[admin]
admin_addr = "127.0.0.1:$adminPort"
admin_require_tls = false
admin_query_timeout_ms = 1000
admin_max_clients = 4

[sharding]
sharding_enabled = true
multi_shard_policy = "fan_out"
route_map_reload_strict = true
route_preview_enabled = true

[[sharding.route_maps]]
[sharding.route_maps.scope]
kind = "database_user"
database = "billing"
user = "reporter"

[sharding.route_maps.strategy]
kind = "hash"

[[sharding.route_maps.targets]]
kind = "primary"
shard_id = "tenant-a"

[[sharding.route_maps.targets]]
kind = "replicas"
shard_id = "tenant-b"
"@
    Set-Content -Path $adminConfigPath -Value $adminConfig -NoNewline

    $proxyStdout = Join-Path $tempRoot "proxy.out.log"
    $proxyStderr = Join-Path $tempRoot "proxy.err.log"
    $proxyProcess = Start-Proxy -ConfigPath $adminConfigPath -StdOutLog $proxyStdout -StdErrLog $proxyStderr

    $adminRows = Wait-For-Admin -Port $adminPort -User "admin"
    Assert-Contains -Text $adminRows -Needle "shard_id" -Message "admin SHOW SHARDS should include the shard id column"
    Assert-Contains -Text $adminRows -Needle "lifecycle_state" -Message "admin SHOW SHARDS should include lifecycle state"
    Assert-Contains -Text $adminRows -Needle "health_summary" -Message "admin SHOW SHARDS should include the health summary"

    $multiShardCompose = @(
        Join-Path $RepoRoot "bench\compose.sharded.yml"
        Join-Path $RepoRoot "bench\compose.multi-shard.yml"
        Join-Path $RepoRoot "bench\compose-sharded.yml"
    ) | Where-Object { Test-Path $_ } | Select-Object -First 1

    if ($multiShardCompose) {
        Write-Host "multi-shard compose smoke would run with $multiShardCompose"
    }

    Write-Host "sharding smoke passed"
    $scriptSucceeded = $true
}
finally {
    if ($proxyProcess -and -not $proxyProcess.HasExited) {
        Stop-Process -Id $proxyProcess.Id -Force
        $null = $proxyProcess.WaitForExit(5000)
    }

    if ($scriptSucceeded) {
        if (Test-Path $tempRoot) {
            Remove-Item -LiteralPath $tempRoot -Recurse -Force
        }
    } elseif (Test-Path $tempRoot) {
        Write-Host "sharding smoke kept temp files at $tempRoot"
        foreach ($logName in "proxy.out.log", "proxy.err.log") {
            $logPath = Join-Path $tempRoot $logName
            if (Test-Path $logPath) {
                Write-Host "--- $logName ---"
                Get-Content -Path $logPath | ForEach-Object { Write-Host $_ }
            }
        }
    }
}
