function Run-Throughput($persistent, $count = 500000, $n = 8, $prefetch = 250, $cost = 4, $round = 0, $csv = $null) {
  $env:PREFETCH = "$prefetch"
  $env:BCRYPT_COST = "$cost"
  docker compose up -d --scale consumer=0 consumer | Out-Null
  docker exec rabbit rabbitmqctl clear_policy dlx-tasks 2>$null | Out-Null
  docker exec rabbit rabbitmqctl purge_queue tasks 2>$null | Out-Null
  Start-Sleep 5

  $start = Get-Date
  $flag = if ($persistent) { "true" } else { "false" }
  $pub = Invoke-RestMethod -Method Post -Uri "http://localhost:8080/publish?count=$count&persistent=$flag&confirm=true" -TimeoutSec 600
  $confirmS = $pub.elapsedMs / 1000
  $before = Get-Msgs

  docker compose up -d --scale consumer=$n consumer | Out-Null
  $i = Wait-Consumers $n
  $t0 = Get-Date
  $limit = (Get-Date).AddMinutes(30)
  do {
    Start-Sleep 1
    $m = Get-Msgs
    if ((Get-Date) -gt $limit) { throw "Red se nije ispraznio" }
  } while ($null -eq $m -or $m -gt 0)
  $drain = ((Get-Date) - $t0).TotalSeconds

  $w = [int]$drain + 20
  $hash = Q "sum(increase(tasks_hash_seconds_sum[${w}s])) / sum(increase(tasks_hash_seconds_count[${w}s])) * 1000"
  docker compose up -d --scale consumer=0 consumer | Out-Null

  $row = [pscustomobject]@{
    Krug             = $round
    Start            = $start.ToString("yyyy-MM-dd HH:mm:ss")
    Persistent       = $flag
    Poruka           = $count
    Consumera        = $n
    Prefetch         = $prefetch
    Bcrypt_cost      = $cost
    Prije            = $before
    Potvrda_s        = [math]::Round($confirmS, 1)
    Publish_msg_s    = [math]::Round($count / $confirmS, 0)
    Drain_s          = [math]::Round($drain, 1)
    Consume_msg_s    = [math]::Round($count / $drain, 0)
    Hash_ms          = [math]::Round($hash, 3)
    Valjan           = ($before -eq $count)
  }
  if ($csv) { $row | Export-Csv $csv -Append -NoTypeInformation -Delimiter ';' }
  $row
}

function Run-ThroughputSeries($rounds = 5, $count = 500000, $csv = "throughput.csv") {
  $all = @(); $base = $null
  try {
    Write-Host "=== Zagrijavanje (ne upisuje se u CSV) ==="
    Run-Throughput $true $count | Out-Null
    Run-Throughput $false $count | Out-Null
    Start-Sleep 30
    for ($r = 1; $r -le $rounds; $r++) {
      $order = if ($r % 2 -eq 1) { @($true, $false) } else { @($false, $true) }
      foreach ($p in $order) {
        Write-Host "=== Krug $r od $rounds, persistent: $p ($(Get-Date -Format HH:mm:ss)) ==="
        $row = Run-Throughput $p $count 8 250 4 $r $csv
        $all += $row
        Write-Host ($row | Format-Table | Out-String)
        if (-not $base) { $base = $row.Hash_ms }
        $dev = [math]::Abs($row.Hash_ms - $base) / $base * 100
        if ($dev -gt 15) { Write-Host ("UPOZORENJE: hash {0} ms odstupa {1:N0}% od prvog prolaza ({2} ms)." -f $row.Hash_ms, $dev, $base) -ForegroundColor Yellow }
        Start-Sleep 30
      }
    }
  } finally {
    Remove-Item Env:PREFETCH -ErrorAction SilentlyContinue
    Remove-Item Env:BCRYPT_COST -ErrorAction SilentlyContinue
    docker compose up -d --scale consumer=0 consumer | Out-Null
    docker exec rabbit rabbitmqctl purge_queue tasks 2>$null | Out-Null
  }
  $all
}