function Wait-Consumers($k, $minutes = 3) {
  $limit = (Get-Date).AddMinutes($minutes)
  do {
    Start-Sleep 1
    $i = QueueInfo
    if ((Get-Date) -gt $limit) { throw "Consumeri se nisu povezali ($k)" }
  } while ($i.consumers -lt $k)
  $i
}

function Run-Prefetch($prefetch, $n = 8, $count = 20000, $round = 0, $csv = $null, $first = 0, $delay = 20) {
  if ($first -le 0 -or $first -gt $n) { $first = $n }
  $env:PREFETCH = "$prefetch"
  docker compose up -d --scale consumer=0 consumer | Out-Null
  docker exec rabbit rabbitmqctl purge_queue tasks 2>$null | Out-Null
  Invoke-RestMethod -Method Post -Uri "http://localhost:8080/publish?count=$count" | Out-Null
  do { Start-Sleep 2; $i = QueueInfo } while ($i.messages_ready -lt $count)

  $from = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
  $start = Get-Date
  docker compose up -d --scale consumer=$first consumer | Out-Null
  $i = Wait-Consumers $first
  $t0 = Get-Date
  $remaining = $i.messages_ready + $i.messages_unacknowledged

  if ($first -lt $n) {
    Start-Sleep $delay
    docker compose up -d --scale consumer=$n consumer | Out-Null
    $i = Wait-Consumers $n
  }
  $pc = (Invoke-RestMethod "http://localhost:15672/api/consumers" -Headers $h | ForEach-Object { $_.prefetch_count } | Sort-Object -Unique) -join ","

  $limit = (Get-Date).AddMinutes(30)
  do {
    Start-Sleep 1
    $i = QueueInfo
    if ((Get-Date) -gt $limit) { throw "Red se nije ispraznio" }
  } while ($i.messages_ready -gt 0 -or $i.messages_unacknowledged -gt 0)
  $drain = ((Get-Date) - $t0).TotalSeconds

  Start-Sleep 10
  $to = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
  $res = (Invoke-RestMethod ("http://localhost:9090/api/v1/query_range?query=tasks_processed_total&start=$from&end=$to&step=5")).data.result
  $fin = @(); $cnt = @()
  foreach ($s in $res) {
    $last = [double]$s.values[-1][1]
    $f = $s.values | Where-Object { [double]$_[1] -eq $last } | Select-Object -First 1
    $fin += [double]$f[0]
    $cnt += $last
  }

  $w = [int](((Get-Date) - $t0).TotalSeconds) + 20
  $hash = Q "sum(increase(tasks_hash_seconds_sum[${w}s])) / sum(increase(tasks_hash_seconds_count[${w}s])) * 1000"
  docker compose up -d --scale consumer=0 consumer | Out-Null

  $t0u    = ([DateTimeOffset]$t0).ToUnixTimeSeconds()
  $maxFin = ($fin | Measure-Object -Maximum).Maximum
  $minFin = ($fin | Measure-Object -Minimum).Minimum
  $idle   = ($fin | ForEach-Object { $maxFin - $_ } | Measure-Object -Sum).Sum
  $mean   = ($cnt | Measure-Object -Average).Average
  $sd     = if ($cnt.Count -gt 1) { [math]::Sqrt((($cnt | ForEach-Object { ($_ - $mean) * ($_ - $mean) } | Measure-Object -Sum).Sum) / ($cnt.Count - 1)) } else { 0 }
  $sum    = ($cnt | Measure-Object -Sum).Sum

  $row = [pscustomobject]@{
    Krug                 = $round
    Start                = $start.ToString("yyyy-MM-dd HH:mm:ss")
    Prefetch             = $prefetch
    Prefetch_potvrdjen   = $pc
    Pocetnih_instanci    = $first
    Kasnjenje_s          = $(if ($first -lt $n) { $delay } else { 0 })
    Instanci_u_metrikama = $cnt.Count
    Drain_s              = [math]::Round($drain, 0)
    Throughput_msg_s     = [math]::Round($remaining / $drain, 1)
    Rep_s                = [math]::Round($maxFin - $minFin, 0)
    Idle_pct             = [math]::Round($idle / ($n * [math]::Max(1, $maxFin - $t0u)) * 100, 1)
    Min_po_inst          = ($cnt | Measure-Object -Minimum).Minimum
    Max_po_inst          = ($cnt | Measure-Object -Maximum).Maximum
    CV_pct               = [math]::Round($sd / $mean * 100, 1)
    Hash_ms              = [math]::Round($hash, 1)
    Valjan               = (($pc -eq "$prefetch") -and ($cnt.Count -eq $n) -and ($sum -eq $count))
  }
  if ($csv) { $row | Export-Csv $csv -Append -NoTypeInformation -Delimiter ';' }
  $row
}

function Run-PrefetchSeries($rounds = 3, $values = @(1,50,500,2500,5000), $n = 8, $first = 4, $delay = 20, $count = 20000, $csv = "prefetch_staged.csv") {
  $all = @()
  try {
    for ($r = 1; $r -le $rounds; $r++) {
      foreach ($p in $values) {
        Write-Host "=== Krug $r od $rounds, prefetch: $p ($(Get-Date -Format HH:mm:ss)) ==="
        $row = Run-Prefetch $p $n $count $r $csv $first $delay
        $all += $row
        Write-Host ($row | Format-Table | Out-String)
        Start-Sleep 60
      }
    }
  } finally {
    Remove-Item Env:PREFETCH -ErrorAction SilentlyContinue
    docker compose up -d --scale consumer=0 consumer | Out-Null
  }
  $all
}