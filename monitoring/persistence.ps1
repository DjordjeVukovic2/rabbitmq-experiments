function Get-Msgs($queue = "tasks") {
  $out = docker exec rabbit rabbitmqctl list_queues name messages --quiet 2>$null
  foreach ($line in $out) {
    if ($line -match ("^" + [regex]::Escape($queue) + "\s+(\d+)\s*$")) { return [int]$Matches[1] }
  }
  return $null
}

function Wait-Stable($queue = "tasks", $minutes = 5) {
  $limit = (Get-Date).AddMinutes($minutes)
  $prev = -1
  while ($true) {
    $m = Get-Msgs $queue
    if ($null -ne $m -and $m -eq $prev) { return $m }
    $prev = if ($null -ne $m) { $m } else { -1 }
    if ((Get-Date) -gt $limit) { throw "Broj poruka se nije ustalio" }
    Start-Sleep 5
  }
}

function Run-Persist($persistent, $count = 20000, $mode = "restart", $round = 0, $csv = $null) {
  docker compose up -d --scale consumer=0 consumer | Out-Null
  docker exec rabbit rabbitmqctl clear_policy dlx-tasks 2>$null | Out-Null
  docker exec rabbit rabbitmqctl purge_queue tasks 2>$null | Out-Null
  Start-Sleep 5
  $start = Get-Date
  $flag = if ($persistent) { "true" } else { "false" }
  $pub = Invoke-RestMethod -Method Post -Uri "http://localhost:8080/publish?count=$count&persistent=$flag"
  $before = Wait-Stable

  $t0 = Get-Date
  if ($mode -eq "kill") {
    docker kill rabbit | Out-Null
    docker start rabbit | Out-Null
  } else {
    docker restart -t 60 rabbit | Out-Null
  }
  $limit = (Get-Date).AddMinutes(5)
  do {
    Start-Sleep 1
    $m = Get-Msgs
    if ((Get-Date) -gt $limit) { throw "RabbitMQ se nije oporavio" }
  } while ($null -eq $m)
  $up = ((Get-Date) - $t0).TotalSeconds
  $after = Wait-Stable

  docker exec rabbit rabbitmqctl purge_queue tasks 2>$null | Out-Null

  $row = [pscustomobject]@{
    Krug                 = $round
    Start                = $start.ToString("yyyy-MM-dd HH:mm:ss")
    Persistent           = $flag
    Nacin                = $mode
    Poslano              = $count
    Prije                = $before
    Poslije              = $after
    Izgubljeno           = $before - $after
    Preostalo_pct        = [math]::Round($after / $before * 100, 1)
    Restart_do_spreman_s = [math]::Round($up, 0)
    Publish_ms           = $pub.elapsedMs
    Valjan               = ($before -eq $count)
  }
  if ($csv) { $row | Export-Csv $csv -Append -NoTypeInformation -Delimiter ';' }
  $row
}

function Run-PersistSeries($rounds = 3, $modes = @("restart","kill"), $count = 20000, $csv = "persistence.csv") {
  $all = @()
  for ($r = 1; $r -le $rounds; $r++) {
    foreach ($p in @($true, $false)) {
      foreach ($mode in $modes) {
        Write-Host "=== Krug $r od $rounds, persistent: $p, nacin: $mode ($(Get-Date -Format HH:mm:ss)) ==="
        $row = Run-Persist $p $count $mode $r $csv
        $all += $row
        Write-Host ($row | Format-Table | Out-String)
        Start-Sleep 20
      }
    }
  }
  $all
}