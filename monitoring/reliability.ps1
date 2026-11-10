function Get-QStats {
  $q = $null
  foreach ($x in (Invoke-RestMethod "http://localhost:15672/api/queues" -Headers $h)) {
    if ($x.name -eq 'tasks') { $q = $x }
  }
  $s = $q.message_stats
  [pscustomobject]@{
    publish   = [long]$(if ($s -and $s.publish)   { $s.publish }   else { 0 })
    ack       = [long]$(if ($s -and $s.ack)       { $s.ack }       else { 0 })
    redeliver = [long]$(if ($s -and $s.redeliver) { $s.redeliver } else { 0 })
  }
}

function Run-Kill($prefetch, $n = 4, $count = 10000, $round = 0, $csv = $null, $killAfter = 30, $mode = "kill") {
  $env:PREFETCH = "$prefetch"
  docker compose up -d --scale consumer=0 consumer | Out-Null
  docker exec rabbit rabbitmqctl purge_queue tasks 2>$null | Out-Null
  Start-Sleep 10
  $s0 = Get-QStats
  $start = Get-Date
  Invoke-RestMethod -Method Post -Uri "http://localhost:8080/publish?count=$count" | Out-Null
  do { Start-Sleep 2; $i = QueueInfo } while ($i.messages_ready -lt $count)

  docker compose up -d --scale consumer=$n consumer | Out-Null
  $i = Wait-Consumers $n
  $t0 = Get-Date
  $pc = (Invoke-RestMethod "http://localhost:15672/api/consumers" -Headers $h | ForEach-Object { $_.prefetch_count } | Sort-Object -Unique) -join ","

  Start-Sleep $killAfter
  $before = QueueInfo
  $id = (docker compose ps -q consumer | Select-Object -First 1)
  if ($mode -eq "stop") { docker stop $id | Out-Null } else { docker kill $id | Out-Null }
  Start-Sleep 8
  $after = QueueInfo

  $limit = (Get-Date).AddMinutes(30)
  do {
    Start-Sleep 1
    $i = QueueInfo
    if ((Get-Date) -gt $limit) { throw "Red se nije ispraznio" }
  } while ($i.messages_ready -gt 0 -or $i.messages_unacknowledged -gt 0)
  $drain = ((Get-Date) - $t0).TotalSeconds

  Start-Sleep 10
  $s1 = Get-QStats
  docker compose up -d --scale consumer=0 consumer | Out-Null

  $row = [pscustomobject]@{
    Krug                  = $round
    Start                 = $start.ToString("yyyy-MM-dd HH:mm:ss")
    Prefetch              = $prefetch
    Prefetch_potvrdjen    = $pc
    Nacin                 = $mode
    Consumera             = $n
    Kill_nakon_s          = $killAfter
    Ready_prije           = $before.messages_ready
    Unacked_prije         = $before.messages_unacknowledged
    Ocekivano_redelivered = [math]::Round($before.messages_unacknowledged / $n, 0)
    Ready_poslije         = $after.messages_ready
    Consumers_poslije     = $after.consumers
    Redelivered           = $s1.redeliver - $s0.redeliver
    Publish_delta         = $s1.publish - $s0.publish
    Ack_delta             = $s1.ack - $s0.ack
    Drain_s               = [math]::Round($drain, 0)
    Valjan                = (($pc -eq "$prefetch") -and (($s1.publish - $s0.publish) -eq $count) -and (($s1.ack - $s0.ack) -eq $count))
  }
  if ($csv) { $row | Export-Csv $csv -Append -NoTypeInformation -Delimiter ';' }
  $row
}

function Run-KillSeries($rounds = 3, $values = @(1,50,500,2500), $n = 4, $count = 10000, $killAfter = 30, $mode = "kill", $csv = "reliability_kill.csv") {
  $all = @()
  try {
    for ($r = 1; $r -le $rounds; $r++) {
      foreach ($p in $values) {
        Write-Host "=== Krug $r od $rounds, prefetch: $p ($(Get-Date -Format HH:mm:ss)) ==="
        $row = Run-Kill $p $n $count $r $csv $killAfter $mode
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