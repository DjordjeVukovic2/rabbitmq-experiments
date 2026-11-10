function Get-Q($name) {
  foreach ($x in (Invoke-RestMethod "http://localhost:15672/api/queues" -Headers $h)) {
    if ($x.name -eq $name) { return $x }
  }
}

function Run-Poison($variant, $n = 4, $count = 3000, $poison = 10, $round = 0, $csv = $null, $watch = 20) {
  switch ($variant) {
    "requeue" { $env:REQUEUE = "true";  $dlq = $false }
    "drop"    { $env:REQUEUE = "false"; $dlq = $false }
    "dlq"     { $env:REQUEUE = "false"; $dlq = $true  }
    default   { throw "Nepoznata varijanta: $variant" }
  }
  docker compose up -d --scale consumer=0 consumer | Out-Null
  docker exec rabbit rabbitmqctl clear_policy dlx-tasks 2>$null | Out-Null
  docker exec rabbit rabbitmqctl purge_queue tasks 2>$null | Out-Null
  docker exec rabbit rabbitmqctl purge_queue tasks.dlq 2>$null | Out-Null
  if ($dlq) {
    docker exec rabbit rabbitmqctl set_policy dlx-tasks "^tasks$" '{\"dead-letter-exchange\":\"dlx\"}' --apply-to queues | Out-Null
  }
  Start-Sleep 10
  $s0 = Get-QStats
  $start = Get-Date
  $pub = Invoke-RestMethod -Method Post -Uri "http://localhost:8080/publish?count=$count&poison=$poison"
  do { Start-Sleep 2; $i = QueueInfo } while ($i.messages_ready -lt $count)

  docker compose up -d --scale consumer=$n consumer | Out-Null
  $i = Wait-Consumers $n
  $t0 = Get-Date
  $healthy = $count - $poison
  $limit = (Get-Date).AddMinutes(30)

  if ($variant -eq "requeue") {
    do {
      Start-Sleep 2
      $s = Get-QStats
      if ((Get-Date) -gt $limit) { throw "Zdrave poruke nisu obradjene" }
    } while (($s.ack - $s0.ack) -lt $healthy)
    $healthyDone = ((Get-Date) - $t0).TotalSeconds
    Start-Sleep $watch
  } else {
    do {
      Start-Sleep 1
      $i = QueueInfo
      if ((Get-Date) -gt $limit) { throw "Red se nije ispraznio" }
    } while ($i.messages_ready -gt 0 -or $i.messages_unacknowledged -gt 0)
    $healthyDone = ((Get-Date) - $t0).TotalSeconds
  }

  Start-Sleep 10
  $s1 = Get-QStats
  $i = QueueInfo
  $d = Get-Q "tasks.dlq"
  $dlqReady = if ($d) { [int]$d.messages_ready } else { 0 }
  docker compose up -d --scale consumer=0 consumer | Out-Null

  $inQueue = $i.messages_ready + $i.messages_unacknowledged
  $row = [pscustomobject]@{
    Krug             = $round
    Start            = $start.ToString("yyyy-MM-dd HH:mm:ss")
    Varijanta        = $variant
    Consumera        = $n
    Ukupno           = $count
    Poison           = $pub.poison
    Publish_delta    = $s1.publish - $s0.publish
    Ack_delta        = $s1.ack - $s0.ack
    Redelivered      = $s1.redeliver - $s0.redeliver
    U_redu_na_kraju  = $inQueue
    DLQ_poruka       = $dlqReady
    Izgubljeno       = ($s1.publish - $s0.publish) - ($s1.ack - $s0.ack) - $inQueue - $dlqReady
    Zdrave_gotove_s  = [math]::Round($healthyDone, 0)
    Valjan           = ((($s1.publish - $s0.publish) -eq $count) -and (($s1.ack - $s0.ack) -eq $healthy))
  }
  if ($csv) { $row | Export-Csv $csv -Append -NoTypeInformation -Delimiter ';' }
  $row
}

function Run-PoisonSeries($rounds = 2, $variants = @("requeue","drop","dlq"), $n = 4, $count = 3000, $poison = 10, $csv = "poison.csv") {
  $all = @()
  try {
    for ($r = 1; $r -le $rounds; $r++) {
      foreach ($v in $variants) {
        Write-Host "=== Krug $r od $rounds, varijanta: $v ($(Get-Date -Format HH:mm:ss)) ==="
        $row = Run-Poison $v $n $count $poison $r $csv
        $all += $row
        Write-Host ($row | Format-Table | Out-String)
        Start-Sleep 60
      }
    }
  } finally {
    Remove-Item Env:REQUEUE -ErrorAction SilentlyContinue
    docker compose up -d --scale consumer=0 consumer | Out-Null
    docker exec rabbit rabbitmqctl clear_policy dlx-tasks 2>$null | Out-Null
    docker exec rabbit rabbitmqctl purge_queue tasks 2>$null | Out-Null
  }
  $all
}