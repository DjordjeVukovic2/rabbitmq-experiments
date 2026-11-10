$h = @{Authorization = "Basic " + [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes("experiment:experiment"))}

function Q($q) {
  $r = (Invoke-RestMethod ("http://localhost:9090/api/v1/query?query=" + [uri]::EscapeDataString($q))).data.result
  if ($r.Count -eq 0) { return $null }
  [double]$r[0].value[1]
}

function QueueInfo {
  Invoke-RestMethod -Uri "http://localhost:15672/api/queues/%2F/tasks" -Headers $h
}

function Run-Pass($n, $count = 60000, $csv = $null, $round = 0) {
  docker compose up -d --scale consumer=0 consumer | Out-Null
  docker exec rabbit rabbitmqctl purge_queue tasks | Out-Null
  Invoke-RestMethod -Method Post -Uri "http://localhost:8080/publish?count=$count" | Out-Null
  do { Start-Sleep 2; $i = QueueInfo } while ($i.messages_ready -lt $count)

  $start = Get-Date
  docker compose up -d --scale consumer=$n consumer | Out-Null
  Start-Sleep 60
  $connected = (QueueInfo).consumers
  Start-Sleep 300

  $tp   = Q 'sum(rate(tasks_processed_total[5m]))'
  $hash = Q 'sum(rate(tasks_hash_seconds_sum[5m])) / sum(rate(tasks_hash_seconds_count[5m])) * 1000'
  $left = (QueueInfo).messages_ready
  docker compose up -d --scale consumer=0 consumer | Out-Null

  $row = [pscustomobject]@{
    Krug             = $round
    Start            = $start.ToString("yyyy-MM-dd HH:mm:ss")
    Instanci         = $n
    Throughput_msg_s = [math]::Round($tp, 2)
    Hash_ms          = [math]::Round($hash, 1)
    Povezano         = $connected
    Preostalo_u_redu = $left
    Valjan           = (($connected -eq $n) -and ($left -gt 5000))
  }
  if ($csv) { $row | Export-Csv $csv -Append -NoTypeInformation -Delimiter ';' }
  $row
}

function Run-Scaling($rounds = 3, $instances = @(1,2,4,6,8), $count = 60000, $csv = "scaling3.csv") {
  $all = @(); $ref = $null
  for ($r = 1; $r -le $rounds; $r++) {
    foreach ($n in $instances) {
      Write-Host "=== Krug $r od $rounds, instanci: $n ($(Get-Date -Format HH:mm:ss)) ==="
      $row = Run-Pass $n $count $csv $r
      if ($n -eq 1 -and $null -eq $ref) { $ref = $row.Hash_ms }
      if ($n -eq 1 -and $ref -and [math]::Abs($row.Hash_ms - $ref) / $ref -gt 0.15) {
        Write-Host "UPOZORENJE: hash za n=1 je $($row.Hash_ms) ms, referenca $ref ms. Stanje računara se promijenilo." -ForegroundColor Yellow
      }
      $all += $row
      Write-Host ($row | Format-Table | Out-String)
      if (-not $row.Valjan) {
        Write-Host "Prolaz nije valjan, ponavljam."
        Start-Sleep 60
        $row = Run-Pass $n $count $csv $r
        $all += $row
        Write-Host ($row | Format-Table | Out-String)
      }
      Start-Sleep 60
    }
  }
  $all
}