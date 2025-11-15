# test_flow.ps1 
# Usage:
#  - Terminal 1: start workers 
#  - Terminal 2: run this script

Write-Host "== Enqueue sample jobs =="
Write-Host "1) Enqueue a successful job"
& python .\queuectl.py enqueue --command 'echo success job' 2>&1 | ForEach-Object { Write-Host $_ }

Write-Host "`n2) Enqueue a failing job (Windows cmd)"
& python .\queuectl.py enqueue --command 'cmd /c exit 1' 2>&1 | ForEach-Object { Write-Host $_ }

Write-Host "`n3) Enqueue a failing job (python exit)"
& python .\queuectl.py enqueue --command "python -c `"import sys; sys.exit(1)`"" 2>&1 | ForEach-Object { Write-Host $_ }

Write-Host "`n== Immediate job list/status =="
& python .\queuectl.py list --limit 50 2>&1 | ForEach-Object { Write-Host $_ }
& python .\queuectl.py status 2>&1 | ForEach-Object { Write-Host $_ }

$maxAttempts = 30   
$attempt = 0
while ($attempt -lt $maxAttempts) {
    Start-Sleep -Seconds 2
    $attempt++
    Write-Host "`n[Poll $attempt] Checking status..."
    $statusOutput = & python .\queuectl.py status 2>&1
    $statusOutput | ForEach-Object { Write-Host $_ }

    $pendingZero = $false
    $processingZero = $false
    foreach ($line in $statusOutput) {
        if ($line -match "pending\s*:\s*0") { $pendingZero = $true }
        if ($line -match "processing\s*:\s*0") { $processingZero = $true }
    }
    if ($pendingZero -and $processingZero) {
        Write-Host "`nAll jobs processed (no pending/processing)."
        break
    }
}

Write-Host "`n== Final job list =="
& python .\queuectl.py list --limit 200 2>&1 | ForEach-Object { Write-Host $_ }

Write-Host "`n== Final status =="
& python .\queuectl.py status 2>&1 | ForEach-Object { Write-Host $_ }

Write-Host "`n== DLQ (dead jobs) =="
& python .\queuectl.py dlq list --limit 200 2>&1 | ForEach-Object { Write-Host $_ }

Write-Host "`nScript finished."
