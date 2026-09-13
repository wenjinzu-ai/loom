param(
    [string]$Message,
    [string]$SessionId = "",
    [string]$TenantId = "scenario-test",
    [string]$UserId = "tester"
)

$utf8 = New-Object System.Text.UTF8Encoding $false
$req = @{ message = $Message; tenant_id = $TenantId; user_id = $UserId }
if ($SessionId) { $req.session_id = $SessionId }
$json = $req | ConvertTo-Json -Depth 10 -Compress
[System.IO.File]::WriteAllText("$env:TEMP\loom_req.json", $json, $utf8)

$resp = curl.exe -s -X POST "http://127.0.0.1:3000/chat" `
    -H "Content-Type: application/json; charset=utf-8" `
    --data-binary "@$env:TEMP\loom_req.json" --max-time 180

$resp | Out-File -FilePath "$env:TEMP\loom_resp.txt" -Encoding utf8

$events = @()
$currentEvent = ""
foreach ($line in $resp -split "`n") {
    if ($line -match '^event:\s*(.*)$') {
        $currentEvent = $matches[1].Trim()
    } elseif ($line -match '^data:\s*(.*)$') {
        $data = $matches[1]
        if ($currentEvent -eq "message") {
            try {
                $obj = $data | ConvertFrom-Json
                $sid = $obj.session_id
                $text = $obj.text
                $tools = $obj.tool_calls_made
                Write-Host "=== SESSION: $sid ===" -ForegroundColor Cyan
                Write-Host "TOOL_CALLS: $tools" -ForegroundColor Yellow
                Write-Host "RESPONSE:" -ForegroundColor Green
                Write-Host $text
                Write-Host "=== END ===" -ForegroundColor Cyan
            } catch {}
        } elseif ($currentEvent -eq "interrupt") {
            try {
                $obj = $data | ConvertFrom-Json
                Write-Host "=== INTERRUPT ===" -ForegroundColor Magenta
                Write-Host "CHECKPOINT_ID: $($obj.checkpoint_id)" -ForegroundColor Yellow
                Write-Host "SESSION_ID: $($obj.session_id)" -ForegroundColor Yellow
                Write-Host "VALUE: $($obj.value | ConvertTo-Json -Depth 5)" -ForegroundColor Green
                Write-Host "=== END ===" -ForegroundColor Magenta
            } catch {}
        } elseif ($currentEvent -eq "progress") {
            try {
                $obj = $data | ConvertFrom-Json
                if ($obj.type -eq "tool_start") {
                    Write-Host "  [TOOL_START] $($obj.tool)" -ForegroundColor DarkGray
                } elseif ($obj.type -eq "tool_end") {
                    $err = if ($obj.is_error) { "ERROR" } else { "ok" }
                    Write-Host "  [TOOL_END] $($obj.tool) [$err]" -ForegroundColor DarkGray
                }
            } catch {}
        }
    }
}