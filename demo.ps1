# Node Manager Demo Script
# This script demonstrates the full Node Manager functionality end-to-end.
# It starts the server, sends heartbeats from multiple nodes, shows role
# assignments, demonstrates failure detection, recovery, and status queries.
#
# Prerequisites: cargo build --workspace (run once before this script)

$ErrorActionPreference = "Continue"
$serverAddr = "http://127.0.0.1:50123"
$serverProcess = $null
$cliBin = "$PSScriptRoot\target\debug\nm-cli.exe"
$serverBin = "$PSScriptRoot\target\debug\node-manager-server.exe"

function Cleanup {
    Write-Host "`n--- Cleaning up ---" -ForegroundColor DarkGray
    if ($serverProcess -and !$serverProcess.HasExited) {
        Stop-Process -Id $serverProcess.Id -Force -ErrorAction SilentlyContinue
        Write-Host "Server stopped." -ForegroundColor DarkGray
    }
    if (Test-Path $tempDir) {
        Remove-Item -Recurse -Force $tempDir -ErrorAction SilentlyContinue
    }
}

# --- Setup temp directory and config ---
$tempDir = [System.IO.Path]::Combine([System.IO.Path]::GetTempPath(), "nm-demo-" + [guid]::NewGuid().ToString("N").Substring(0, 8))
New-Item -ItemType Directory -Path $tempDir | Out-Null
$configPath = Join-Path $tempDir "config.yaml"
$statePath = Join-Path $tempDir "state.json"

$configContent = @"
server:
  bind_address: "127.0.0.1:50123"
heartbeat:
  check_interval_secs: 3
  missed_threshold: 2
roles:
  metadata_count: 1
  storage_min_count: 1
  storage_max_percent: 50
  lease_duration_secs: 60
storage:
  backend: json_file
  json_file_path: "$($statePath.Replace('\','/'))"
"@

Set-Content -Path $configPath -Value $configContent

# --- Check prerequisites ---
if (-not (Test-Path $serverBin)) {
    Write-Host "Server binary not found. Building..." -ForegroundColor Yellow
    cargo build -p node-manager-server --bin node-manager-server 2>&1 | Out-Null
}
if (-not (Test-Path $cliBin)) {
    Write-Host "CLI binary not found. Building..." -ForegroundColor Yellow
    cargo build -p cli --bin nm-cli 2>&1 | Out-Null
}
if (-not (Test-Path $serverBin) -or -not (Test-Path $cliBin)) {
    Write-Host "Build failed. Please run: cargo build --workspace" -ForegroundColor Red
    exit 1
}

# --- Helper functions ---

function Run-Cli([string[]]$CliArgs) {
    $output = & $cliBin @CliArgs 2>&1
    # Filter out any stderr lines (PowerShell captures them as ErrorRecord)
    $stdout = $output | Where-Object { $_ -is [string] }
    return $stdout
}

function Send-Heartbeat([string]$NodeId, [string]$ClusterId, [string]$Health = "healthy") {
    Write-Host "`n  Sending heartbeat: node=$NodeId, cluster=$ClusterId, health=$Health" -ForegroundColor Cyan
    $output = Run-Cli @("--server", $serverAddr, "heartbeat", "--node-id", $NodeId, "--cluster-id", $ClusterId, "--health", $Health)
    if ($LASTEXITCODE -ne 0) {
        Write-Host "  [FAILED] Heartbeat request failed" -ForegroundColor Red
        return
    }
    $output | ForEach-Object {
        Write-Host "  $_" -ForegroundColor White
    }
}

function Get-Status([string]$ClusterId = "", [string]$Label = "") {
    $title = if ($Label) { $Label } elseif ($ClusterId) { "Status for cluster: $ClusterId" } else { "Full cluster status" }
    Write-Host "`n  Querying: $title" -ForegroundColor Cyan
    $cliArgs = @("--server", $serverAddr, "status")
    if ($ClusterId) { $cliArgs += @("--cluster-id", $ClusterId) }
    $output = Run-Cli @cliArgs
    $output | ForEach-Object {
        Write-Host "  $_" -ForegroundColor White
    }
}

function Write-Step([int]$Step, [string]$Title, [string]$Description) {
    Write-Host ""
    Write-Host "============================================================" -ForegroundColor Yellow
    Write-Host "  STEP $Step : $Title" -ForegroundColor Yellow
    Write-Host "============================================================" -ForegroundColor Yellow
    Write-Host "  $Description" -ForegroundColor DarkGray
}

# ======================================================================
# DEMO STARTS HERE
# ======================================================================

Write-Host ""
Write-Host "############################################################" -ForegroundColor Magenta
Write-Host "#                                                          #" -ForegroundColor Magenta
Write-Host "#         Node Manager Service - Live Demo                 #" -ForegroundColor Magenta
Write-Host "#                                                          #" -ForegroundColor Magenta
Write-Host "############################################################" -ForegroundColor Magenta
Write-Host ""
Write-Host "This demo shows:" -ForegroundColor Gray
Write-Host "  1. Single node registration with full role assignment" -ForegroundColor Gray
Write-Host "  2. Multi-node cluster with deterministic role distribution" -ForegroundColor Gray
Write-Host "  3. Node failure detection and role reassignment" -ForegroundColor Gray
Write-Host "  4. Node recovery and role restoration" -ForegroundColor Gray
Write-Host "  5. Multi-cluster isolation" -ForegroundColor Gray
Write-Host "  6. Unhealthy node immediately loses roles" -ForegroundColor Gray
Write-Host ""
Write-Host "Config: metadata_count=1, storage_max=50%, heartbeat_interval=3s, missed_threshold=2" -ForegroundColor DarkGray

# --- Start server ---
Write-Step 0 "Start the Node Manager server" "Starting gRPC server on port 50123 with 3s heartbeat interval"

$serverProcess = Start-Process -FilePath $serverBin -ArgumentList "--config", $configPath -PassThru -NoNewWindow -RedirectStandardOutput (Join-Path $tempDir "server-stdout.log") -RedirectStandardError (Join-Path $tempDir "server-stderr.log")

# Wait for server to be ready
Write-Host "  Waiting for server to start..." -ForegroundColor DarkGray
$ready = $false
for ($i = 0; $i -lt 30; $i++) {
    Start-Sleep -Milliseconds 200
    try {
        $tcp = New-Object System.Net.Sockets.TcpClient("127.0.0.1", 50123)
        $tcp.Close()
        $ready = $true
        break
    } catch { }
}
if (!$ready) {
    Write-Host "  [FAILED] Server did not start within 6 seconds" -ForegroundColor Red
    Cleanup; exit 1
}
Write-Host "  Server is ready at $serverAddr" -ForegroundColor Green

# ======================================================================
# STEP 1: First node heartbeat
# ======================================================================
Write-Step 1 "First node registers and gets ALL roles" @"
  A single node in cluster 'prod-east' sends a healthy heartbeat.
  Expected: ASSIGNABLE status with all 3 roles (METADATA + DATA + STORAGE).
  The fencing_token for METADATA should be > 0 (split-brain protection).
"@

Send-Heartbeat -NodeId "data-node-1" -ClusterId "prod-east" -Health "healthy"

Write-Host "`n  --> data-node-1 now holds METADATA (exactly 1 node), DATA (100%), and STORAGE." -ForegroundColor Green

# ======================================================================
# STEP 2: Second node joins
# ======================================================================
Write-Step 2 "Second node joins the same cluster" @"
  data-node-2 sends a heartbeat to cluster 'prod-east'.
  Expected: DATA role assigned (100% of assignable nodes get DATA).
  STORAGE may or may not be assigned (50% max = 1 of 2 nodes).
  METADATA stays on data-node-1 (lowest node_id wins).
"@

Send-Heartbeat -NodeId "data-node-2" -ClusterId "prod-east" -Health "healthy"

Get-Status -ClusterId "prod-east" -Label "Verify: both nodes in cluster, metadata on node-1 only"

# ======================================================================
# STEP 3: Third node joins
# ======================================================================
Write-Step 3 "Third node joins - storage distribution changes" @"
  data-node-3 joins cluster 'prod-east'.
  With 3 nodes and storage_max_percent=50%: max_storage = max(1, 3*50/100) = max(1,1) = 1.
  So only 1 node holds STORAGE (data-node-1, lowest ID).
  All 3 nodes get DATA. METADATA stays on data-node-1.
"@

Send-Heartbeat -NodeId "data-node-3" -ClusterId "prod-east" -Health "healthy"

Get-Status -ClusterId "prod-east" -Label "Full cluster state: 3 nodes, 1 metadata, 3 data, 1 storage"

# ======================================================================
# STEP 4: Node goes unhealthy
# ======================================================================
Write-Step 4 "Node goes UNHEALTHY - immediate role revocation and reassignment" @"
  data-node-1 (which holds METADATA) reports UNHEALTHY status.
  Expected: Immediately becomes UNASSIGNABLE, all roles revoked.
  METADATA transfers to data-node-2 (next lowest node_id).
"@

Write-Host "`n  !!! data-node-1 (metadata holder) reports UNHEALTHY" -ForegroundColor Red
Send-Heartbeat -NodeId "data-node-1" -ClusterId "prod-east" -Health "unhealthy"

# Have node-2 heartbeat to see the new assignment
Send-Heartbeat -NodeId "data-node-2" -ClusterId "prod-east" -Health "healthy"

Get-Status -ClusterId "prod-east" -Label "Verify: node-1 UNASSIGNABLE, METADATA now on node-2"

# ======================================================================
# STEP 5: Node recovery
# ======================================================================
Write-Step 5 "Node recovers from UNASSIGNABLE" @"
  data-node-1 sends a HEALTHY heartbeat.
  Expected: Recovers to ASSIGNABLE status. Gets DATA role back.
  METADATA stays on data-node-2 (no preemption).
"@

Write-Host "`n  data-node-1 is back online, sends healthy heartbeat" -ForegroundColor Green
Send-Heartbeat -NodeId "data-node-1" -ClusterId "prod-east" -Health "healthy"

Get-Status -ClusterId "prod-east" -Label "Verify: node-1 recovered to ASSIGNABLE, metadata still on node-2"

# ======================================================================
# STEP 6: Multi-cluster isolation
# ======================================================================
Write-Step 6 "Multi-cluster isolation" @"
  Register nodes in a second cluster 'prod-west'.
  The two clusters are completely independent - nodes and roles
  in one cluster do not affect the other.
"@

Send-Heartbeat -NodeId "west-node-1" -ClusterId "prod-west" -Health "healthy"
Send-Heartbeat -NodeId "west-node-2" -ClusterId "prod-west" -Health "healthy"

Get-Status -Label "All clusters: both prod-east (3 nodes) and prod-west (2 nodes)"

Get-Status -ClusterId "prod-west" -Label "prod-west cluster only: independent role assignments"

# ======================================================================
# STEP 7: Draining node
# ======================================================================
Write-Step 7 "Node enters DRAINING state" @"
  west-node-2 reports DRAINING status (graceful shutdown).
  Expected: Immediately becomes UNASSIGNABLE, all roles revoked.
  Useful for planned maintenance - the node signals it should
  not receive new work.
"@

Send-Heartbeat -NodeId "west-node-2" -ClusterId "prod-west" -Health "draining"

Get-Status -ClusterId "prod-west" -Label "Verify: west-node-2 is UNASSIGNABLE, all roles on west-node-1"

# ======================================================================
# STEP 8: Final status overview
# ======================================================================
Write-Step 8 "Final system overview" "Complete state of the Node Manager across all clusters."

Get-Status -Label "FINAL STATE: All clusters and nodes"

# --- Summary ---
Write-Host ""
Write-Host "############################################################" -ForegroundColor Magenta
Write-Host "#                    Demo Complete                          #" -ForegroundColor Magenta
Write-Host "############################################################" -ForegroundColor Magenta
Write-Host ""
Write-Host "What we demonstrated:" -ForegroundColor Green
Write-Host "  [1] Node registration with automatic role assignment" -ForegroundColor White
Write-Host "  [2] Deterministic role distribution (metadata=1, data=100%, storage<=50%)" -ForegroundColor White
Write-Host "  [3] Fencing tokens on METADATA for split-brain prevention" -ForegroundColor White
Write-Host "  [4] Immediate role revocation on UNHEALTHY status" -ForegroundColor White
Write-Host "  [5] Automatic METADATA failover to next node" -ForegroundColor White
Write-Host "  [6] Node recovery from UNASSIGNABLE to ASSIGNABLE" -ForegroundColor White
Write-Host "  [7] Complete cluster isolation (prod-east vs prod-west)" -ForegroundColor White
Write-Host "  [8] DRAINING status for graceful shutdown" -ForegroundColor White
Write-Host ""

Cleanup
Write-Host "Demo finished. Server stopped, temp files cleaned up." -ForegroundColor DarkGray
