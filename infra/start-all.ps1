$vms = @("vm1-k3s-ing-server","vm2-k3s-ing-server","vm3-k3s-ing-server","vm4-k3s-ing-server","vm5-k3s-ing-server")
$vms | ForEach-Object {
    Write-Host "Starting $_..."
    yc compute instance start --name $_ --async
}
Write-Host "All start commands sent. Wait ~1 min then: .\infra\ssh.ps1 vm1"
