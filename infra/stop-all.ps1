$vms = @("vm1-k3s-ing-server","vm2-k3s-ing-server","vm3-k3s-ing-server","vm4-k3s-ing-server","vm5-k3s-ing-server")
$vms | ForEach-Object {
    Write-Host "Stopping $_..."
    yc compute instance stop --name $_ --async
}
Write-Host "All stop commands sent."
