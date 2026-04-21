param([string]$vm)
$vmName = if ($vm -match "^vm\d+$") { "$vm-k3s-ing-server" } else { $vm }
$ip = (yc compute instance get --name $vmName --format json | ConvertFrom-Json).network_interfaces[0].primary_v4_address.one_to_one_nat.address
Write-Host "Connecting to $vm @ $ip"
& "C:\Windows\System32\OpenSSH\ssh.exe" -i "C:\Users\B-ZONE\Downloads\lr\YTsaurus\.ssh\id_ed25519" "chig_k3s@$ip"
