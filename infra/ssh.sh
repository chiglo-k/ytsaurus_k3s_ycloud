#!/bin/bash
# Usage: ./ssh.sh vm1
VM=$1
IP=$(yc compute instance get --name "$VM" --format json | python -c "import sys,json; d=json.load(sys.stdin); print(d['network_interfaces'][0]['primary_v4_address']['one_to_one_nat']['address'])")
echo "Connecting to $VM @ $IP"
ssh -i "C:/Users/B-ZONE/Downloads/lr/YTsaurus/.ssh/id_ed25519" "chig_k3s@$IP"
