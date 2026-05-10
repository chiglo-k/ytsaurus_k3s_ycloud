sudo tee /etc/systemd/system/spyt-cluster.service >/dev/null <<'EOF'
[Unit]
Description=Launch SPYT cluster
After=network-online.target k3s.service
Wants=network-online.target

[Service]
Type=oneshot
User=chig_k3s
Group=chig_k3s
Environment=HOME=/home/chig_k3s
ExecStart=/usr/local/bin/start-spyt-cluster.sh
RemainAfterExit=yes
TimeoutStartSec=900

[Install]
WantedBy=multi-user.target
EOF