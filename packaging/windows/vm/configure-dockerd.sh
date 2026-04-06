#!/bin/sh
# Runs inside Alpine chroot during OVA build.
# Configures dockerd to listen on TCP for VirtualBox NAT access.
set -e

# Docker already installed via --packages in alpine-make-vm-image
rc-update add docker boot
rc-update add networking boot

# Listen on both Unix socket and TCP (no TLS — localhost NAT only)
mkdir -p /etc/docker
cat > /etc/docker/daemon.json << 'EOF'
{
  "hosts": ["unix:///var/run/docker.sock", "tcp://0.0.0.0:2375"],
  "iptables": true,
  "log-driver": "json-file",
  "log-opts": {"max-size": "10m", "max-file": "3"}
}
EOF

# DHCP on eth0 via VirtualBox NAT
cat > /etc/network/interfaces << 'EOF'
auto lo
iface lo inet loopback
auto eth0
iface eth0 inet dhcp
EOF

# Enable IP forwarding for container networking
echo "net.ipv4.ip_forward=1" >> /etc/sysctl.conf
