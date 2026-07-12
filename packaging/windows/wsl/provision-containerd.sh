#!/bin/sh
# Provision containerd + nerdctl inside the Provisa WSL2 distro (one-time).
# Installs the bundled nerdctl-full archive (containerd, runc, CNI, buildkit) and
# writes a start script. The Windows equivalent of the Lima nerdctl-full install
# on macOS. Runs as root inside the distro. POSIX sh (Alpine/Ubuntu rootfs).
set -eu

NERDCTL_ARCHIVE="$1"   # /mnt/... path to nerdctl-full-<ver>-linux-amd64.tar.gz

if command -v nerdctl >/dev/null 2>&1; then
  echo "[provision] nerdctl already installed."
  exit 0
fi

echo "[provision] Installing nerdctl-full into /usr/local ..."
tar -C /usr/local -xzf "$NERDCTL_ARCHIVE"

# containerd needs a writable state/root; WSL2 has no systemd by default, so we
# start containerd as a plain background daemon from start-containerd.sh.
mkdir -p /etc/nerdctl /var/lib/containerd /run/containerd
cat > /etc/nerdctl/nerdctl.toml <<'TOML'
address = "/run/containerd/containerd.sock"
namespace = "default"
TOML

echo "[provision] Done."
