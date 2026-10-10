# Sourced by the AppImage scripts. The file under test is ~/Provisa.AppImage: the one artifact,
# copied into the guest on its own, as a visitor's download is.
#
# Says how the AppImage's runtime runs here. A static runtime needs no loader, so asking it for
# its payload's offset proves it starts at all. Then: can it mount its payload (FUSE, through the
# host's fusermount), or must it unpack itself first (APPIMAGE_EXTRACT_AND_RUN)? Either is the
# one file running unaided; the job's log records which.
APPIMAGE="$HOME/Provisa.AppImage"
chmod +x "$APPIMAGE"

if ! offset="$("$APPIMAGE" --appimage-offset 2>&1)"; then
  echo "$offset"
  echo "::error::The AppImage's runtime does not start on this host."
  exit 1
fi
echo "The runtime starts (payload at byte $offset)."

mount_log="$(mktemp)"
"$APPIMAGE" --appimage-mount > "$mount_log" 2>&1 &
mount_pid=$!
mounted=false
for _ in $(seq 1 20); do
  mount_point="$(head -n 1 "$mount_log" 2>/dev/null || true)"
  if [ -n "$mount_point" ] && [ -x "$mount_point/AppRun" ]; then mounted=true; break; fi
  if ! kill -0 "$mount_pid" 2>/dev/null; then break; fi
  sleep 1
done
kill "$mount_pid" 2>/dev/null || true
wait "$mount_pid" 2>/dev/null || true

if [ "$mounted" = true ]; then
  echo "::notice::AppImage runtime on $(. /etc/os-release; echo "$PRETTY_NAME"): MOUNTED its payload (FUSE)."
else
  cat "$mount_log"
  echo "::notice::AppImage runtime on $(. /etc/os-release; echo "$PRETTY_NAME"): could not mount; ran by EXTRACT-AND-RUN."
  export APPIMAGE_EXTRACT_AND_RUN=1
fi
