#!/usr/bin/env bash
# Package a release's core images as release assets, each under GitHub's 2 GiB per-asset limit.
#
# Usage: package_core_images.sh <images-dir> <arch> <tag> <out-dir> <part-bytes>
#   <images-dir>  the gzipped `docker save` tarballs (<name>.tar.gz) release-prebuilt.yml's
#                 pull-images-core(-amd64) job saved
#   <arch>        arm64 | amd64
#   <part-bytes>  the size of a part (release-prebuilt.yml passes 1 GiB: well under the limit,
#                 and a failed download repeats at most one part)
#
# Writes into <out-dir>, for every image:
#   provisa-core-image-<name>-<arch>-<tag>.tar.gz            the image's tarball, as saved
#   provisa-core-image-<name>-<arch>-<tag>.tar.gz.part<N>    in its place, when the tarball is
#                                                            <part-bytes> or more: its bytes split
#                                                            in order (cat part1 part2 ... = it)
# and the manifest every installer reads first:
#   provisa-core-images-<arch>-<tag>.sha256                  `sha256sum` lines, one per asset,
#                                                            parts in order
# Then checks every file it wrote is under the limit (.github/actions/check-release-asset-size),
# so a dry run, which uploads nothing to the release, fails here as a release would.
set -euo pipefail

[ $# -eq 5 ] || { echo "usage: $0 <images-dir> <arch> <tag> <out-dir> <part-bytes>" >&2; exit 2; }
images_dir="$1" arch="$2" tag="$3" out_dir="$4" part_bytes="$5"
case "$arch" in
  arm64 | amd64) ;;
  *) echo "unsupported arch '$arch' (arm64 | amd64)" >&2; exit 2 ;;
esac

shopt -s nullglob
tarballs=("$images_dir"/*.tar.gz)
[ ${#tarballs[@]} -gt 0 ] || { echo "no image tarballs (*.tar.gz) in $images_dir" >&2; exit 1; }

mkdir -p "$out_dir"
manifest="provisa-core-images-${arch}-${tag}.sha256"
: > "$out_dir/$manifest"
written=("$out_dir/$manifest")

for src in "${tarballs[@]}"; do
  name="$(basename "$src" .tar.gz)"
  asset="provisa-core-image-${name}-${arch}-${tag}.tar.gz"
  size="$(wc -c < "$src" | tr -d " ")"
  if [ "$size" -lt "$part_bytes" ]; then
    cp "$src" "$out_dir/$asset"
    files=("$asset")
  else
    # split names its pieces <prefix>000, <prefix>001, ...; each is renamed to its plain
    # 1-based part number.
    split -b "$part_bytes" -d -a 3 "$src" "$out_dir/$asset.part"
    files=()
    for piece in "$out_dir/$asset".part[0-9][0-9][0-9]; do
      n=$((10#${piece##*.part} + 1))
      mv "$piece" "$out_dir/$asset.part$n"
      files+=("$asset.part$n")
    done
  fi
  for f in "${files[@]}"; do
    (cd "$out_dir" && sha256sum "$f") >> "$out_dir/$manifest"
    written+=("$out_dir/$f")
  done
done

bash "$(dirname "$0")/../../.github/actions/check-release-asset-size/check.sh" "${written[@]}"
cat "$out_dir/$manifest"
