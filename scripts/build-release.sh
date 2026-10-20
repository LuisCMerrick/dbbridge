#!/usr/bin/env bash
# Builds the release binaries and per-platform packages into ${1:-dist}.
# The output is reproducible for a given source tree, toolchain and
# SOURCE_DATE_EPOCH: binaries are built with -trimpath and no VCS stamping,
# the embedded build date and archive timestamps come from SOURCE_DATE_EPOCH
# (default: the commit time of HEAD when git is available), and archives are
# written with sorted entries and fixed ownership.
set -euo pipefail
cd "$(dirname "$0")/.."
release_dir="${1:-dist}"
mkdir -p "$release_dir"
# Hash every Go source plus module files and the build scripts/workflows;
# plain find avoids a ripgrep dependency and does not depend on .gitignore rules.
source_hash="$(find . \( -path ./.git -o -path "./$release_dir" \) -prune -o -type f \( -name '*.go' -o -name go.mod -o -name go.sum -o -path './scripts/*' -o -path './.github/workflows/*' \) -print0 | LC_ALL=C sort -z | xargs -0 sha256sum | sha256sum | cut -d ' ' -f 1)"
if [ -z "${SOURCE_DATE_EPOCH:-}" ]; then
  SOURCE_DATE_EPOCH="$(git log -1 --format=%ct 2>/dev/null || date -u +%s)"
fi
export SOURCE_DATE_EPOCH
build_date="$(date -u -d "@$SOURCE_DATE_EPOCH" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -r "$SOURCE_DATE_EPOCH" +%Y-%m-%dT%H:%M:%SZ)"
# Without DBB_VERSION the version records the (UTC) source date.
default_version="1.4.$(date -u -d "@$SOURCE_DATE_EPOCH" +%y.%m.%d 2>/dev/null || date -u -r "$SOURCE_DATE_EPOCH" +%y.%m.%d)"
release_version="${DBB_VERSION:-$default_version}"
source_identity="${DBB_COMMIT:-source-$source_hash}"
if [[ ! "$release_version" =~ ^[A-Za-z0-9._+-]+$ || ! "$source_identity" =~ ^[A-Za-z0-9._:+-]+$ ]]; then
  echo 'Invalid release version or source identity' >&2
  exit 1
fi

# Documentation and samples shipped next to every binary.
package_files=()
for f in README* CHANGELOG* LICENSE* NOTICE*; do
  if [ -f "$f" ]; then package_files+=("$f"); fi
done
for d in docs configs deploy; do
  if [ -d "$d" ]; then package_files+=("$d"); fi
done

# third_party_notices writes, for the modules linked into binary $1, a
# module list (third_party/MODULES.txt: path, version, checksum) and each
# module's license and notice files into directory $2.
third_party_notices() {
  local binary="$1" out="$2" dep path version sum dir f
  mkdir -p "$out"
  : > "$out/MODULES.txt"
  go version -m "$binary" | awk '$1 == "dep" { print $2, $3, $4 }' | LC_ALL=C sort | while read -r path version sum; do
    printf '%s %s %s\n' "$path" "$version" "$sum" >> "$out/MODULES.txt"
    if [ -d "vendor/$path" ]; then
      dir="vendor/$path"
    else
      dir="$(go mod download -json "$path@$version" | sed -n 's/^[[:space:]]*"Dir": "\(.*\)",*$/\1/p')"
    fi
    if [ -z "$dir" ] || [ ! -d "$dir" ]; then
      echo "no source for $path@$version to copy its license from" >&2
      return 1
    fi
    dep="$out/$path"
    mkdir -p "$dep"
    for f in "$dir"/LICENSE* "$dir"/COPYING* "$dir"/NOTICE*; do
      if [ -f "$f" ]; then cp "$f" "$dep/"; fi
    done
    if ! ls "$dep" | grep -q .; then
      echo "no license file found for $path@$version" >&2
      return 1
    fi
  done
}

# Absolute output path for commands that run in another directory (zip).
release_dir_abs="$(cd "$release_dir" && pwd)"
staging="$(mktemp -d)"
trap 'rm -rf "$staging"' EXIT
artifacts=()
# Supported release targets: linux/amd64, linux/arm64, windows/amd64.
# DBB_PLATFORMS overrides the target list (space separated GOOS/GOARCH).
for target_platform in ${DBB_PLATFORMS:-linux/amd64 linux/arm64 windows/amd64}; do
  target_os="${target_platform%/*}"
  target_arch="${target_platform#*/}"
  binary_suffix=""
  if [ "$target_os" = windows ]; then binary_suffix=.exe; fi
  binary_name="dbbridge-$target_os-$target_arch$binary_suffix"
  CGO_ENABLED=0 GOOS="$target_os" GOARCH="$target_arch" go build -trimpath -buildvcs=false \
    -ldflags "-s -w -buildid= -X main.Version=$release_version -X main.GitCommit=$source_identity -X main.BuildDate=$build_date" \
    -o "$release_dir/$binary_name" ./cmd/dbbridge
  artifacts+=("$binary_name")

  package_name="dbbridge-$release_version-$target_os-$target_arch"
  root="$staging/$package_name"
  mkdir -p "$root"
  cp "$release_dir/$binary_name" "$root/dbbridge$binary_suffix"
  if [ "${#package_files[@]}" -gt 0 ]; then
    cp -R "${package_files[@]}" "$root/"
  fi
  third_party_notices "$release_dir/$binary_name" "$root/third_party"
  find "$root" -exec touch -h -d "@$SOURCE_DATE_EPOCH" {} + 2>/dev/null || true
  if [ "$target_os" = windows ] && command -v zip >/dev/null 2>&1; then
    archive="$package_name.zip"
    (cd "$staging" && find "$package_name" -print | LC_ALL=C sort | TZ=UTC zip -q -X -D -@ "$release_dir_abs/$archive")
  else
    archive="$package_name.tar.gz"
    tar --sort=name --mtime="@$SOURCE_DATE_EPOCH" --owner=0 --group=0 --numeric-owner \
      -C "$staging" -cf - "$package_name" | gzip -n -9 > "$release_dir/$archive"
  fi
  artifacts+=("$archive")
done
printf 'version=%s\nsource=%s\nsource_sha256=%s\nbuilt_utc=%s\nsource_date_epoch=%s\ntoolchain=%s\n' \
  "$release_version" "$source_identity" "$source_hash" "$build_date" "$SOURCE_DATE_EPOCH" "$(go version)" > "$release_dir/BUILD-INFO.txt"
artifacts+=(BUILD-INFO.txt)
(cd "$release_dir" && sha256sum "${artifacts[@]}" > SHA256SUMS)
