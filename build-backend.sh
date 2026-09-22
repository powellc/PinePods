#!/usr/bin/env bash
# Build the PinePods backend API binary (pinepods-api) locally.
#
# Usage:
#   ./build-backend.sh linux  [amd64|arm64] [version]
#   ./build-backend.sh native               [version]
#
# `linux` uses Docker (rust:alpine) so the result is a static musl binary
# matching the container image. `native` uses the host toolchain (glibc).
# Output: dist/pinepods-api-<version>-<platform> (+ .sha256)
#
# Docker remains the supported way to run PinePods; this is for packaging or
# advanced/non-container use. The binary is the API server only: it still needs
# PostgreSQL/MySQL, Valkey/Redis, a separately served web UI, the container's
# /opt/pinepods + /var/www/html/static paths, and database migrations.
set -euo pipefail

cd "$(dirname "$0")"

usage() {
  cat >&2 <<'EOF'
Usage:
  ./build-backend.sh linux  [amd64|arm64] [version]
  ./build-backend.sh native               [version]

Examples:
  ./build-backend.sh linux amd64
  ./build-backend.sh linux arm64 1.0
  ./build-backend.sh native
EOF
  exit 1
}

MODE="${1:-}"
[ -n "$MODE" ] || usage

RAW_ARCH="${2:-$(uname -m)}"
case "$RAW_ARCH" in
  x86_64|amd64) ARCH=amd64 ;;
  aarch64|arm64) ARCH=arm64 ;;
  *) echo "Unsupported arch: $RAW_ARCH (use amd64 or arm64)" >&2; exit 1 ;;
esac

VERSION="${3:-$(grep -m1 '^version' rust-api/Cargo.toml | sed -E 's/.*"([^"]+)".*/\1/')}"

HOST_ARCH_RAW="$(uname -m)"
case "$HOST_ARCH_RAW" in
  x86_64|amd64) HOST_ARCH=amd64 ;;
  aarch64|arm64) HOST_ARCH=arm64 ;;
  *) HOST_ARCH="$HOST_ARCH_RAW" ;;
esac

case "$MODE" in
  linux)
    PLATFORM="linux-${ARCH}"
    PLATFORM_FLAG=""
    if [ "$ARCH" != "$HOST_ARCH" ]; then
      PLATFORM_FLAG="--platform linux/${ARCH}"
      echo "Note: cross-arch Docker build (${ARCH} on ${HOST_ARCH}) needs QEMU and will be slow." >&2
    fi
    command -v docker >/dev/null 2>&1 || { echo "docker is required for 'linux' builds" >&2; exit 1; }
    mkdir -p dist
    # shellcheck disable=SC2086
    docker run --rm $PLATFORM_FLAG -v "$PWD:/src" -w /src -e OPENSSL_STATIC=1 rust:alpine sh -c '
      set -e
      apk add --no-cache musl-dev pkgconfig openssl-dev openssl-libs-static
      cargo build --release --manifest-path rust-api/Cargo.toml
      strip rust-api/target/release/pinepods-api
      cp rust-api/target/release/pinepods-api /src/dist/pinepods-api
    '
    ;;
  native)
    if [ "$ARCH" != "$HOST_ARCH" ]; then
      echo "'native' builds can only target the host arch ($HOST_ARCH)." >&2
      exit 1
    fi
    PLATFORM="$(uname -s | tr '[:upper:]' '[:lower:]')-${ARCH}"
    mkdir -p dist
    cargo build --release --manifest-path rust-api/Cargo.toml
    if command -v strip >/dev/null 2>&1; then
      strip rust-api/target/release/pinepods-api
    fi
    cp rust-api/target/release/pinepods-api dist/pinepods-api
    ;;
  *)
    usage
    ;;
esac

ARTIFACT="pinepods-api-${VERSION}-${PLATFORM}"
mv dist/pinepods-api "dist/${ARTIFACT}"
(cd dist && sha256sum "${ARTIFACT}" > "${ARTIFACT}.sha256")

echo "Wrote dist/${ARTIFACT}"
