#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
target="${1:-android}"
if ! command -v gomobile >/dev/null 2>&1; then
  echo 'gomobile is not installed. Install golang.org/x/mobile/cmd/gomobile on a trusted build host and run gomobile init.' >&2
  exit 2
fi
mkdir -p build/mobile
case "$target" in
  android)
    if ! command -v javac >/dev/null 2>&1; then echo 'Android JDK required' >&2; exit 2; fi
    gomobile bind -target android -androidapi 24 -o build/mobile/Mira.aar ./mobile
    ;;
  ios)
    if [[ "$(uname -s)" != Darwin ]]; then echo 'iOS framework generation requires macOS and the Xcode toolchain' >&2; exit 2; fi
    xcodebuild -version >/dev/null
    gomobile bind -target ios -o build/mobile/Mira.xcframework ./mobile
    ;;
  *) echo 'usage: build-mobile.sh android|ios' >&2; exit 2;;
esac
