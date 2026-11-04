#!/usr/bin/env bash
# Setup.sh - install all build dependencies and download simdjson
# Usage: bash Setup.sh
set -euo pipefail

# simdjson release to download (single-header build); bump deliberately
SIMDJSON_VERSION="v5.1.1"

echo "=== Installing Dependencies ==="
sudo apt update
sudo apt install -y \
    g++ \
    cmake \
    libboost-all-dev \
    libssl-dev \
    ca-certificates \
    curl

echo ""
echo "=== Downloading simdjson ${SIMDJSON_VERSION} (only two files needed) ==="
mkdir -p third_party
base="https://raw.githubusercontent.com/simdjson/simdjson/${SIMDJSON_VERSION}/singleheader"
curl -fsSL -o third_party/simdjson.h   "${base}/simdjson.h"
curl -fsSL -o third_party/simdjson.cpp "${base}/simdjson.cpp"

echo ""
echo "=== Dependencies installed successfully ==="
echo ""
echo "Next steps (from the repository root):"
echo "  cmake -S . -B build && cmake --build build -j"
echo "  ctest --test-dir build --output-on-failure"
echo "  ./build/crypto_hft_engine        # run from the repo root so config.json and logs/ resolve"
