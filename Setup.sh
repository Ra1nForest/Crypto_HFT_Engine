# setup.sh - install all dependencies
# Usage: bash Setup.sh

echo "=== Installing Dependencies ==="
sudo apt update
sudo apt install -y \
    g++ \
    cmake \
    libboost-all-dev \
    libssl-dev

echo ""
echo "=== Downloading simdjson (header-only, only two files needed) ==="
mkdir -p third_party
cd third_party

# Download the single-header version of simdjson
curl -L -o simdjson.h https://raw.githubusercontent.com/simdjson/simdjson/master/singleheader/simdjson.h
curl -L -o simdjson.cpp https://raw.githubusercontent.com/simdjson/simdjson/master/singleheader/simdjson.cpp

cd ..

echo ""
echo "=== Dependencies installed successfully ==="
echo ""
echo "Next steps:"
echo "  mkdir build && cd build"
echo "  cmake .."
echo "  make"
echo "  ./crypto_hft_engine"