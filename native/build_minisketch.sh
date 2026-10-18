#!/bin/sh
# Build libminisketch (bitcoin-core/minisketch, commit a12f5de) with the bulk helpers, for the
# PinSketch baseline. Usage: native/build_minisketch.sh <path to minisketch checkout>
set -e
SRC=${1:?path to a minisketch checkout}
HERE=$(cd "$(dirname "$0")" && pwd)
cp "$HERE/ms_helper.cpp" "$SRC/src/ms_helper.cpp"
c++ -O2 -std=c++17 -fPIC -shared -DENABLE_FIELD_INT_64 -I"$SRC/include" \
    "$SRC/src/minisketch.cpp" "$SRC"/src/fields/generic_*.cpp "$SRC/src/ms_helper.cpp" \
    -o "$SRC/libminisketch.dylib"
echo "built $SRC/libminisketch.dylib"
