#!/usr/bin/env bash
# Count framed chunk records in a Worker pack data dir (segments/*.dat).
set -euo pipefail
root=${1:?worker data dir}
python3 - "$root" <<'PY'
import pathlib, struct, sys
magic = 0x46584B31
root = pathlib.Path(sys.argv[1]) / "segments"
n = 0
if root.is_dir():
    for path in sorted(root.glob("seg-*.dat")):
        data = path.read_bytes()
        i = 0
        while i + 40 <= len(data):
            m, length = struct.unpack_from("<II", data, i)
            if m != magic:
                break
            i += 40 + length
            n += 1
print(n)
PY
