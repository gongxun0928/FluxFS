#!/usr/bin/env bash
set -euo pipefail

if [[ ! -c /dev/fuse ]]; then
    echo "SKIP: /dev/fuse is unavailable" >&2
    exit 77
fi
if ! command -v fusermount3 >/dev/null 2>&1; then
    echo "SKIP: fusermount3 is unavailable" >&2
    exit 77
fi

repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
test_root=$(mktemp -d -t fluxfs-mount-test.XXXXXX)
data_dir="$test_root/data"
mount_dir="$test_root/mnt"
mount_pid=""

cleanup() {
    if mountpoint -q "$mount_dir" 2>/dev/null; then
        fusermount3 -u "$mount_dir" || true
    fi
    if [[ -n "$mount_pid" ]]; then
        wait "$mount_pid" 2>/dev/null || true
    fi
    rm -rf -- "$test_root"
}
trap cleanup EXIT

mkdir -p "$data_dir" "$mount_dir"
cargo build --manifest-path "$repo_dir/Cargo.toml" -p fluxfs

start_mount() {
    "$repo_dir/target/debug/fluxfs" mount \
        --no-ufs \
        --data-dir "$data_dir" \
        --mountpoint "$mount_dir" &
    mount_pid=$!
    for _ in $(seq 1 100); do
        if mountpoint -q "$mount_dir"; then
            return 0
        fi
        if ! kill -0 "$mount_pid" 2>/dev/null; then
            wait "$mount_pid"
            return 1
        fi
        sleep 0.05
    done
    echo "FluxFS did not mount within 5 seconds" >&2
    return 1
}

stop_mount() {
    fusermount3 -u "$mount_dir"
    wait "$mount_pid"
    mount_pid=""
}

start_mount
printf 'hello fluxfs\n' >"$mount_dir/hello.txt"
mkdir "$mount_dir/dir"
printf 'nested\n' >"$mount_dir/dir/nested.txt"
printf 'remove me\n' >"$mount_dir/remove.txt"
test "$(cat "$mount_dir/hello.txt")" = "hello fluxfs"
test "$(cat "$mount_dir/dir/nested.txt")" = "nested"
printf 'XY' | dd of="$mount_dir/hello.txt" bs=1 seek=6 conv=notrunc status=none
test "$(cat "$mount_dir/hello.txt")" = "hello XYuxfs"
truncate -s 8 "$mount_dir/hello.txt"
test "$(cat "$mount_dir/hello.txt")" = "hello XY"

# Core POSIX file-descriptor path: pwrite, ftruncate sparse growth, fchmod,
# futimens/utime, fdatasync, and close/flush must all succeed and persist.
python3 - "$mount_dir/posix.bin" <<'PY'
import os, stat, sys

path = sys.argv[1]
fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o644)
try:
    assert os.write(fd, b"abcdefgh") == 8
    assert os.pwrite(fd, b"XY", 2) == 2
    os.ftruncate(fd, 12)
    os.fchmod(fd, 0o600)
    os.utime(fd, ns=(1_234_567_890_000_000_000, 1_234_567_890_000_000_000))
    os.fdatasync(fd)
finally:
    os.close(fd)

st = os.stat(path)
assert stat.S_IMODE(st.st_mode) == 0o600
assert st.st_size == 12
assert int(st.st_mtime) == 1_234_567_890
assert open(path, "rb").read() == b"abXYefgh" + b"\0" * 4
PY

# Atomic namespace operations through real FUSE callbacks.
mkdir "$mount_dir/rename-left" "$mount_dir/rename-right"
printf 'rename-source\n' >"$mount_dir/rename-left/source"
mv "$mount_dir/rename-left/source" "$mount_dir/rename-right/moved"
test ! -e "$mount_dir/rename-left/source"
test "$(cat "$mount_dir/rename-right/moved")" = "rename-source"
printf 'replacement\n' >"$mount_dir/rename-right/replacement"
mv "$mount_dir/rename-right/moved" "$mount_dir/rename-right/replacement"
test "$(cat "$mount_dir/rename-right/replacement")" = "rename-source"
mkdir "$mount_dir/rename-left/tree"
printf 'child\n' >"$mount_dir/rename-left/tree/child"
if rmdir "$mount_dir/rename-left/tree" 2>/dev/null; then
    echo "rmdir unexpectedly removed a non-empty directory" >&2
    exit 1
fi
rm "$mount_dir/rename-left/tree/child"
rmdir "$mount_dir/rename-left/tree"
mkdir "$mount_dir/rename-left/move-tree"
mv "$mount_dir/rename-left/move-tree" "$mount_dir/rename-right/move-tree"
rmdir "$mount_dir/rename-right/move-tree"
rmdir "$mount_dir/rename-left"
rm "$mount_dir/remove.txt"
test ! -e "$mount_dir/remove.txt"

# Managed hardlink/symlink and xattr/ACL semantics through real kernel FUSE.
printf 'linked-data\n' >"$mount_dir/hard-source"
ln "$mount_dir/hard-source" "$mount_dir/hard-alias"
test "$(stat -c '%i' "$mount_dir/hard-source")" = "$(stat -c '%i' "$mount_dir/hard-alias")"
test "$(stat -c '%h' "$mount_dir/hard-source")" = "2"
rm "$mount_dir/hard-source"
test "$(cat "$mount_dir/hard-alias")" = "linked-data"
ln -s ../hard-alias "$mount_dir/dir/relative-link"
test "$(readlink "$mount_dir/dir/relative-link")" = "../hard-alias"
test "$(cat "$mount_dir/dir/relative-link")" = "linked-data"

python3 - "$mount_dir/hard-alias" <<'PY'
import errno, os, sys

path = sys.argv[1]
os.setxattr(path, "user.fluxfs", b"xattr-value", os.XATTR_CREATE)
assert os.getxattr(path, "user.fluxfs") == b"xattr-value"
assert "user.fluxfs" in os.listxattr(path)
try:
    os.setxattr(path, "user.fluxfs", b"duplicate", os.XATTR_CREATE)
    raise AssertionError("XATTR_CREATE unexpectedly replaced existing value")
except OSError as error:
    assert error.errno == errno.EEXIST
os.setxattr(path, "user.fluxfs", b"replacement", os.XATTR_REPLACE)
try:
    os.setxattr(path, "user.missing", b"missing", os.XATTR_REPLACE)
    raise AssertionError("XATTR_REPLACE unexpectedly created missing value")
except OSError as error:
    assert error.errno == errno.ENODATA
PY

setfacl -m u:12345:r-- "$mount_dir/hard-alias"
getfacl -cn "$mount_dir/hard-alias" | grep -q '^user:12345:r--$'
setfacl -d -m u:12345:r-x "$mount_dir/dir"
printf 'acl-child\n' >"$mount_dir/dir/acl-child"
getfacl -cn "$mount_dir/dir/acl-child" | grep -q '^user:12345:r-x$'
mkdir "$mount_dir/dir/acl-child-dir"
getfacl -cn "$mount_dir/dir/acl-child-dir" | grep -q '^user:12345:r-x$'
getfacl -cdn "$mount_dir/dir/acl-child-dir" | grep -q '^user:12345:r-x$'
stop_mount

# Reopen the same metadata/chunk directories and verify acknowledged data.
start_mount
test "$(cat "$mount_dir/hello.txt")" = "hello XY"
test "$(cat "$mount_dir/dir/nested.txt")" = "nested"
test ! -e "$mount_dir/remove.txt"
python3 - "$mount_dir/posix.bin" <<'PY'
import os, stat, sys

path = sys.argv[1]
st = os.stat(path)
assert stat.S_IMODE(st.st_mode) == 0o600
assert st.st_size == 12
assert int(st.st_mtime) == 1_234_567_890
assert open(path, "rb").read() == b"abXYefgh" + b"\0" * 4
PY
test "$(cat "$mount_dir/rename-right/replacement")" = "rename-source"
test "$(cat "$mount_dir/hard-alias")" = "linked-data"
test "$(readlink "$mount_dir/dir/relative-link")" = "../hard-alias"
test "$(cat "$mount_dir/dir/relative-link")" = "linked-data"
python3 - "$mount_dir/hard-alias" <<'PY'
import os, sys

path = sys.argv[1]
assert os.getxattr(path, "user.fluxfs") == b"replacement"
assert "system.posix_acl_access" in os.listxattr(path)
PY
getfacl -cn "$mount_dir/hard-alias" | grep -q '^user:12345:r--$'
getfacl -cn "$mount_dir/dir/acl-child" | grep -q '^user:12345:r-x$'
getfacl -cn "$mount_dir/dir/acl-child-dir" | grep -q '^user:12345:r-x$'
getfacl -cdn "$mount_dir/dir/acl-child-dir" | grep -q '^user:12345:r-x$'
rm "$mount_dir/rename-right/replacement"
rmdir "$mount_dir/rename-right"
printf '\nrestart-ok\n' >>"$mount_dir/hello.txt"
test "$(tail -n 1 "$mount_dir/hello.txt")" = "restart-ok"
stop_mount

echo "local mount + restart: ok"
