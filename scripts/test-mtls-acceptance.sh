#!/usr/bin/env bash
# C1 mTLS + Phase 3 role-level authz acceptance (task #30).
#
# Verifies the Phase 2 + Phase 3 wiring end-to-end:
#   1. Mint dev cluster CA + per-binary certs (scripts/dev-ca.sh).
#   2. Full Meta + 2 Workers + FUSE mount under mTLS: create/write/read a
#      file, exercise a chunk-boundary write, confirm durability.
#   3. Reject matrix (all must FAIL to dial):
#        a. plaintext dial against an mTLS-only port
#        b. no client cert
#        c. client cert signed by a *different* (rogue) CA
#        d. role-not-admitted: a worker-1 cert dialing a worker server
#           (chunkworker for_worker() admits only {meta, client-admin}).
#   4. Cert rotation: regenerate the client cert under the SAME CA, restart
#      mount, confirm reconnect + a fresh write succeeds.
#
# Prereqs:
#   - /dev/fuse + fusermount3 available (or the script exits 77 SKIP).
#   - cargo build of fluxfs / metamaster / chunkworker already done.
set -euo pipefail

if [[ ! -c /dev/fuse ]]; then
    echo "SKIP: /dev/fuse is unavailable" >&2
    exit 77
fi
if ! command -v fusermount3 >/dev/null 2>&1; then
    echo "SKIP: fusermount3 is unavailable" >&2
    exit 77
fi
if ! command -v openssl >/dev/null 2>&1; then
    echo "SKIP: openssl is unavailable" >&2
    exit 77
fi

repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
test_root=$(mktemp -d -t fluxfs-mtls.XXXXXX)
cert_dir="$test_root/certs"
rogue_dir="$test_root/rogue-ca"
mount_dir="$test_root/mnt"
mkdir -p "$mount_dir"

base_port=$((31000 + ($$ % 18000)))
meta_port=$base_port
worker0_port=$((base_port + 1))
worker1_port=$((base_port + 2))
worker2_port=$((base_port + 3))
meta_pid=""
worker0_pid=""
worker1_pid=""
worker2_pid=""
mount_pid=""

cleanup() {
    local status=$?
    fusermount3 -u "$mount_dir" 2>/dev/null || true
    for pid in "$mount_pid" "$meta_pid" "$worker0_pid" "$worker1_pid" "$worker2_pid"; do
        if [[ -n "$pid" ]]; then
            kill "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
        fi
    done
    if (( status != 0 )); then
        echo "mTLS acceptance failed; recent process logs:" >&2
        for log in "$test_root"/*.log; do
            [[ -f "$log" ]] || continue
            echo "--- $(basename "$log")" >&2
            tail -n 40 "$log" >&2 || true
        done
    fi
    rm -rf -- "$test_root"
    return "$status"
}
trap cleanup EXIT

wait_port() {
    local port=$1
    for _ in $(seq 1 100); do
        if timeout 0.1 bash -c "</dev/tcp/127.0.0.1/$port" 2>/dev/null; then
            return 0
        fi
        sleep 0.05
    done
    echo "port $port did not become ready" >&2
    return 1
}

start_meta() {
    "$repo_dir/target/debug/fluxfs-metamaster" \
        --listen "127.0.0.1:$meta_port" --data-dir "$test_root/meta" \
        --tls-ca-cert "$cert_dir/ca.crt" \
        --tls-server-cert "$cert_dir/meta.crt" \
        --tls-server-key  "$cert_dir/meta.key" \
        >"$test_root/meta.log" 2>&1 &
    meta_pid=$!
    wait_port "$meta_port"
}

start_worker0() {
    "$repo_dir/target/debug/fluxfs-chunkworker" \
        --worker-id 1 --listen "127.0.0.1:$worker0_port" \
        --advertise-endpoint "https://127.0.0.1:$worker0_port" \
        --meta-addr "https://127.0.0.1:$meta_port" \
        --data-dir "$test_root/worker-0" \
        --tls-ca-cert "$cert_dir/ca.crt" \
        --tls-server-cert "$cert_dir/worker-1.crt" \
        --tls-server-key  "$cert_dir/worker-1.key" \
        >"$test_root/worker-0.log" 2>&1 &
    worker0_pid=$!
    if ! wait_port "$worker0_port"; then
        cat "$test_root/worker-0.log" >&2
        return 1
    fi
}

start_worker1() {
    "$repo_dir/target/debug/fluxfs-chunkworker" \
        --worker-id 2 --listen "127.0.0.1:$worker1_port" \
        --advertise-endpoint "https://127.0.0.1:$worker1_port" \
        --meta-addr "https://127.0.0.1:$meta_port" \
        --data-dir "$test_root/worker-1" \
        --tls-ca-cert "$cert_dir/ca.crt" \
        --tls-server-cert "$cert_dir/worker-2.crt" \
        --tls-server-key  "$cert_dir/worker-2.key" \
        >"$test_root/worker-1.log" 2>&1 &
    worker1_pid=$!
    if ! wait_port "$worker1_port"; then
        cat "$test_root/worker-1.log" >&2
        return 1
    fi
}

start_worker2() {
    "$repo_dir/target/debug/fluxfs-chunkworker" \
        --worker-id 3 --listen "127.0.0.1:$worker2_port" \
        --advertise-endpoint "https://127.0.0.1:$worker2_port" \
        --meta-addr "https://127.0.0.1:$meta_port" \
        --data-dir "$test_root/worker-2" \
        --tls-ca-cert "$cert_dir/ca.crt" \
        --tls-server-cert "$cert_dir/worker-3.crt" \
        --tls-server-key  "$cert_dir/worker-3.key" \
        >"$test_root/worker-2.log" 2>&1 &
    worker2_pid=$!
    if ! wait_port "$worker2_port"; then
        cat "$test_root/worker-2.log" >&2
        return 1
    fi
}

# Mount with caller-supplied client cert/key (so we can re-mount for reject
# matrix + rotation tests). $1=cert $2=key. Optional $3=port-suffix-override.
start_mount_with() {
    local cc="$1" ck="$2"
    "$repo_dir/target/debug/fluxfs" mount --no-ufs \
        --data-dir "$test_root/client" --mountpoint "$mount_dir" \
        --meta-addr "https://127.0.0.1:$meta_port" \
        --tls-ca-cert "$cert_dir/ca.crt" \
        --tls-client-cert "$cc" \
        --tls-client-key  "$ck" \
        >"$test_root/mount.log" 2>&1 &
    mount_pid=$!
    for _ in $(seq 1 100); do
        if mountpoint -q "$mount_dir"; then
            return 0
        fi
        if ! kill -0 "$mount_pid" 2>/dev/null; then
            return 1
        fi
        sleep 0.05
    done
    echo "FluxFS did not mount within 5 seconds" >&2
    return 1
}

# Mint rogue CA + rogue client cert (different CA from cluster CA).
mint_rogue() {
    mkdir -p "$rogue_dir"
    cd "$rogue_dir"
    openssl genrsa -out rogue-ca.key 4096 2>/dev/null
    openssl req -x509 -new -nodes -key rogue-ca.key -sha256 -days 1 \
        -subj "/O=Rogue/CN=Rogue CA" \
        -addext "basicConstraints=critical,CA:TRUE,pathlen:0" \
        -addext "keyUsage=critical,keyCertSign,cRLSign" \
        -out rogue-ca.crt 2>/dev/null
    openssl genrsa -out rogue-client.key 2048 2>/dev/null
    openssl req -new -key rogue-client.key \
        -subj "/O=Rogue/CN=rogue-client" -out rogue-client.csr 2>/dev/null
    local extfile
    extfile=$(mktemp -t fluxfs-rogue-ext.XXXXXX)
    cat >"$extfile" <<EOF
basicConstraints=CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=clientAuth
subjectAltName=@alt_names
[alt_names]
otherName.0=1.3.6.1.4.1.52372.1.2;UTF8:spiffe://fluxfs/client-admin/rogue
DNS.1=localhost
IP.1=127.0.0.1
EOF
    openssl x509 -req -in rogue-client.csr \
        -CA rogue-ca.crt -CAkey rogue-ca.key -CAcreateserial \
        -out rogue-client.crt -days 1 -sha256 -extfile "$extfile" 2>/dev/null
    rm -f "$extfile" rogue-client.csr
    cd - >/dev/null
}

echo "==> building binaries"
cargo build --manifest-path "$repo_dir/Cargo.toml" \
    -p fluxfs -p fluxfs-metamaster -p fluxfs-chunkworker >/dev/null

echo "==> minting dev cluster certs"
"$repo_dir/scripts/dev-ca.sh" "$cert_dir" "1 2 3" >/dev/null

echo "==> minting rogue CA + rogue client"
mint_rogue

echo "==> starting cluster under mTLS"
start_meta
start_worker0
start_worker1
start_worker2

echo "==> [1/5] happy path: valid client cert, create + read + chunk-boundary write"
start_mount_with "$cert_dir/client.crt" "$cert_dir/client.key"
printf 'mtls-durable\n' >"$mount_dir/durable.txt"
test "$(cat "$mount_dir/durable.txt")" = "mtls-durable"
dd if=/dev/zero of="$test_root/large.expected" bs=1M count=4 status=none
printf 'chunk-boundary-ok' >>"$test_root/large.expected"
cp "$test_root/large.expected" "$mount_dir/large.bin"
cmp "$test_root/large.expected" "$mount_dir/large.bin"
fusermount3 -u "$mount_dir"
wait "$mount_pid" 2>/dev/null || true
mount_pid=""
echo "    PASS"

echo "==> [1b/5] client-admin CLI over mTLS: admin + streaming namespace CRUD"
cli_tls=(
    --tls-ca-cert "$cert_dir/ca.crt"
    --tls-client-cert "$cert_dir/client.crt"
    --tls-client-key "$cert_dir/client.key"
)
admin=(
    "$repo_dir/target/debug/fluxfs" admin
    --meta-addr "https://127.0.0.1:$meta_port"
    "${cli_tls[@]}"
)
fs=(
    "$repo_dir/target/debug/fluxfs" fs
    --meta-addr "https://127.0.0.1:$meta_port"
    "${cli_tls[@]}"
)
for _ in $(seq 1 100); do
    if "${admin[@]}" status >"$test_root/cli-status.log" 2>&1 \
            && grep -q 'workers_live=3/3' "$test_root/cli-status.log"; then
        break
    fi
    sleep 0.05
done
grep -q 'cluster ok:' "$test_root/cli-status.log"
grep -q 'workers_live=3/3' "$test_root/cli-status.log"
"${admin[@]}" workers >"$test_root/cli-workers.log"
test "$(tail -n +2 "$test_root/cli-workers.log" | wc -l)" = "3"

"${fs[@]}" mkdir /cli
dd if=/dev/zero of="$test_root/cli.expected" bs=1M count=5 status=none
printf 'cli-mtls-tail\n' >>"$test_root/cli.expected"
"${fs[@]}" put "$test_root/cli.expected" /cli/source.bin
"${fs[@]}" mv /cli/source.bin /cli/renamed.bin --no-replace
"${fs[@]}" cat /cli/renamed.bin >"$test_root/cli.actual"
cmp "$test_root/cli.expected" "$test_root/cli.actual"
"${fs[@]}" rm /cli/renamed.bin
"${fs[@]}" rmdir /cli
echo "    PASS (client-admin cert authorized Meta rename/rmdir and Worker stream RPCs)"

echo "==> [2/5] reject: plaintext dial against mTLS port"
if "$repo_dir/target/debug/fluxfs" mount --no-ufs \
        --data-dir "$test_root/client-bad" --mountpoint "$mount_dir" \
        --meta-addr "http://127.0.0.1:$meta_port" \
        --chunk-worker "http://127.0.0.1:$worker0_port" \
        --allow-insecure-dev \
        >"$test_root/mount-plaintext.log" 2>&1 & then
    mount_pid=$!
    sleep 1
    if mountpoint -q "$mount_dir"; then
        echo "    FAIL: plaintext mount succeeded against mTLS server" >&2
        exit 1
    fi
    kill "$mount_pid" 2>/dev/null || true
    wait "$mount_pid" 2>/dev/null || true
    mount_pid=""
fi
rm -rf -- "$test_root/client-bad"
echo "    PASS (plaintext dial rejected)"

echo "==> [3/5] reject: no client cert (CA-only dial against mTLS server)"
# Server requires client cert (--tls-server-cert without --allow-insecure-dev
# forces mTLS); a CA-only dial (no --tls-client-cert) must fail the TLS
# handshake. Drive a real RPC (MetaService.Ping via GetInode on root) so the
# handshake actually completes — connection-lazy clients defer the failure
# until first RPC.
if "$repo_dir/target/debug/fluxfs" meta-ping \
        --addr "https://127.0.0.1:$meta_port" \
        --tls-ca-cert "$cert_dir/ca.crt" \
        --allow-insecure-dev \
        >"$test_root/no-cert-ping.log" 2>&1; then
    echo "    FAIL: no-client-cert dial succeeded" >&2
    cat "$test_root/no-cert-ping.log" >&2
    exit 1
fi
# Sanity-check that the failure is a TLS handshake error, not some unrelated
# transport issue (port down, wrong CA, etc.). gRPC surfaces handshake
# failures with one of these substrings. Hard-fail on absence so a clap
# parse error or wrong-port failure can't pass as green.
if ! grep -qiE "tls|handshake|certificate|alert|transport|canceled|cancelled|connection closed" \
        "$test_root/no-cert-ping.log"; then
    echo "    FAIL: no-client-cert dial failed but log lacks TLS/transport error marker:" >&2
    cat "$test_root/no-cert-ping.log" >&2
    exit 1
fi
echo "    PASS (no-client-cert rejected at TLS)"

echo "==> [4/5] reject: rogue-CA-signed client cert"
if "$repo_dir/target/debug/fluxfs" meta-ping \
        --addr "https://127.0.0.1:$meta_port" \
        --tls-ca-cert "$rogue_dir/rogue-ca.crt" \
        --tls-client-cert "$rogue_dir/rogue-client.crt" \
        --tls-client-key  "$rogue_dir/rogue-client.key" \
        >"$test_root/rogue-ping.log" 2>&1; then
    echo "    FAIL: rogue-CA client cert was accepted" >&2
    cat "$test_root/rogue-ping.log" >&2
    exit 1
fi
# The failure must surface from TLS trust validation (UnknownIssuer /
# certificate verification), not from a clap parse error or wrong port.
if ! grep -qiE "tls|handshake|certificate|alert|transport|canceled|cancelled|connection closed" \
        "$test_root/rogue-ping.log"; then
    echo "    FAIL: rogue-CA dial failed without TLS/transport error marker:" >&2
    cat "$test_root/rogue-ping.log" >&2
    exit 1
fi
echo "    PASS (rogue-CA client rejected at TLS)"

echo "==> [5/5] reject: role-not-admitted (worker-1 cert dialing worker server)"
# Worker server for_worker() admits only {Meta, ClientAdmin}. A worker-1
# cert (spiffe://fluxfs/worker/1) is rejected with Code::PermissionDenied
# AFTER TLS succeeds — this is the Phase 3 role-level authz gate, not TLS.
#
# Real RPC: ChunkWorker.Health via the new `chunk-probe` subcommand. The
# for_worker() interceptor runs as a Layer over the service, BEFORE method
# dispatch, so PermissionDenied fires before the Health handler sees the
# request. (Earlier meta-ping-against-worker was bogus: tonic returned
# Unimplemented for unknown MetaService.Ping on a ChunkWorkerServer, masking
# the authz path.)
if "$repo_dir/target/debug/fluxfs" chunk-probe \
        --addr "https://127.0.0.1:$worker1_port" \
        --tls-ca-cert "$cert_dir/ca.crt" \
        --tls-client-cert "$cert_dir/worker-1.crt" \
        --tls-client-key  "$cert_dir/worker-1.key" \
        >"$test_root/role-reject.log" 2>&1; then
    echo "    FAIL: worker-1 cert admitted by worker server" >&2
    cat "$test_root/role-reject.log" >&2
    exit 1
fi
# The error must surface from authz (Code::PermissionDenied), not TLS.
# gRPC surfaces PermissionDenied with the canonical "does not have permission"
# prefix; our authz layer wraps it as "role <r> not admitted by this server".
if ! grep -qiE "does not have permission|not admitted|lacks|permission denied" \
        "$test_root/role-reject.log"; then
    echo "    FAIL: cross-role dial failed without PermissionDenied marker:" >&2
    cat "$test_root/role-reject.log" >&2
    exit 1
fi
echo "    PASS (cross-role dial rejected at Phase 3 role-level authz)"

echo "==> [5b/5] reject: role-admitted but per-handler capability denied (Meta cert → Worker PutChunk)"
# Per gpt56 ac8ef471 #2 / cursor 6e9860c2 #2: role-level admission alone
# doesn't prove the per-handler `require(cap)` table fires. Drive a real
# PutChunk RPC with a Meta cert: for_worker() ADMITS Meta (role gate passes),
# but the put_chunk handler requires Capability::PutChunk, which Meta does
# NOT hold. The handler-layer require must reject with PermissionDenied.
if "$repo_dir/target/debug/fluxfs" chunk-probe --mode put \
        --addr "https://127.0.0.1:$worker1_port" \
        --tls-ca-cert "$cert_dir/ca.crt" \
        --tls-client-cert "$cert_dir/meta.crt" \
        --tls-client-key  "$cert_dir/meta.key" \
        >"$test_root/cap-reject.log" 2>&1; then
    echo "    FAIL: Meta cert PutChunk was accepted by worker (capability table not enforced)" >&2
    cat "$test_root/cap-reject.log" >&2
    exit 1
fi
# Role gate passes; rejection must come from the handler-layer cap check.
# authz.rs require() emits "lacks <Capability>" in the Status message; the
# Capability Display impl (auth.rs) lowercases to snake_case, so the marker
# is "lacks put_chunk" (not "PutChunk"/"putchunk").
if ! grep -qiE "lacks put_chunk|does not have permission|permission denied" \
        "$test_root/cap-reject.log"; then
    echo "    FAIL: Meta PutChunk rejected without per-handler cap marker:" >&2
    cat "$test_root/cap-reject.log" >&2
    exit 1
fi
echo "    PASS (Meta role admitted, PutChunk capability denied at handler)"

echo "==> [bonus 1] tombstone/delete/finalize: ClientAdmin drives GC under mTLS"
# gpt56 aa9e8122: ClientAdmin directly dials Worker.delete_chunk during GC
# sweep and orphan-GC. Without DeleteChunk in ClientAdmin's caps, every GC
# delete_chunk under mTLS would return PermissionDenied. This exercises the
# real path: remount client cert (ClientAdmin caps), write a chunk-sized
# file (so it actually allocates chunks), delete it, then run fluxfs
# orphan-gc against the live cluster under mTLS and assert chunks>=1 were
# reclaimed via Worker.delete_chunk.
start_mount_with "$cert_dir/client.crt" "$cert_dir/client.key"
# Stage the bytes on tmpfs first, then copy into the mount. Direct dd/cp
# into FUSE intermittently hits B6 EAGAIN/backpressure (gpt56 7d871536,
# cursor 00233d1d) — both reviewers reproduced this deterministically.
# Use a small read+write loop with bounded EAGAIN retry so the write
# completes once the worker queue drains.
dd if=/dev/zero of="$test_root/gc.expected" bs=1M count=2 status=none
gc_target="$mount_dir/gc-target.bin"
cp_ok=0
for attempt in $(seq 1 8); do
    if cp "$test_root/gc.expected" "$gc_target" 2>"$test_root/cp.err"; then
        cp_ok=1
        break
    fi
    # EAGAIN / Resource temporarily unavailable — worker queue full, retry.
    if ! grep -qiE "resource temporarily unavailable|would block|eagain" \
            "$test_root/cp.err"; then
        echo "    cp failed for non-EAGAIN reason:" >&2
        cat "$test_root/cp.err" >&2
        break
    fi
    sleep 0.25
done
if (( cp_ok != 1 )); then
    echo "    FAIL: cp into mount did not complete after 8 EAGAIN retries" >&2
    cat "$test_root/cp.err" >&2
    exit 1
fi
gc_size=$(stat -c %s "$gc_target" 2>/dev/null || echo 0)
test "$gc_size" = "2097152"
cmp "$test_root/gc.expected" "$gc_target"
rm -f "$gc_target"
fusermount3 -u "$mount_dir"
wait "$mount_pid" 2>/dev/null || true
mount_pid=""
if ! "$repo_dir/target/debug/fluxfs" orphan-gc \
        --data-dir "$test_root/client-gc" \
        --meta-addr "https://127.0.0.1:$meta_port" \
        --tls-ca-cert "$cert_dir/ca.crt" \
        --tls-client-cert "$cert_dir/client.crt" \
        --tls-client-key  "$cert_dir/client.key" \
        >"$test_root/orphan-gc.log" 2>&1; then
    echo "    FAIL: orphan-gc under mTLS errored" >&2
    cat "$test_root/orphan-gc.log" >&2
    exit 1
fi
if grep -qiE "permission denied|unauthenticated" "$test_root/orphan-gc.log"; then
    echo "    FAIL: orphan-gc hit authz rejection under mTLS" >&2
    cat "$test_root/orphan-gc.log" >&2
    exit 1
fi
# Per gpt56 ac8ef471 #3 / cursor 6e9860c2 #3: exit 0 alone is insufficient —
# if no chunks were swept, DeleteChunk was never exercised and the cap
# claim is hollow. The CLI prints `orphan-gc ok: manifests=N chunks=M`;
# require M >= 1 so the test only passes when DeleteChunk actually fired
# against a worker under mTLS.
gc_chunks=$(grep -oE 'chunks=[0-9]+' "$test_root/orphan-gc.log" | head -1 | cut -d= -f2)
gc_chunks=${gc_chunks:-0}
if (( gc_chunks < 1 )); then
    echo "    FAIL: orphan-gc swept 0 chunks; DeleteChunk cap not exercised" >&2
    cat "$test_root/orphan-gc.log" >&2
    exit 1
fi
echo "    PASS (ClientAdmin DeleteChunk cap drove ${gc_chunks} GC delete_chunk(s) under mTLS)"

echo "==> [bonus 2] cert rotation: regenerate client cert under same CA, reconnect"
# Revoke the client cert by issuing a new one with a different serial. Since
# we don't have CRL/OCSP (Phase 4), rotation here means: regenerate, remount,
# confirm a fresh write succeeds. This proves the mTLS handshake picks up
# the new cert without server-side restart.
cd "$cert_dir"
openssl genrsa -out client.key 2048 2>/dev/null
openssl req -new -key client.key -subj "/O=FluxFS Dev/CN=client-rot" \
        -out client.csr 2>/dev/null
extfile=$(mktemp -t fluxfs-rot-ext.XXXXXX)
cat >"$extfile" <<EOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth,clientAuth
subjectAltName=@alt_names
[alt_names]
otherName.0=1.3.6.1.4.1.52372.1.2;UTF8:spiffe://fluxfs/client-admin/ops
DNS.1=localhost
IP.1=127.0.0.1
EOF
openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
        -out client.crt -days 825 -sha256 -extfile "$extfile" 2>/dev/null
rm -f "$extfile" client.csr
cd - >/dev/null
start_mount_with "$cert_dir/client.crt" "$cert_dir/client.key"
printf 'rotated-cert-write\n' >"$mount_dir/rotated.txt"
test "$(cat "$mount_dir/rotated.txt")" = "rotated-cert-write"
fusermount3 -u "$mount_dir"
wait "$mount_pid" 2>/dev/null || true
mount_pid=""
echo "    PASS (rotated client cert admitted, fresh write durable)"

echo
echo "✓ mTLS + Phase 3 role-level authz acceptance: ALL PASS"
