#!/usr/bin/env bash
# Dev-only cluster CA + per-binary mTLS certs for FluxFS (task #30 C1 Phase 2).
#
# Generates:
#   ca.crt / ca.key                         — cluster CA (trust anchor)
#   meta.crt / meta.key                     — MetaMaster identity
#                                                spiffe://fluxfs/meta/g0
#   worker-<id>.crt / worker-<id>.key       — ChunkWorker identities
#                                                spiffe://fluxfs/worker/<id>
#   client.crt / client.key                 — privileged admin client identity
#                                                spiffe://fluxfs/client-admin/ops
#
# Every leaf cert carries:
#   - subjectAltName = otherName:<SPIFFE URI OID>;UTF8:<spiffe uri>
#                     + DNS:localhost + IP:127.0.0.1 (dev loopback dialing)
#   - extendedKeyUsage = serverAuth, clientAuth  (so each cert works in either
#     role — Meta is both server and Raft client; workers are server for chunk
#     puts and client of Meta for reservations)
#
# USAGE
#   scripts/dev-ca.sh [out_dir] [worker_ids]
#   scripts/dev-ca.sh                         # ./target/dev-certs default, workers 0 1 2
#   scripts/dev-ca.sh /tmp/certs "0 1 2 3"    # custom path + 4 workers
#
# This is a DEV helper. Production MUST use a real PKI (cert-manager / Vault /
# cfssl / an internal CA) with per-binary CSRs, short-lived leaf certs, CRL /
# OCSP endpoints, and a separate client-CA from server-CA. See task #30
# Phase 4 (rotation/CRL/audit — not yet implemented).
set -euo pipefail

out_dir="${1:-$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)/target/dev-certs}"
worker_ids_str="${2:-0 1 2}"
readonly SPIFFE_OID="1.3.6.1.4.1.52372.1.2"

mkdir -p "$out_dir"
cd "$out_dir"

# Reuse the CA across runs if it already exists (lets you mint new leaf certs
# without breaking already-issued ones). Pass CLEAN=1 to regenerate.
if [[ "${CLEAN:-0}" == "1" || ! -f ca.key ]]; then
    echo "→ generating cluster CA ($out_dir/ca.{crt,key})"
    openssl genrsa -out ca.key 4096 2>/dev/null
    openssl req -x509 -new -nodes -key ca.key -sha256 -days 3650 \
        -subj "/O=FluxFS Dev/CN=FluxFS Dev Cluster CA" \
        -addext "basicConstraints=critical,CA:TRUE,pathlen:0" \
        -addext "keyUsage=critical,keyCertSign,cRLSign" \
        -out ca.crt 2>/dev/null
fi

issue_leaf() {
    local name="$1" spiffe="$2"
    local keyfile="${name}.key" crtfile="${name}.crt" csrfile="${name}.csr"
    echo "→ issuing $crtfile (SAN: $spiffe, DNS:localhost, IP:127.0.0.1)"
    openssl genrsa -out "$keyfile" 2048 2>/dev/null
    openssl req -new -key "$keyfile" -subj "/O=FluxFS Dev/CN=$name" -out "$csrfile" 2>/dev/null

    local extfile
    extfile=$(mktemp -t fluxfs-dev-ca-ext.XXXXXX)
    cat >"$extfile" <<EOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth,clientAuth
subjectAltName=@alt_names

[alt_names]
otherName.0=$SPIFFE_OID;UTF8:$spiffe
DNS.1=localhost
IP.1=127.0.0.1
EOF

    openssl x509 -req -in "$csrfile" -CA ca.crt -CAkey ca.key -CAcreateserial \
        -out "$crtfile" -days 825 -sha256 -extfile "$extfile" 2>/dev/null
    rm -f "$extfile" "$csrfile"
}

# MetaMaster — single cluster for now; g0 = raft-group-0.
issue_leaf "meta" "spiffe://fluxfs/meta/g0"

# ChunkWorkers — one identity per worker_id. worker_id is also lifted into
# the cert so Phase 3 authz checks don't re-parse the name.
for wid in $worker_ids_str; do
    issue_leaf "worker-$wid" "spiffe://fluxfs/worker/$wid"
done

# Admin client (mount binary, ops tooling). Tenant scope comes from a mount
# token (Phase 3), not from this cert.
issue_leaf "client" "spiffe://fluxfs/client-admin/ops"

echo
echo "✓ dev certs ready in $out_dir"
echo "  CA:              $out_dir/ca.crt"
echo "  metamaster:      $out_dir/meta.{crt,key}     (spiffe://fluxfs/meta/g0)"
echo "  client (admin):  $out_dir/client.{crt,key}   (spiffe://fluxfs/client-admin/ops)"
echo
echo "Run binaries with:"
echo "  fluxfs-metamaster --tls-ca-cert $out_dir/ca.crt \\"
echo "                    --tls-server-cert $out_dir/meta.crt \\"
echo "                    --tls-server-key  $out_dir/meta.key"
echo "  fluxfs-chunkworker --worker-id 0 \\"
echo "                     --tls-ca-cert $out_dir/ca.crt \\"
echo "                     --tls-server-cert $out_dir/worker-0.crt \\"
echo "                     --tls-server-key  $out_dir/worker-0.key"
echo "  fluxfs mount ... --tls-ca-cert $out_dir/ca.crt \\"
echo "                   --tls-client-cert $out_dir/client.crt \\"
echo "                   --tls-client-key  $out_dir/client.key"
