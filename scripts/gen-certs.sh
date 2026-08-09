#!/usr/bin/env bash
# gen-certs.sh — Generate a self-signed CA, server certificate, and client
# certificate for fluxion mTLS.
#
# Usage:
#   ./scripts/gen-certs.sh [output-dir]
#
# Defaults to /etc/fluxion if no output directory is supplied.
#
# Files produced:
#   ca.crt / ca.key          — CA certificate and key
#   server.crt / server.key  — Worker server certificate and key
#   client.crt / client.key  — Scheduler client certificate and key
set -euo pipefail

OUT="${1:-/etc/fluxion}"
mkdir -p "$OUT"

echo "[gen-certs] Writing certificates to $OUT"

# ── CA ────────────────────────────────────────────────────────────────────────
openssl genrsa -out "$OUT/ca.key" 4096
openssl req -new -x509 -days 3650 \
    -key "$OUT/ca.key" \
    -subj "/CN=fluxion-ca" \
    -out "$OUT/ca.crt"

# ── Server (worker) certificate ───────────────────────────────────────────────
openssl genrsa -out "$OUT/server.key" 2048
openssl req -new \
    -key "$OUT/server.key" \
    -subj "/CN=fluxion-worker" \
    -out "$OUT/server.csr"
openssl x509 -req -days 365 \
    -in "$OUT/server.csr" \
    -CA "$OUT/ca.crt" -CAkey "$OUT/ca.key" -CAcreateserial \
    -out "$OUT/server.crt"
rm -f "$OUT/server.csr"

# ── Client (scheduler) certificate ───────────────────────────────────────────
openssl genrsa -out "$OUT/client.key" 2048
openssl req -new \
    -key "$OUT/client.key" \
    -subj "/CN=fluxion-scheduler" \
    -out "$OUT/client.csr"
openssl x509 -req -days 365 \
    -in "$OUT/client.csr" \
    -CA "$OUT/ca.crt" -CAkey "$OUT/ca.key" -CAcreateserial \
    -out "$OUT/client.crt"
rm -f "$OUT/client.csr"

echo "[gen-certs] Done."
echo ""
echo "  CA cert:      $OUT/ca.crt"
echo "  Server cert:  $OUT/server.crt  (use with --tls-cert)"
echo "  Server key:   $OUT/server.key  (use with --tls-key)"
echo "  Client cert:  $OUT/client.crt  (use in workflow YAML tls.cert)"
echo "  Client key:   $OUT/client.key  (use in workflow YAML tls.key)"
