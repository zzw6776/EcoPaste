#!/usr/bin/env bash

set -euo pipefail

if [[ -z "${APPLE_CERTIFICATE:-}" || -z "${APPLE_CERTIFICATE_PASSWORD:-}" ]]; then
  echo "::error::macOS release signing secrets are not configured."
  exit 1
fi

certificate_bundle="${RUNNER_TEMP}/ecopaste-release-signing.p12"
certificate_pem="${RUNNER_TEMP}/ecopaste-release-signing.pem"
cleanup() {
  rm -f "${certificate_bundle}" "${certificate_pem}"
}
trap cleanup EXIT

printf '%s' "${APPLE_CERTIFICATE}" \
  | openssl base64 -d -A -out "${certificate_bundle}"
extract_certificate() {
  openssl pkcs12 "$@" \
    -in "${certificate_bundle}" \
    -clcerts \
    -nokeys \
    -passin env:APPLE_CERTIFICATE_PASSWORD \
    -out "${certificate_pem}"
}
if openssl pkcs12 -help 2>&1 | grep -Fq -- '-legacy'; then
  extract_certificate -legacy
else
  extract_certificate
fi

expected_fingerprint='DE:D1:2F:3B:5E:76:32:56:9C:C0:70:AE:54:5B:F4:B5:4F:40:DB:25:54:73:BB:98:F5:07:15:2D:C8:F9:42:15'
actual_fingerprint="$(
  openssl x509 -in "${certificate_pem}" -noout -fingerprint -sha256 \
    | cut -d= -f2
)"
if [[ "${actual_fingerprint}" != "${expected_fingerprint}" ]]; then
  echo "::error::macOS release signing certificate fingerprint does not match the pinned identity."
  exit 1
fi

sudo security add-trusted-cert \
  -d \
  -r trustRoot \
  -p codeSign \
  -k /Library/Keychains/System.keychain \
  "${certificate_pem}"
