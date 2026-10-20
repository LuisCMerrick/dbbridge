#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
go version
go mod verify
go mod tidy -diff
go vet ./...
# Platform-specific code (Windows service, file ACLs) is type-checked too.
GOOS=windows GOARCH=amd64 go vet ./...
go test ./... -count=1 -timeout=90s
go test -race ./... -count=1 -timeout=120s
go test -tags=database ./integration -run '^$'
export DBB_TOKEN_TO_HQ=configuration-check-current
export DBB_TOKEN_FROM_PLANT_CURRENT=configuration-check-current
for config_path in configs/*.yaml; do
  go run ./cmd/dbbridge check-config --config "$config_path"
done
# Production examples reference /etc/dbbridge TLS files: check their structure
# and cross references offline (no secrets or certificates are read).
for config_path in configs/production/peer-*.yaml; do
  go run ./cmd/dbbridge check-config --offline --config "$config_path"
done
# Production examples are checked with ephemeral certificate/key pairs; deployment
# check-config must still validate the actual provisioned files.
go test ./internal/config -run 'Test(V2ExampleModeMatrix|TLSCertificatePreflight)$' -count=1
# Needs network access (downloads govulncheck and the vulnerability database).
go run golang.org/x/vuln/cmd/govulncheck@v1.9.0 ./...
