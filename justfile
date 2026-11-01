set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

ldflags := "-s -w"

# production binary in this directory
build:
    just _bin kvfs ./cmd/kvfs

# unit tests
test:
    go test ./...

# gofmt, vet, and unit tests
check:
    files="$(gofmt -l .)"; test -z "$files" || { printf '%s\n' "$files"; exit 1; }
    go vet ./...
    just test

# name and package, one pair per line. Both release tarballs contain every one of these.
bins := "
kvfs ./cmd/kvfs
"

# linux tarballs in dist/, one per architecture
release:
    mkdir -p dist
    just _pack amd64
    just _pack arm64

[private]
_bin name pkg:
    go build -trimpath -ldflags={{ quote(ldflags) }} -o {{ name }} {{ pkg }}

[private]
_pack arch:
    #!/usr/bin/env bash
    set -euo pipefail
    stage="dist/linux-{{arch}}"
    rm -rf "$stage"
    mkdir -p "$stage"
    names=()
    while read -r name pkg; do
        [ -n "$name" ] || continue
        CGO_ENABLED=0 GOOS=linux GOARCH={{arch}} just _bin "$stage/$name" "$pkg"
        names+=("$name")
    done <<< {{ quote(bins) }}
    tar -C "$stage" -czf "dist/kvfs-linux-{{arch}}.tar.gz" "${names[@]}"
    rm -rf "$stage"
