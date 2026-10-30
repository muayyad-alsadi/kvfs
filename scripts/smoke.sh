#!/usr/bin/env bash
# Release gate: the README smoke checklist against real FUSE mounts.
#
#   scripts/smoke.sh <namespace|dir|file> [redis-addrs]
#
# It mounts one namespace twice (to check cross-mount visibility), runs the
# checklist, and exits non-zero on the first failed check. Needs /dev/fuse,
# fusermount3, a Go toolchain and a reachable Redis/Valkey (or cluster: pass the
# comma-separated node list).
set -euo pipefail

level=${1:?usage: smoke.sh <namespace|dir|file> [redis-addrs]}
addrs=${2:-127.0.0.1:6379}
ns="smoke-${level}-$$"
work=$(mktemp -d)
a="$work/a"
b="$work/b"
mkdir -p "$a" "$b"
pids=()

cleanup() {
	for m in "$a" "$b"; do fusermount3 -u "$m" 2>/dev/null || true; done
	for p in "${pids[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null || true; done
	wait 2>/dev/null || true
	rm -rf "$work"
}
trap cleanup EXIT

fail() { echo "FAIL [$level]: $*" >&2; exit 1; }
pass() { echo "ok   [$level]: $*"; }

# eventually retries a check for a few seconds: the kernel caches entries for
# one second, so another mount sees a change within that timeout.
eventually() {
	local what=$1
	shift
	for _ in $(seq 1 40); do
		if "$@" 2>/dev/null; then pass "$what"; return 0; fi
		sleep 0.25
	done
	fail "$what"
}

go build -o "$work/kvfs" ./cmd/kvfs
for m in "$a" "$b"; do
	"$work/kvfs" -redis "$addrs" -ns "$ns" -shard-level "$level" "$m" &
	pids+=($!)
done
for m in "$a" "$b"; do
	for _ in $(seq 1 40); do mountpoint -q "$m" && break; sleep 0.25; done
	mountpoint -q "$m" || fail "$m did not mount"
done
pass "two mounts of one namespace"

same() { [ "$(cat "$1")" = "$2" ]; }
inode() { stat -c %i "$1"; }

echo FOO=1 >"$a/.env"
eventually "a file written on one mount is read on the other" same "$b/.env" "FOO=1"
ln -s .env "$a/link"
eventually "a symlink is visible on the other mount" test "$(readlink "$b/link")" = ".env"
[ "$(inode "$a/.env")" = "$(inode "$b/.env")" ] || fail "st_ino differs between mounts"
pass "inode numbers agree across mounts"

mkdir "$a/dir"
before=$(inode "$a/dir")
mv "$a/dir" "$a/renamed"
[ "$(inode "$a/renamed")" = "$before" ] || fail "directory inode changed on rename"
pass "directory rename keeps st_ino"

for i in 1 2 3; do echo "n$i" >"$a/renamed/f$i"; done
mkdir "$a/other"
mv "$a/renamed/f1" "$a/other/f1"
[ "$(cat "$a/other/f1")" = "n1" ] || fail "content lost in a cross-directory move"
pass "cross-directory file move"
mv "$a/renamed" "$a/other/renamed"
[ "$(ls "$a/other/renamed" | wc -l)" = "2" ] || fail "children lost in a directory move"
[ "$(cat "$a/other/renamed/f2")" = "n2" ] || fail "child content lost in a directory move"
pass "directory move keeps its children"
eventually "the move is visible on the other mount" same "$b/other/renamed/f3" "n3"

if rmdir "$a/other" 2>/dev/null; then fail "rmdir removed a non-empty directory"; fi
pass "rmdir refuses a non-empty directory"

mkdir "$a/many"
for i in $(seq 1 300); do : >"$a/many/file-$i"; done
[ "$(ls -f "$a/many" | grep -vc '^\.\.\?$')" = "300" ] || fail "a 300-entry directory lists wrongly"
pass "a 300-entry directory lists completely"

mv "$a/.env" "$a/.env2"
eventually "a rename is visible on the other mount" same "$b/.env2" "FOO=1"
rm "$a/.env2" "$a/link"
eventually "an unlink is visible on the other mount" test ! -e "$b/.env2"

echo old >"$a/victim"; echo new >"$a/mover"
mv "$a/mover" "$a/victim"
[ "$(cat "$a/victim")" = "new" ] || fail "mv did not replace an existing file"
eventually "the replacement is visible on the other mount" same "$b/victim" "new"
[ ! -e "$a/mover" ] || fail "the source survived a replacing mv"
pass "mv replaces an existing file"

: >"$a/empty"; mv "$a/empty" "$a/victim"
[ ! -s "$a/victim" ] || fail "an empty file did not replace the old content"
mkdir "$a/d1" "$a/d2" "$a/full"; : >"$a/full/x"
mv -T "$a/d1" "$a/d2" || fail "mv did not replace an empty directory"
if mv -T "$a/d2" "$a/full" 2>/dev/null; then fail "mv replaced a non-empty directory"; fi
if mv -T "$a/d2" "$a/victim" 2>/dev/null; then fail "mv replaced a file with a directory"; fi
if mv -T "$a/victim" "$a/d2" 2>/dev/null; then fail "mv replaced a directory with a file"; fi
pass "rename follows rename(2) for every type pairing"

printf 'AAAAAAAAAA' >"$a/range"
printf 'bb' | dd of="$a/range" bs=1 seek=3 conv=notrunc 2>/dev/null
[ "$(cat "$a/range")" = "AAAbbAAAAA" ] || fail "a partial write replaced the file"
eventually "a partial write is read on the other mount" same "$b/range" "AAAbbAAAAA"
[ "$(stat -c %s "$a/range")" = "10" ] || fail "size is not the content length"
truncate -s 4 "$a/range"
[ "$(cat "$a/range")" = "AAAb" ] || fail "truncate to a smaller size"
pass "partial writes, size and truncate"

# The reader keeps file bytes in its kernel cache, so a change through either
# mount must reach the other one (see coherence.sh for the cases).
here=$(cd "$(dirname "$0")" && pwd)
"$here/coherence.sh" "$a" "$b" || fail "a change through one mount stayed stale in the other's cache"
"$here/coherence.sh" "$b" "$a" || fail "a change through the other mount stayed stale in the first one's cache"
pass "cached bytes follow a change on the other mount"

if command -v sqlite3 >/dev/null; then
	sqlite3 "$a/t.db" 'create table t(v);'
	writers=()
	for p in 1 2; do
		(for i in $(seq 1 40); do sqlite3 -cmd '.timeout 20000' "$a/t.db" "insert into t values($p$i);"; done) &
		writers+=($!)
	done
	wait "${writers[@]}"
	[ "$(sqlite3 "$a/t.db" 'select count(*) from t;')" = "80" ] || fail "two sqlite writers on one mount lost rows"
	[ "$(sqlite3 "$a/t.db" 'pragma integrity_check;')" = "ok" ] || fail "sqlite integrity check"
	pass "two sqlite writers on one mount (rollback journal)"
fi

kill -USR1 "${pids[0]}"
pass "health counters on SIGUSR1"
echo "smoke [$level] passed"
