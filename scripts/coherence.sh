#!/usr/bin/env bash
# Cross-mount cache coherence. A change made through WRITER must reach READER even
# though READER has the old bytes in its kernel page cache. Each case warms the
# reader's cache, changes the file through the writer, then polls the reader.
# Works for any mix of kvfs and pykvfs mounts that share one namespace.
#
# Usage: coherence.sh WRITER_MOUNT READER_MOUNT
# Prints CSV: case,result,ms   (ms = writer's change until the reader sees it)
# Exit status 1 if any case stayed stale for 2 seconds.
set -u

W=$1
R=$2
D=coh-$$
POLLS=400          # 400 polls x 5 ms = 2 s budget per case
failed=0

now_ms() { date +%s%3N; }

# converged COMMAND...: retries COMMAND every 5 ms until it succeeds or the budget ends
converged() {
  local i
  for ((i = 0; i < POLLS; i++)); do
    "$@" && return 0
    sleep 0.005
  done
  return 1
}

# expect_at PATH WANT: true when PATH shows WANT (content, <gone> or <exists>)
expect_at() {
  case $2 in
    "<gone>") [[ ! -e $1 ]] ;;
    "<exists>") [[ -e $1 ]] ;;
    *) [[ "$(cat "$1" 2>/dev/null)" == "$2" ]] ;;
  esac
}

# apply PATH CONTENT: writes CONTENT, or removes PATH when CONTENT is <gone>
apply() {
  if [[ $2 == "<gone>" ]]; then
    rm -f "$1"
  else
    printf '%s' "$2" > "$1"
  fi
}

# case_run NAME BEFORE AFTER
case_run() {
  local name=$1 before=$2 after=$3 p=$D/$1 t0 t1 result
  if [[ $before != "<gone>" ]]; then
    apply "$W/$p" "$before"
  fi
  if ! expect_at "$R/$p" "$before"; then
    echo "$name,BEFORE_MISMATCH,0"
    failed=1
    return
  fi
  t0=$(now_ms)
  apply "$W/$p" "$after"
  if converged expect_at "$R/$p" "$after"; then
    result=ok
  else
    result=STALE
    failed=1
  fi
  t1=$(now_ms)
  echo "$name,$result,$((t1 - t0))"
}

# held_case: the reader keeps one descriptor open across the writer's change and
# re-reads it with pread, inside the 1 s attribute timeout, so the kernel does not
# re-validate on its own. Only a content invalidation can make it see the change.
held_case() {
  local p=$D/held t0 t1 out
  if ! command -v python3 >/dev/null; then
    echo "held,skipped,0"
    return
  fi
  apply "$W/$p" "held-v1"
  if ! expect_at "$R/$p" "held-v1"; then
    echo "held,BEFORE_MISMATCH,0"
    failed=1
    return
  fi
  out=$(python3 - "$R/$p" "$W/$p" <<'EOF'
import os, subprocess, sys, time

reader, writer = sys.argv[1], sys.argv[2]
fd = os.open(reader, os.O_RDONLY)
os.pread(fd, 100, 0)
start = time.monotonic()
subprocess.run(["sh", "-c", f"printf '%s' held-v2 > '{writer}'"], check=True)
while True:
    seen = os.pread(fd, 100, 0)
    if seen == b"held-v2" or time.monotonic() - start > 2:
        break
    time.sleep(0.005)
ms = int((time.monotonic() - start) * 1000)
print(("ok" if seen == b"held-v2" else "STALE") + f",{ms}")
EOF
)
  case $out in
    ok,*) ;;
    *) failed=1 ;;
  esac
  echo "held,$out"
}

mkdir -p "$W/$D"
case_run overwrite version1 version2
case_run grow short "a much longer replacement payload"
case_run shrink "a long original payload" tiny
case_run unlink "gone soon" "<gone>"
case_run create "<gone>" "appeared"
held_case
rm -rf "$W/$D"
exit "$failed"
