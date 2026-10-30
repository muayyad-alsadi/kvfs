# Roadmap

[`SPEC.md`](SPEC.md) is implemented at all three sharding levels (`namespace`, `dir`, `file`). This file lists what is not done. Everything under "Verified" is done, with the evidence for it.

## Not done

### 1. No CI runs the release gates

The gates exist and pass when run by hand, but nothing runs them on a change.

- What is missing: a CI configuration that runs `gofmt -l .`, `go vet ./...`, `go test -race ./...`, `scripts/smoke.sh` at each level, and `TestLiveCluster` against a cluster.
- Why it is open: the repository has no remote and no CI file, so a configuration written now could not be run.
- Done when: a CI run executes those commands on every push and fails the build if one fails.

### 2. Crashes are simulated at the client, not by killing a process

`TestCrashAtEveryStep` cuts every operation off after each command it sends and checks the repaired result. The cut is made by a client wrapper, so it proves the protocol's states, not what a killed `kvfs` leaves in a real Valkey, and it does not exercise a connection that drops in the middle of a pipeline.

- Done when: a test starts `kvfs` against a real server, kills it at random points during a workload, restarts it, runs repair, and checks the same invariants.

### 3. Mounts on separate hosts are untested

The smoke script mounts one namespace twice on one host. Cross-mount visibility goes through the same pub/sub channel, but no run used two machines.

- Done when: `scripts/smoke.sh`'s cross-mount checks pass with the two mounts on different hosts.

### 4. No authentication or TLS flags (not in the spec)

The spec does not ask for them, but a server that requires a password or TLS cannot be mounted.

- Done when: `kvfs` accepts a username, a password source and a TLS option and mounts against a server that requires them.

### 5. Directory renames copy at `dir` and `file` level, which is not constant time

The objective is constant work in the size of a directory. At `namespace` level it holds. At the other two it does not, and under the key grammar it cannot:

- A directory's listing is addressed by the directory's own location, and that location contains its name (`file` level) or its parent (`dir` level, across parents). A rename that changes the location must move the listing, in one `HSET` that grows with the directory's children. Measured at 10,000 children: 41 ms at `file` level against 1 ms at `namespace` level.
- A file rename that must copy grows with the file's size.

Constant time here needs a different slot rule, such as tagging an entry by its inode, which changes the grammar and makes `lookup` take two steps (SPEC, Deferred). Until that decision is made, the constant-time directory rename is `namespace` level, and `dir` level within one parent.

### 6. SQLite and other shared writers across hosts

A file's content is read and written by range, so handles on one mount, and on several, see the same bytes. What SQLite needs beyond that is mutual exclusion, and this is where the filesystem stops. Tested with Python's `sqlite3` (SQLite 3.51.2) on real mounts at all three levels:

| Case | Result |
|---|---|
| One connection, one mount | works |
| Several connections or processes on one mount, rollback journal (the default) | works: 80 concurrent inserts, none lost, `integrity_check` ok (`scripts/smoke.sh`, when `sqlite3` is installed) |
| An open reader sees another connection's commit, same mount | works |
| Two mounts (two hosts) writing the same database | **fails**: `disk I/O error`, lost inserts |
| WAL mode | **fails**: `disk I/O error` |

Why it fails:

- `fcntl` locks are enforced by the kernel of one host. This filesystem does not implement `SETLK` and `GETLK`, so two mounts never exclude each other (SPEC lists POSIX locks as out of scope).
- WAL needs shared-memory `mmap` between processes, which the mount does not provide. I did not trace which call fails.

Done when: a database opened by writers on two mounts passes `integrity_check` with no lost rows. That needs byte-range locks held in Redis (SQLite takes SHARED, RESERVED and EXCLUSIVE on different byte ranges) with a lease and a renewal while the holder is alive, and a decision on WAL. It is a feature of its own, not a fix.

### 7. Smaller gaps in the content path

- A handle keeps the name it was opened under. A write or close after the file was renamed through another path is refused with `ENOENT`. Found by reading the code, not tested.
- `O_APPEND` takes its offset from the kernel's cached size, which can be a second stale across mounts. Not tested.
- Truncating to a smaller size costs the size kept, and a write at a large offset into a short file allocates the zero fill up to that offset (SPEC, file content).

### 8. An unlink racing a rename of the same name can delete the rename's new file

`TestUnlinkRacingRenameKeepsDataReachableOrGone` runs an unlink of `b` and a rename of `a` over `b` at the same time. On 2026-10-30 it failed in 9 of 30 runs, at `dir` and `file` level. When it fails, the listing keeps `b`, which now belongs to the renamed file, but `b`'s metadata is gone, so `lookup` returns `ENOENT` for a name that `readdir` shows.

The cause is the order of the steps in `unlinkLinked` (`internal/store/linked.go`). It deletes the entry's keys first, and only then does the compare-and-set on the listing field. A rename that takes over the name between those two steps has its new metadata deleted. The compare-and-set fails, and the unlink returns `ErrNotFound`, but the keys are already gone. The namespace level is not affected, because its unlink is one script.

- Done when: the test passes 100 runs in a row at both levels, and `TestCrashAtEveryStep` still passes.
- Why it is a decision and not a patch: the keys and the listing field are in different slots at these levels, so no single script can check both. The fix is an order: commit the field first, as a claim that repair understands, and then delete the keys. That changes the crash states the spec defines, so it needs the spec changed with it.

## Known limits the spec accepts

- At `dir` and `file` level, a name left pending by a crash answers `EBUSY` to `unlink`, `rmdir` and `rename` until repair runs. The background loop repairs it after ten operation timeouts, and `kvfs -repair=<path>` does it at once.
- A refused create at `dir` and `file` level spends an inode number.
- `RENAME_EXCHANGE` and `RENAME_WHITEOUT` fail with `EINVAL`. `RENAME_NOREPLACE` works.
- A crash in the middle of a rename that replaces an existing name leaves the replaced entry gone and the source in place after repair.
- Server-side scripting must be enabled. This store has no fallback. The Python implementation does (`--sss-mode=no`, see the README), at more round trips per mutation.

## Not planned

The spec's Out of Scope list stands: hardlinks, extended attributes, ACLs, POSIX locks, quotas, large-file chunking, compression, encryption at rest, deduplication, cross-namespace operations, snapshots, Windows, macOS and FUSE 2, changing a namespace's level after initialization, and the `kvfs_v2` migration (deferred to the next breaking change).

## Verified

Run on 2026-10-27 with Go 1.26.10, Valkey 9.0.6 and `fusermount3` on Fedora, as an ordinary user. On 2026-10-30 the gates were run again: `gofmt -l .` and `go vet ./...` are clean, and `go test ./...` passes except `TestUnlinkRacingRenameKeepsDataReachableOrGone` (item 8). The rows below are from 2026-10-27, except the Python row. `-race` and the smoke script were not run again on 2026-10-30.

| What | How | Result |
|---|---|---|
| Key grammar, escaping, three tag placements | golden table in `keys_test.go` | pass |
| All operations at all three levels, round-trip costs | `store_test.go`, `filelevel_test.go`, `dir_test.go`, `worked_example_test.go` | pass |
| Level inference, mismatch and split namespace refusals for every pair of levels | `dir_test.go` | pass |
| Link protocol states, busy refusal, rename repair, two renames of one source | `filelevel_test.go`, `races_test.go` | pass |
| Stale parent refused, create racing rmdir leaves no keys | `orphan_test.go` at all three levels | pass |
| Crash after every command of every operation, then repair: before or after, nothing unresolved, no stray key | `TestCrashAtEveryStep`, all three levels | pass |
| Range reads and writes, size from the content length, truncate, a write to an unlinked file refused, another client sees a write, a write sends only its bytes | `content_test.go`, all three levels | pass |
| Partial writes, truncate and two SQLite writers on one mount, real mounts | `scripts/smoke.sh` at all three levels | pass |
| Pending fields and ghosts repaired by the mount, health counters, `EBUSY` | `internal/fs/repair_test.go` | pass |
| Exactly one address per mutation, two per rename, none for a refused create | `pubsub_test.go` at all three levels | pass |
| Every script and every transaction names keys from one slot | `TestEveryCommandStaysInOneSlot` | pass |
| Race detector | `go test -race ./...` | pass |
| Performance commitments | `TestPerformanceCommitments`, `TestFileMoveTransfersNoBody`, benchmarks (numbers in `README.md`) | pass |
| Real 3-node cluster, all levels, same operations | `TestLiveCluster` with `KVFS_TEST_CLUSTER` | pass, keys spread over all three nodes |
| Real FUSE mounts, two mounts per namespace, all levels, standalone server | `scripts/smoke.sh namespace\|dir\|file` | pass |
| Real FUSE mounts, all levels, 3-node cluster | `scripts/smoke.sh <level> <three nodes>` | pass |
| `kvfs -repair` against a standalone server and the cluster | run by hand: removes a dangling name, refuses a wrong level, refuses `directory` | pass |
| Python implementation writes the same keys: `bench/parity.py` in pykvfs, with scripts on (`yes`) and off (`no`), against this store's harness | outcomes and normalized key dump compared, 24 steps | pass (2026-10-30) |
