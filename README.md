# kvfs

Redis/Valkey-backed FUSE3 filesystem for small shared files (`.env`, config) across many hosts.

> The schema below is the contract in [`SPEC.md`](./SPEC.md). All three sharding levels it defines are implemented: `namespace` (default), `dir` and `file`. [`ROADMAP.md`](./ROADMAP.md) lists what is not done.

## Build

```bash
go build -o kvfs ./cmd/kvfs
```

## Mount

```bash
mkdir -p /mnt/kvfs
./kvfs -redis 127.0.0.1:6379 -ns prod /mnt/kvfs
```

Flags:

- `-redis` — Redis/Valkey `host:port` (default `127.0.0.1:6379`). A comma-separated list names the nodes of a cluster. The client detects a standalone server or a cluster by itself.
- `-ns` — namespace / key prefix (default `default`)
- `-shard-level` — `namespace` (default) | `dir` | `file`; see [Sharding](#sharding). Any other value is refused before a key is read.
- `-repair` — repair every component of a path (for example `-repair /conf/app.toml`), print what was done to each, and exit. No mountpoint is needed. See [Repair and health](#repair-and-health).
- `-timeout` — maximum duration of one Redis operation (default `1s`)
- `-allow-other` — pass `allow_other` to FUSE
- `-debug` — FUSE debug logs

Requires `fusermount3` (fuse3).

## Key schema

One infix rule for every suffix: `<parent id>/<name>`, where the parent is the entry's owner and `name` is its basename.

```
kvfs/<ns>/0/./counter              INCR-only inode allocator (never reset)
kvfs/<ns>/0/./pubsub               namespace invalidation channel
kvfs/<ns>/0/./meta                 root metadata (id = 1)
kvfs/<ns>/0/./dir                  HASH of the root's children
kvfs/<ns>/<parent id>/<name>/dir   HASH of a directory's children
kvfs/<ns>/<parent id>/<name>/meta  JSON metadata (carries the immutable inode)
kvfs/<ns>/<parent id>/<name>/data  file bytes or symlink target
```

- `0` is the namespace node — a pseudo-inode that is only the root's parent. The root is an ordinary entry named `.` under it, with its own id `1`.
- Every key uses `/` as the delimiter with one fixed escaping rule. A key never needs the entry's own inode to compute.
- Basenames are percent-encoded; `%`, `/`, `"`, `*`, `?`, `[`, `]`, `{`, `}`, `\` and `0x00`–`0x1F` are escaped, everything else is verbatim. Decoding uses the standard percent-decoder (`+` is never a space).
- A namespace is validated at startup: `[A-Za-z0-9-]+`.
- The infix is always `<namespace>/<parent id>/<name>`. The sharding level only moves the braces of the hash tag: `kvfs/{prod}/1/README.md/data`, `kvfs/{prod/1}/README.md/data` or `kvfs/{prod/1/README.md}/data`.

## Directories

A directory's child list is a **hash at the directory's own address**, not a set and not a placeholder:

- field = child basename, value = `type:inode` (`f`, `d`, `l`), e.g. `d:42`
- `mkdir` creates no member; an empty hash simply does not exist
- `readdir` is one `HGETALL` and carries each child's type, so `ls -l` never stats per entry
- `rmdir` asks for a count, never for the names
- a rename moves the directory's own metadata and listing; its children are addressed by the directory's **inode** and are not touched
- the listing value is a cache of **immutable** facts only. Metadata is the authority; permissions and timestamps are never duplicated, and a file's size is the length of its content key, not a stored field.

## Sharding

| level | tag | what shares a slot |
|---|---|---|
| `namespace` *(default)* | `{<ns>}` | the whole namespace; every mutation is one atomic script |
| `dir` | `{<ns>/<parent id>}` | the entries of one directory (every entry with the same parent id) |
| `file` | `{<ns>/<parent id>/<name>}` | one entry: metadata, content, and its listing |

The level is inferred from the namespace's own keys on every mount, never stored. Mounting with a level that differs from the namespace, or against a namespace written at two levels, fails at startup.

The commitment is constant work: an operation's cost must not depend on how many entries its directory holds. Round trips are a secondary cost, because a fixed number of constant-time steps is still constant time. At the default level `create`, `mkdir`, `rename`, `unlink` and `rmdir` are constant (one script each), a rename moves keys instead of bytes, and a directory rename of ten thousand children costs the same as an empty one. `readdir` is one `HGETALL`, linear only in the names it returns. A file's content is one string; a read is one `GETRANGE` and a write is one `SETRANGE`, so a write costs the bytes written and not the size of the file, and an open reads nothing. A file handle holds no copy, so several handles see the same bytes. Changing permissions, ownership or timestamps is a watched read-modify-write of the metadata document: two round trips however many fields one `setattr` carries, and a change another mount lands in between makes it retry instead of being overwritten.

`dir` and `file` level spread entries across nodes, at a documented price. A directory's listing is addressed by the directory's own location and its children by the directory's inode, so a link operation writes two slots. It claims the name in the parent's listing, writes the entry in its own slot, then settles the field. A refused create spends an inode number but writes nothing.

| operation | `namespace` | `dir` | `file` |
|---|---|---|---|
| `lookup` / `stat` / `readdir` | 1 round trip | 1 round trip | 1 round trip |
| create / mkdir / symlink | 1 round trip, 1 slot | 4 round trips, 3 slots | 4 round trips, 3 slots |
| unlink / rmdir | 1 round trip, 1 slot | 3 round trips, 2 slots | 3 round trips, 2 slots |
| rename, same parent | 1 round trip, keys renamed | 4 round trips, 2 slots, keys renamed | 6 round trips, 3 slots, entry copied |
| rename, across parents | 1 round trip, keys renamed | 7 round trips, 4 slots, entry copied | 7 round trips, 4 slots, entry copied |

Every row of the table is constant in the number of entries in the directory except one: a directory rename that has to copy its listing (any directory rename at `file` level, and a cross-parent directory rename at `dir` level) is one `HSET` of all its names, so it grows with the directory's children. A file rename that copies grows with the file's size. Both are listed in [`ROADMAP.md`](./ROADMAP.md). Every other step is constant, and the extra round trips at `dir` and `file` level are a fixed number of such steps.

Rename onto an existing name follows `rename(2)`: a file replaces a file, a directory replaces an empty directory, and the other pairings fail with `EISDIR`, `ENOTDIR` or `ENOTEMPTY`. `RENAME_NOREPLACE` is honored; `RENAME_EXCHANGE` is not. At `dir` and `file` level a replacing rename costs two to three round trips more.

A copy is bounded by the entry (a file's content, or a directory's listing in one `HSET`), never by a directory's children. At `dir` and `file` level `unlink`, `rmdir` and `rename` of a name that another client has claimed and not settled fail with `EBUSY`.

A crash between slots leaves a pending field, and a repair pass reconciles one named entry at a time. See [Repair and health](#repair-and-health).

## Repair and health

A crash during a link operation at `dir` or `file` level leaves one of three states:

- a field whose metadata is missing is removed;
- a pending field (`f:12:p`) whose metadata is complete is settled (`f:12`);
- a pending rename destination (`f:12:p:<src parent id>/<src name>`) is discarded when the source still owns the inode, and settled when it does not.

`readdir` reports a pending field as an unknown type, so the kernel stats it. The mount repairs in three ways:

- a `lookup` miss removes a settled field that has no metadata;
- a background loop repairs a pending field that has held the same value for ten operation timeouts (10 s by default), as a compare-and-set, so a client that is still working is never undone;
- `kvfs -repair=/path/to/entry` repairs every component of a path and exits. Use it only when nothing is writing to that path.

`SIGUSR1` prints the health counters to the log: `unknown` (listed fields with an unreadable or pending type), `pending`, `dangling` (listed names whose metadata was missing on lookup, at `dir` and `file` level) and `repaired`. A counter that keeps climbing means listings and metadata disagree.

A create racing an `rmdir`, or racing a rename that replaces the directory, has two outcomes and neither leaves keys behind: the create is refused with `ENOENT`, or `rmdir` fails with `ENOTEMPTY`. See SPEC, Link operations.

## Symlinks

Stored like files: `type=link` in metadata, target path in `data`.

## Caching / invalidation

- No userspace LRU; getattr/lookup read Redis (1s op timeout by default).
- Kernel `AttrTimeout` / `EntryTimeout` = 1s, `NegativeTimeout` = 500ms.
- Rueidis client-side caching disabled. The same client option set works for a standalone server and for a cluster.
- Separate client for `SUBSCRIBE` so a blocking subscribe never starves a mutation.
- Read-only opens use the kernel page cache (`FOPEN_KEEP_CACHE`). Writable opens bypass it (`FOPEN_DIRECT_IO`): a write through the cache is split into about 1.5 requests per 10 KiB, and each request is one Redis script call. Another host's writes reach the cache only through the invalidation below.
- After each successful mutation the mount publishes the affected address on `kvfs/<ns>/0/./pubsub` — `<parent id>/<encoded basename>`, the pair the key grammar already uses — so another host can drop that entry and the cached bytes and attributes of its file. A rename publishes the old and the new address; a directory operation publishes the directory's own address, because that is the listing that changed.
- At `namespace` level `PUBLISH` rides inside the script that commits the change, so it costs no extra round trip. At `dir` and `file` level it rides inside the settle script; the subscriber hands the address to a background loop, never to the FUSE request path (that deadlocks). The loop calls `NotifyEntry` and `NotifyContent(0, 0)` and waits when busy instead of dropping. The 1s timeouts bound a missed message for entries and attributes only; cached bytes stay stale until the kernel drops the pages.
- Bootstrap initializes a fresh namespace in one atomic script (probe both placements, then set the counter to `1` and write the root), so concurrent mounts create the root exactly once and the root id is always `1`.

## Multi-host

Mount the same `-ns` on each server against the same Redis/Valkey.

## Python implementation

[`pykvfs`](https://github.com/muayyad-alsadi/pykvfs) is a Python implementation of this filesystem at the `namespace` level, on PyPI as `pip install pykvfs`. Its command is also called `kvfs`, so put the Go binary under another name or call it by path. Both write the same keys, so a namespace mounted by either one reads the other's files, and mounts of both can share a namespace.

The differences that matter to an operator:

- This store always runs its mutations as server-side scripts. pykvfs can also run them as `WATCH`/`MULTI`/`EXEC` transactions, for a server that disallows scripts: `--sss-mode=no`. The default, `--sss-mode=auto`, uses scripts when the server allows them.
- pykvfs has only the `namespace` level. `-shard-level=dir` and `file` are refused there.
- A writable open keeps the kernel cache in pykvfs. This store bypasses it (see Caching / invalidation).
- pykvfs has no `-repair` and no repair pass. The `dir` and `file` link protocol is not in it.

## Smoke checklist

`scripts/smoke.sh` runs this checklist against real FUSE mounts and exits non-zero on the first failure. It needs `/dev/fuse`, `fusermount3`, Go and a reachable Redis/Valkey (pass the comma-separated node list for a cluster):

```bash
scripts/smoke.sh namespace 127.0.0.1:6379
scripts/smoke.sh dir       127.0.0.1:6379
scripts/smoke.sh file      127.0.0.1:6379
```

Cached bytes are checked by `scripts/coherence.sh WRITER_MOUNT READER_MOUNT`, which `smoke.sh` runs in both directions. It warms the reader's cache, changes the file through the writer, and polls the reader for up to 2 seconds. Use it between any mix of kvfs and pykvfs mounts that share one namespace.

By hand, for one level:

```bash
go build -o kvfs ./cmd/kvfs
mkdir -p /tmp/a /tmp/b
./kvfs -redis 127.0.0.1:6379 -ns smoke -shard-level=namespace /tmp/a &
./kvfs -redis 127.0.0.1:6379 -ns smoke -shard-level=namespace /tmp/b &
echo FOO=1 > /tmp/a/.env
ln -s .env /tmp/a/link
cat /tmp/b/.env                     # cross-mount visibility (pub/sub)
readlink /tmp/b/link
mkdir /tmp/a/dir && mv /tmp/a/dir /tmp/a/renamed
stat -c %i /tmp/a/renamed           # inode unchanged
ls -la /tmp/a
fusermount3 -u /tmp/a /tmp/b
```

The script also writes part of a file and truncates it, runs two `sqlite3` writers on one mount when `sqlite3` is installed, moves a file across directories, moves a directory with children, lists a 300-entry directory, and checks that a rename and an unlink on one mount reach the other.

## Tests and benchmarks

Unit tests (store operations at all three levels, key builder, level inference, link protocol states, races, repair, pub/sub payloads, slot safety of every script and transaction) run without FUSE:

```bash
go test ./...
go test -race ./...
```

To run the same operations against a live cluster, point `KVFS_TEST_CLUSTER` at its nodes:

```bash
KVFS_TEST_CLUSTER=127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003 go test ./internal/store -run TestLiveCluster
```

Benchmarks run at the store seam against `miniredis`, so they measure the commands each level issues and not network latency:

```bash
go test -run '^$' -bench . -benchtime 20x ./internal/store
```

One run on a 12-thread workstation (time per operation):

| benchmark | `namespace` | `dir` | `file` |
|---|---|---|---|
| `Readdir10k` (one `HGETALL`, 10,000 entries) | 26.0 ms | 26.3 ms | 27.0 ms |
| `RenameDirectory10k` (same parent) | 1.0 ms | 2.6 ms | 40.7 ms |
| `RenameFile1MiB` (same parent) | 0.9 ms | 2.9 ms | 7.0 ms |

Directory rename at `file` level copies the listing, so it grows with the listing. At `namespace` and `dir` level it does not: `TestPerformanceCommitments` asserts a 10,000-entry directory renames with the same commands as an empty one, and `TestFileMoveTransfersNoBody` asserts that a file move sends no content. The `readdir` time is the 10,000-entry decode in the client.
