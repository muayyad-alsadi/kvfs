# kvfs

Redis/Valkey-backed FUSE3 filesystem for small shared files (`.env`, config) across many hosts.

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

- `-redis` — Redis/Valkey `host:port` (default `127.0.0.1:6379`)
- `-ns` — namespace / key prefix (default `default`)
- `-allow-other` — pass `allow_other` to FUSE
- `-debug` — FUSE debug logs

Requires `fusermount3` (fuse3).

## Key schema

```
kvfs:{ns}:counter                        INCR-only id counter
kvfs:{ns}:meta:{parentId}:{basename}     JSON metadata (includes immutable id)
kvfs:{ns}:dir:{parentId}:{basename}      SET of child basenames (dirs)
kvfs:{ns}:data:{parentId}:{basename}     file bytes or symlink target
kvfs:{ns}                                PUB/SUB channel; payload "{parentId}:{basename}"
```

Literal Redis keys include a hash tag around the namespace (e.g. `kvfs:{prod}:meta:1:.env`) so MULTI/EXEC works on Redis Cluster.
Root lives at `meta:0:` / `dir:0:` with immutable `id=1`. Children of a directory use that directory’s immutable id as `parentId` in meta/data keys. The directory’s own child SET is keyed by the directory’s location `(parentId, basename)`.

## Symlinks

Stored like files: `type=link` in metadata, target path in `data:`.

## Caching / invalidation

- No userspace LRU; getattr/lookup hit Redis.
- Kernel `AttrTimeout` / `EntryTimeout` = 1s.
- Rueidis client-side caching disabled.
- On meta updates and file close: `PUBLISH kvfs:{ns} {parentId}:{basename}`; mounts subscribe and `NotifyEntry`.

## Multi-host

Mount the same `-ns` on each server against the same Redis/Valkey.

## Smoke checklist

```bash
go build -o kvfs ./cmd/kvfs
mkdir -p /tmp/a /tmp/b
./kvfs -redis 127.0.0.1:6379 -ns smoke /tmp/a &
./kvfs -redis 127.0.0.1:6379 -ns smoke /tmp/b &
echo FOO=1 > /tmp/a/.env
ln -s .env /tmp/a/link
# other mount should see files (pub/sub NotifyEntry)
cat /tmp/b/.env
readlink /tmp/b/link
mv /tmp/a/.env /tmp/a/renamed.env   # inode id unchanged
stat -c %i /tmp/a/renamed.env
fusermount3 -u /tmp/a /tmp/b
```

Unit tests (store MULTI/rename/symlink) run without FUSE:

```bash
go test ./...
```
