# kvfs next generation — key schema, directory representation, and O(1) mutations

## Problem Statement

The current generation stores a namespace in four flat key families separated by `:`, with the child list of a directory held as a Redis **set** of basenames. That works for the intended load (small shared config files) but it breaks down in five concrete ways:

1. **Keys are not safely encodable.** The delimiter `:` is also a legal filename character, and no escaping rule exists, so a basename that contains `:` or `*` can collide with the schema or be interpreted as a glob by `SCAN`/`KEYS`/pub-sub. There is no documented list of characters that may appear literal in a key.
2. **The sharding level is frozen at the namespace.** All keys for a tenant must live on one cluster slot, so a single hot tenant cannot be spread. There is also no guard against changing the intended level between mounts, which would silently split a filesystem across slots.
3. **`readdir` is N+1.** Listing a directory reads the child-name set and then performs one metadata read — with a JSON decode — per child, purely to learn each child's inode and type. This is the most expensive everyday operation and it scales with directory size.
4. **Mutations do work proportional to data that does not change.** Renaming a directory copies its entire child list element by element; renaming or moving a file reads and rewrites the file body even though neither its metadata nor its bytes change; `rmdir` fetches every child name only to test emptiness; `mkdir` writes and deletes a placeholder member solely to materialize an empty set.
5. **The inode number is only discoverable through a JSON blob.** There is no cheap name→inode map, so every consumer unpacks a ~137-byte document to learn one integer, and the representation cannot express a directory entry as a single addressable record.

Operators also lack a written convention for root identity, bookkeeping keys, and `parentId` naming, which makes independent tooling (backups, audits, repair scripts) guess at the layout.

## Solution

A second-generation schema on the existing `kvfs/` magic prefix — with `kvfs_v2/` kept in reserve for a future breaking change — with four properties:

- **One key builder.** Every key is produced by a single function of (`namespace`, `sharding level`, `parent id`, `basename`, `suffix`). The suffix may be empty so a prefix can be computed once and reused. No other code concatenates keys.
- **`/` as the delimiter, with one canonical escaping rule.** Basenames are percent-encoded with a small, fixed character set, so the number and position of delimiters is invariant and tooling can split a key without ambiguity.
- **A directory is a hash at the directory's own address.** The field is the child's basename; the value is `type:inode`. One infix rule covers every suffix, so a directory's listing sits at the same `<parent id>/<basename>` address as its metadata and content. Its children are addressed by the directory's **inode**, which does not change — so a rename touches the directory's own keys and no child at all.
- **Mutations are constant-time in the size of the directory.** This is the commitment, and it is about work, not round trips: an operation issues a fixed number of commands, and each command does constant work however many entries the directory holds. A fixed number of constant-time steps is still constant time, so a protocol of four or seven steps meets it as well as one script does. Round trips are a secondary objective, kept low because each one is latency, never traded against the constant-work rule. `readdir` is one `HGETALL` and is linear only in the names it returns, directory rename at the default level is a single atomic script with no per-child commands, file rename moves keys instead of bytes, `rmdir` asks for a count, and `mkdir` creates no placeholder. A `dir`- or `file`-level namespace trades atomicity of link operations and key moves for spreading entries across nodes (see Sharding), and where a rename must copy it is not constant: a directory's listing is copied in one `HSET` that grows with its children, and a file's content grows with its size. That is the one place the commitment does not hold at those levels, and `ROADMAP.md` tracks it.

Existing semantics are preserved: an immutable inode number allocated by an `INCR` counter, metadata as JSON, file bytes in the data suffix, symlinks stored as data, cross-host invalidation over pub/sub, and 1-second attribute/entry timeouts.

## User Stories

1. As a platform operator, I want every key of a namespace to sort and split deterministically, so that backup and audit tooling can parse the schema without guessing at filenames.
2. As a platform operator, I want the sharding level to be derived from the namespace's own keys on every mount, so that a mistyped flag cannot silently split a filesystem across cluster slots.
3. As a platform operator, I want the layout to stay on the existing `kvfs/` prefix, so that tooling and dashboards keyed on the prefix keep working, with `kvfs_v2` reserved only for a future breaking change.
4. As a platform operator, I want a namespace to be created deterministically on first mount, so that repeated or concurrent mounts converge on the same root and counter instead of racing.
5. As a platform operator, I want a documented inventory of the bookkeeping keys (counter, channel, root record), so that I can reason about what a namespace contains before deleting anything.
6. As a platform operator, I want mounting with a different sharding level to fail with a clear error rather than start a broken filesystem, so that the failure is loud and immediate.
7. As an application developer, I want `readdir` on a directory with thousands of entries to be a single Redis round trip, so that `ls`, `find`, `tar`, and `git status` on the mount do not stall as directories grow.
8. As an application developer, I want directory entries to carry their type in the listing, so that the kernel does not have to issue a `stat` for every entry.
9. As an application developer, I want a listed entry whose type byte is unreadable to still be returned (as unknown) rather than dropped, so that a corrupt record degrades to a `stat` instead of silently hiding a file.
10. As an application developer, I want renaming a directory with ten thousand children to cost the same as renaming an empty one at the default sharding level, so that reorganizing a mount is not a bulk data operation.
11. As an application developer, I want moving a file between directories to move its keys instead of copying its bytes at the default sharding level, so that `mv` of a multi-megabyte file is O(1) and does not risk a partially written destination.
12. As an application developer, I want `rmdir` to decide emptiness by asking for a count, so that removing an empty directory never transfers its children's names.
13. As an application developer, I want `mkdir` to need no placeholder member or cleanup pair, so that directory creation is a short, single atomic transaction.
14. As an application developer, I want a file to keep its inode number across rename and across mounts, so that hardlink-free tooling that keys on `st_ino` remains correct.
15. As an application developer, I want a directory's child list to remain correct after the directory is renamed or moved, so that path changes never desynchronize a listing.
16. As an application developer, I want `stat` to keep answering from a single metadata read, so that the schema change does not make attribute lookups slower.
17. As an application developer, I want `lookup` to keep answering from a single metadata read, so that path resolution does not regress.
18. As an application developer, I want creating a name that already exists to fail deterministically even under concurrent clients, so that two creators cannot both believe they own the name.
19. As an application developer, I want rename onto an existing name to behave as `rename(2)` says, so that `mv new old` replaces a file or an empty directory atomically, a type mismatch or a non-empty directory is refused with the right errno, and a client that asks for `RENAME_NOREPLACE` gets `EEXIST` instead of a clobbered file.
20. As an application developer, I want filenames containing `*`, `?`, `[`, `]`, `{`, `}`, `\`, `"`, `%`, `/`, colons, spaces, and non-ASCII characters to round-trip exactly, so that the filesystem does not mangle legal names.
21. As an application developer, I want a cache/miss mismatch between a directory entry and its metadata to be resolved in favor of the metadata and repaired, so that a single bad write cannot make `ls -l` disagree with `stat` forever.
22. As an application developer, I want a create or rename interrupted midway to leave either no visible change or a complete one — or, at `dir` and `file` level, a name reported as unknown type that repair resolves — so that a crash cannot leave a name pointing at nothing.
23. As an application developer, I want an unreadable list entry to be surfaced through the health diagnostics, so that drift between the child list and metadata is detected rather than guessed at.
24. As a multi-host operator, I want a rename in one mount to be visible in another within the existing invalidation timeout, so that the second generation does not regress cross-host coherence.
25. As a multi-host operator, I want the invalidation payload to use the same encoding as the key grammar, so that a subscriber can address the affected entry without re-deriving it.
26. As a cluster administrator, I want every key touched by one filesystem operation to share a hash tag at the default sharding level, so that the operation can remain a single transaction on one node.
27. As a cluster administrator, I want a sharding level this build does not implement to be rejected at configuration time rather than at the first cross-slot failure, so that misconfiguration is caught before data is written.
28. As a cluster administrator, I want `dir` and `file` levels to be available when one node cannot hold the working set, and every operation they weaken to be documented and crash-safe, so that choosing one is a capacity decision rather than an atomicity surprise.
29. As an auditor, I want each directory listing entry to record the immutable type alongside the inode, so that I can reconstruct a directory tree from keys alone without reading every metadata document.
30. As an auditor, I want the metadata document to remain the authority for permissions, ownership and timestamps, and the content key's length to be the only record of a file's size, so that one source of truth exists for each mutable attribute.
31. As a maintainer, I want the schema expressed as a pure, table-tested function, so that key changes can be reviewed as data rather than discovered in call sites.
32. As a maintainer, I want filesystem operations tested through the store's public surface against an in-memory Redis, so that I can assert behavior and Redis command counts without a FUSE mount.
33. As a maintainer, I want a command-count assertion for `readdir`, directory rename, and file move, so that a future change cannot quietly reintroduce O(N) work.
34. As a maintainer, I want the domain vocabulary (`namespace`, `parent id`, `basename`, `suffix`, `sharding level`, `type code`) written down and used consistently, so that code and tooling share one language.

## Implementation Decisions

### Key grammar

Every key is `<magic>/<infix>/<suffix>` where the infix is `<parent id>/<name>` — the parent is the entry's owner, the same thing under two names:

```
kvfs/<ns>/0/./counter              INCR-only inode allocator (never reset)
kvfs/<ns>/0/./pubsub               namespace event channel
kvfs/<ns>/0/./meta                 root's metadata (id = 1)
kvfs/<ns>/0/./dir                  HASH of the root's children
kvfs/<ns>/<parent id>/<name>/dir   HASH of a directory's children (field=basename, value=type:inode)
kvfs/<ns>/<parent id>/<name>/meta  JSON metadata for the entry
kvfs/<ns>/<parent id>/<name>/data  file bytes or symlink target
```

- One infix rule for every suffix: **all four keys of an entry share one address**, `<parent id>/<basename>`. There is no second addressing scheme to remember, no suffix whose key lives somewhere else, and no key that requires the entry's own inode to compute.
- `0` is the **namespace node**: a pseudo-inode that exists only as the root's parent, is never a FUSE inode, and is never handed out by the counter (ids start at `1`). The namespace node has exactly one child — the root, named `.` — so root is an ordinary entry and needs no special case.
- `.` is a reserved **basename**, not an operator: the kernel refuses to create a child called `.` or `..`, so nothing a client can create can collide with the root's address.
- Root therefore has two ids in play, and they mean different things: `0` is its **parent** (the first field of its keys), `1` is its **own** inode (the value in the namespace's listing, and the `id` in its metadata). Nothing in the schema equates them.
- `parent id` (equivalently, `owner`) in this document always means an **inode number**, never a name.
- Counter and pub-sub are namespace-wide records sharing the root's infix; they are not filesystem entries.
- The number and position of `/` delimiters is fixed for every key; consumers may split without escaping awareness beyond the basename field.
- The examples show `namespace` level. At `dir` level the tag is `{<ns>/<parent id>}` and at `file` level it is `{<ns>/<parent id>/<name>}`. The infix and the suffix are the same at every level, and only the braces move (see Sharding).

### Escaping

- Basenames are percent-encoded with a **custom encoder**; decoding uses the platform's standard percent-decoder (`+` must not be interpreted as space).
- Exactly these bytes are escaped: `%`, `/`, `"`, `*`, `?`, `[`, `]`, `{`, `}`, `\`, and `0x00`–`0x1F`.
- Everything else — `.`, `..`-like names, spaces, `:`, non-ASCII — is stored verbatim; no double-encoding and no normalization (byte-exact round trip).
- A namespace is validated at startup: non-empty, `[A-Za-z0-9-]+`, bounded length. Anything else is a startup error, not a runtime fallback.
- The escaping rule is the schema's contract: `SCAN`, `KEYS`, and pub-sub patterns must remain meaningful, so glob metacharacters never appear literally.

### Directory representation

- A directory's child list is a **hash** at the directory's own address — `<parent id>/<basename>/dir`, the same infix as its metadata and content — field = child basename, value = `type:inode` with a one-character type code (`f` file, `d` dir, `l` link), e.g. `d:42`. At `dir` and `file` level the value may carry a trailing state marker (`d:42:p`, or `d:42:p:<src parent id>/<escaped src name>` for the destination of a rename) until the field is settled; the marker is never written at `namespace` level.
- The listing moves with the name, and only the listing does: renaming a directory renames this key, while its children's keys are addressed by the directory's **inode** and do not move at all. At `namespace` level that is one `RENAME` in one slot, regardless of how many children it holds.
- `type` is duplicated from metadata **because it is immutable**: a name's type never changes after creation, so the listing is authoritative for type checks (`unlink` and `rmdir` refuse the wrong kind without a second read and without a read-then-delete race), while metadata is the authority for everything mutable. At `namespace` level nothing repairs a disagreement: every mutation is one script, so a disagreement can only come from outside the mount, and no path rewrites a field behind a reader's back. At `dir` and `file` level the repair pass rewrites a field from the metadata on demand (see Link operations), so story 21 is met there and deliberately not at `namespace` level.
- Permissions and timestamps are **never** duplicated, and the size is not stored at all: it is the length of the content key, so a write has no second record to keep in step.
- Permissions/timestamps are **never** duplicated: they are mutable, and a duplicate would require every `chmod`/`write`/`setattr` to rewrite the parent's hash — a write that is not addressable from attribute-update paths.
- Metadata continues to carry the inode number and permission bits. Removing the inode from metadata was rejected: it would turn `lookup` (currently one read) into two, and it saves only ~23 bytes of a ~137-byte document.
- An empty hash simply does not exist; no placeholder member, no create-then-delete pair.

### Sharding

Every key is `kvfs/<infix>/<suffix>`, and the infix is always `<namespace>/<parent id>/<basename>`. The level decides how much of the infix the braces wrap. Nothing else about a key changes: the infix, the suffixes and the escaping stay identical. What differs is what an operation must touch, and at `dir` and `file` level that is enough to need the link protocol below.

| level | tag | a key of the root's listing | what shares a slot |
|---|---|---|---|
| `namespace` *(default)* | `{<ns>}` | `kvfs/{prod}/0/./dir` | the entire namespace |
| `dir` | `{<ns>/<parent id>}` | `kvfs/{prod/0}/./dir` | every entry with one parent id, which is the entries of one directory |
| `file` | `{<ns>/<parent id>/<name>}` | `kvfs/{prod/0/.}/dir` | one entry: its metadata, its content, and its listing if it is a directory |

For the entry `README.md` under parent `1` of namespace `default`, the three levels write `kvfs/{default}/1/README.md/data`, `kvfs/{default/1}/README.md/data` and `kvfs/{default/1/README.md}/data`.

- **`dir` level** puts the entries of one directory in one slot, because they share a parent id. A directory's own listing is addressed by the directory's location, so it lives in the slot of the directory's parent, while the directory's children live in the slot named by the directory's inode. A link therefore spans two slots, the listing's and the entry's, and runs the protocol below. What the level buys is the move inside one parent: the source and destination keys share a slot, so a rename keeps its keys and moves no byte.
- **`file` level** gives every entry its own slot: a directory's `meta`, `data` and `dir` share `{<ns>/<parent id>/<name>}`, and each of its children lives in its own. `lookup`, `stat` and `readdir` remain one round trip, and no listing ever spans servers.
- At every level the counter, the pub-sub channel and the root's metadata share the root's infix, so they share one slot, and the channel name travels to scripts as an argument rather than as a key.
- What the levels cost, stated once and precisely:

  | operation | `namespace` | `dir` | `file` |
  |---|---|---|---|
  | `lookup` / `stat` / `readdir` | 1 round trip | 1 round trip | 1 round trip |
  | create / mkdir / symlink | one script, atomic | 4 round trips, 3 slots (the counter, the parent's listing, the entry) | 4 round trips, 3 slots |
  | unlink / rmdir | one script, atomic | 3 round trips, 2 slots | 3 round trips, 2 slots |
  | rename, same parent | one script, no bytes moved, no child touched | 4 round trips, 2 slots: keys renamed, no bytes moved | 6 round trips, 3 slots: the entry is copied |
  | rename, across parents | one script, atomic | 7 round trips, 4 slots: the entry is copied | 7 round trips, 4 slots: the entry is copied |

  A copy is physics, not an implementation gap. At `file` level an entry's tag contains its name, so a new name is a new slot. At `dir` level the tag contains the parent id, so a new parent is a new slot. `RENAME` is refused across slots. A copy is not constant: a file's content grows with the file, and a directory's listing, copied in one `HSET`, grows with the directory's children. It is bounded by the entry being moved, not by its siblings. If that ever costs more than the spread is worth, the alternative is to tag an entry's slot by its inode, which stays stable across renames at the price of a two-round-trip `lookup`. In round trips the same work costs: create 4 (allocate, claim, prepare, settle), unlink and rmdir 3 (read the field, clear the entry, commit the field), a copying rename 7 across parents (read the source field, claim the destination field, copy the entry out, copy it in, clear the source entry, commit the source field, settle the destination), a copying rename inside one parent 6 and a key-moving rename inside one parent 4 (read the source field, claim the destination field, rename the keys, then one script that removes the source field and settles the destination, which can be one step because both fields are in the same listing). The counter is a slot of its own, so a create allocates in the namespace slot and then commits in the parent's.
- The level is a mount flag, `-shard-level=namespace|dir|file`, and is never stored: it is inferred at mount from where the namespace's counter key lives, so a namespace written at one level cannot be silently read at another. The default is `namespace`. A value that names none of the three, including `directory`, is refused before any key is read.

### Link operations at `dir` and `file` level

A link (create, unlink, rename, mkdir, rmdir) writes the parent's listing in one slot and the entry's own keys in another, and a rename writes two entry slots. None of them can be a single script. The keys of a rename within one parent at `dir` level do share a slot and are renamed by one script, but the parent's listing is in another slot, so that rename still runs the protocol around the script. A link is written as **claim → prepare → settle**, with the parent's listing as the commit point and the listing value carrying the state:

1. **Claim**: insert the field into the parent's listing with a trailing marker, `f:12:p` or `d:12:p`. A set-if-absent insert decides an exclusive create and makes a no-replace rename fail when the destination name is taken. A replacing rename that finds the name taken takes the field over instead (see Rename onto an existing name). The field goes **first**, before the entry is written, because it is the exclusive decision: a client that loses the race writes nothing and can never overwrite the winner's metadata. The price is that a refused create may spend an inode number, because the allocator lives in the namespace slot and cannot be tested in the same atomic step.
2. **Prepare**: write the entry's own keys in its slot: `meta`, `data`, and no listing key for a directory (an empty listing is absent, never created).
3. **Settle**: rewrite the field without the marker (`f:12`) and publish the invalidation. Every step that changes a field is a compare-and-set on the value it expects, so a client that lost a race cannot overwrite the winner's field. A settle that finds the field already resolved reports `ENOENT` instead of success.

The destination field of a rename names its source, `f:12:p:<src parent id>/<escaped src name>`, so repair can tell a rename that never took the source from one that did.

Readers never take a second round trip to answer a listing. `readdir` reports a marked field as unknown type rather than hiding it or trusting it, and `lookup` keeps resolving names from metadata alone, so a marked field whose metadata is already complete resolves normally.

**A pending name belongs to the client that claimed it.** `unlink`, `rmdir` and `rename` of a pending name fail with `EBUSY`, because deleting a name mid-commit can destroy another client's create or rename. A create or rename onto a pending name fails with `EEXIST`, like any taken name, even when the rename may replace. A pending name left by a crash stays busy until repair settles or removes it.

**Crash states.** A link operation removes the entry's own keys before it removes the field, and a rename copies the entry into the destination **in one atomic write** (one `HSET` for a directory's whole listing, one transaction for its metadata and content), so a partially copied listing is never observable. A crash leaves one of three states, and repair resolves one entry at a time without scanning a listing:

| state | what it means | repair |
|---|---|---|
| a field whose metadata is missing | an interrupted unlink or rmdir, or a rename that removed its source keys | remove the field |
| a pending field, complete metadata, no source address | a create that stopped before settle | settle the field |
| a pending field with a source address, complete metadata | a rename stopped before or after taking the source | if the source address still holds metadata with the same inode, the rename never took it, so delete the destination keys and field and keep the source; otherwise settle |

A rename inside one parent removes the source field and settles the destination in one script, because both fields are in the same listing hash, so the state between those two writes does not exist for it. Every entry's own keys are deleted by a single `DEL`, never one command per key, so a crash cannot strand the content or the listing of an entry whose metadata is already gone.

Repair is a per-entry call: it reads that entry's field and that entry's metadata, plus the source's metadata for a marked rename, and is idempotent, so repeating it after a crash is safe.

**Who runs it.**

- A `lookup` that misses. A name that is not listed costs one field read. A settled field with no metadata is removed before the miss is answered. A pending field is left alone on a miss, because it may be a create between its claim and its prepare.
- The mount's background loop. A pending field it saw in a listing, or refused as busy, is repaired once it has held the same value for ten operation timeouts, as a compare-and-set on that value. A link operation is a few round trips bounded by the timeout, so a client that is still working is never undone. Repair is never run per child during `readdir`, which is what keeps a listing at one round trip.
- An operator, with `kvfs -repair=<path>`, which repairs every component of the path and exits. The operator asserts that nothing is in flight on that path.

**Rename onto an existing name.** A rename replaces a taken destination as `rename(2)` does, at every level:

| source | destination | result |
|---|---|---|
| file or symlink | file or symlink | the destination is replaced |
| directory | empty directory | the destination is replaced |
| directory | non-empty directory | `ENOTEMPTY` |
| directory | file or symlink | `ENOTDIR` |
| file or symlink | directory | `EISDIR` |

`RENAME_NOREPLACE` makes any taken name `EEXIST`. `RENAME_EXCHANGE` and `RENAME_WHITEOUT` are not supported and fail with `EINVAL`. At `namespace` level the checks and the replacement are in the one rename script, so it is still one round trip. At `dir` and `file` level a taken name is replaced by a compare-and-set **takeover** of the destination field: the field is read, the types are checked, a destination directory is emptied and deleted by the atomic empty-and-delete script, and the field is overwritten with the rename's pending marker only while it still holds the value that was read. The copy that follows overwrites the destination's metadata and clears its content key, so nothing of the replaced entry survives. A replacing rename costs two more round trips than a rename onto a free name (the failed claim is wasted, then the field is read and taken over), and three more when the destination is a directory. A crash after the takeover and before the settle leaves the replaced entry gone and the source still in place, because repair discards the half-made destination; a finished rename would have destroyed the replaced entry as well.

**Lost races.** A rename whose source field changed under it, because another rename or an unlink took the name, deletes its destination and fails with `ENOENT`. Of two renames of one source, exactly one wins. The same holds for the unlink/rename interleavings on one name: at most one outcome survives, and no listed name is left without metadata after repair.

**No orphaned keys.** A directory's metadata and listing share a slot at every level, and the scripts that decide whether a child may be written read that metadata. `rmdir` deletes the metadata and the listing in one script together with the emptiness test. A create, mkdir, symlink or rename into a directory checks, in the script that claims the name, that the parent's metadata still exists and still carries the inode the caller holds. A creator that loses to `rmdir`, or that holds a directory a rename replaced, is refused with `ENOENT` and writes nothing. A creator that wins makes `rmdir` fail with `ENOTEMPTY`, because its pending field counts as an entry. A replacing rename clears the destination directory with the same atomic empty-and-delete script before it takes the name over.

The mount repairs through the three paths above and nowhere else.

### Operations

- **`readdir`**: one `HGETALL` on the directory's hash, addressed by the directory's own location — the `(parent id, name)` pair that `lookup` already resolved to reach it. So a cold `ls` of a directory costs one lookup plus one listing; a warm one (inside the entry timeout) costs only the listing. No metadata is read and no JSON is decoded. Unknown type code → unknown type, entry still emitted.
- **`lookup`**: unchanged — one metadata read by (`parent id`, basename). The inode comes from the metadata document; the hash is not consulted.
- **`stat`/`getattr`**: unchanged — one metadata read.
- **create / mkdir / symlink**: allocate id, write metadata (and data), insert one hash field, all in one transaction. At `dir` and `file` level the same steps split across slots and run as claim → prepare → settle: the field can only be claimed before the entry is written, because a set-if-absent insert is the exclusive decision.
- A regular file always has a content key — the empty string when the file is empty — so a move never needs an existence branch, and presence never encodes emptiness.
- **rename / move (single direct child)**: one Lua script on the slot owner — the destination check (a taken name is refused, or replaced under the rules of Rename onto an existing name), `RENAME` of the metadata key, `RENAME` of the data key (files/links), field move in the source and destination hashes, then publish. No bytes are read or rewritten; no per-child commands. This is the `namespace` path. At `dir` level a rename within one parent keeps its keys in one slot, so the keys are renamed and no byte is read. Any other move, and every move at `file` level (the new name is a new slot), is the claim → prepare → settle protocol and the entry is copied.
- **rename / move (directory)**: the same script plus a `RENAME` of the directory's own listing; no child is touched, because children are addressed by the directory's inode, which does not change. At `dir` level within one parent the listing is renamed together with the metadata. At `file` level, and at `dir` level across parents, the listing moves to a new slot and is copied in one atomic write, so no reader ever sees half of it.
- **unlink / rmdir**: field delete plus key delete in one transaction; emptiness is an `HLEN`/`HEXISTS`-style count, never a full member fetch. The type check reads the listing's immutable code inside the same script, so there is no read-then-delete window and one round trip suffices. At `dir` and `file` level both are link operations over two slots and use the same claim → prepare → settle protocol: the listing's code is read first (at most one extra round trip, and only when the field is still pending), the entry's keys are removed, and the field delete is the commit point.
- **A renamed directory's address changes with its name.** A node holds its own `(parent id, basename)` pair — that pair *is* the listing's address — so a rename rewrites the pair on the kernel's node for the moved inode. Without that, the next `readdir` would address the old, now absent, listing.
- **file content (read, write, truncate, stat)**: a file's content is one string at its `data` key and its size is that string's length, so the metadata document carries no size. A read is one `GETRANGE`. A write is one script that checks the entry's metadata still exists and then runs one `SETRANGE`, so a write through a handle whose file was unlinked is refused and cannot recreate the key. A write's cost is the bytes written and never the size of the file, and a handle holds no copy of the content, so any number of handles, on one mount or several, read the same bytes. `stat` reads the metadata and the content length (`STRLEN`, constant time) in one pipelined round trip. The modification time is stamped once, when a handle flushes or closes after writing, and that stamp publishes the invalidation. Truncate to a smaller size rewrites the bytes kept; a larger size pads with zero bytes. A write at a large offset into a short file allocates the zero fill up to that offset.
- **create with exclusivity**: the destination field is tested inside the same script that allocates the id and writes the entry; a taken name returns before any write and before any allocation. Two concurrent creators cannot both succeed and the earlier check-then-write race is closed. At `dir` and `file` level the test is the claim itself, so a refused create writes nothing and still spends the inode number it allocated: the counter is in another slot and cannot be checked atomically with the name.
- **Invalidation is published after every successful mutation.** The mount publishes the affected address on `kvfs/<ns>/0/./pubsub` — `<parent id>/<encoded basename>`, the same pair the key grammar uses — so a client on another host can drop exactly that entry, and the cached attributes and bytes of its file, from its kernel cache and let the next access re-read them. A read-only open keeps file bytes in the kernel page cache (`FOPEN_KEEP_CACHE`). A writable open bypasses it (`FOPEN_DIRECT_IO`), so each write the caller makes reaches the content key as one request. A rename publishes both the old and the new address; a directory operation publishes the directory's own address, because that is the listing that changed.
- Publishing is best-effort and never on another operation's critical path: at `namespace` level `PUBLISH` rides inside the mutation script itself, and a subscriber hands each address to a background loop rather than calling `NotifyEntry` from the request path, which deadlocks. The loop drops the entry (`NotifyEntry`) and the child's cached attributes and bytes (`NotifyContent(0, 0)`), and it waits when busy rather than dropping a message. The 1-second attribute and entry timeouts bound how long a missed message can leave an entry or its attributes stale. Cached bytes have no timeout: a missed message can leave them stale until the kernel drops the pages.
- At `dir` and `file` level the publish happens at settle, so an entry is announced once, with its final value, and a pending field is never announced.

### Worked example: one session from an empty namespace

Namespace `default` at `namespace` level, so every key below shares one slot and each numbered step is one round trip. Bootstrap has run: the counter is `1` — the root consumed the first allocation — and the root exists with id `1`. No listing key exists yet, because an empty hash is the absence of a key. Metadata is abbreviated to the fields that carry the decision.

Each step is one script (or one transaction) on the namespace slot. The exclusivity check comes before the `INCR`, so a refused create does not burn an id.

**1. create `README.md` in the root, content `Hello, world!`** — takes id `2`

```
HEXISTS kvfs/default/0/./dir README.md                → 0
INCR    kvfs/default/0/./counter                      → 2
SET     kvfs/default/1/README.md/meta {"id":2,"type":"file","mode":420,…}
SET     kvfs/default/1/README.md/data "Hello, world!"
HSET    kvfs/default/0/./dir README.md f:2
PUBLISH kvfs/default/0/./pubsub 1/README.md
```

**2. create directory `sub1` in the root** — id `3`

```
INCR kvfs/default/0/./counter → 3
SET  kvfs/default/1/sub1/meta {"id":3,"type":"dir","mode":493,…}
HSET kvfs/default/0/./dir sub1 d:3
PUBLISH kvfs/default/0/./pubsub 1/sub1
```

No `kvfs/default/1/sub1/dir` is written: the directory exists as a link, and its listing materializes with its first child.

**3. create `README.md` inside `sub1`** — id `4`

```
INCR kvfs/default/0/./counter → 4
SET  kvfs/default/3/README.md/meta {"id":4,"type":"file","mode":420,…}
SET  kvfs/default/3/README.md/data ""
HSET kvfs/default/1/sub1/dir README.md f:4
PUBLISH kvfs/default/0/./pubsub 3/README.md
```

The same basename as step 1 under a different owner (`3` instead of `1`), so the keys cannot collide. The listing is addressed by `sub1`'s **location** — `<parent 1>/<name sub1>/dir` — while `sub1`'s children are addressed by its **inode** (`3`). A regular file always has a content key — the empty string when the size is `0` — so a later move needs no existence branch.

**4. create file `sub2` inside `sub1`** — id `5`

```
INCR kvfs/default/0/./counter → 5
SET  kvfs/default/3/sub2/meta {"id":5,"type":"file","mode":420,…}
SET  kvfs/default/3/sub2/data ""
HSET kvfs/default/1/sub1/dir sub2 f:5
PUBLISH kvfs/default/0/./pubsub 3/sub2
```

**5. rename `sub1` to `mydir`** — the directory already holds two children

```
EVAL  -- one script, slot {default} (pseudo-Lua)
  if HGET    kvfs/default/0/./dir sub1 is nil    then return 'missing' end
  if HEXISTS kvfs/default/0/./dir mydir == 1     then return 'exists'  end
  RENAME kvfs/default/1/sub1/meta kvfs/default/1/mydir/meta
  RENAME kvfs/default/1/sub1/dir  kvfs/default/1/mydir/dir
  HDEL   kvfs/default/0/./dir sub1
  HSET   kvfs/default/0/./dir mydir d:3
  PUBLISH kvfs/default/0/./pubsub 1/sub1
  PUBLISH kvfs/default/0/./pubsub 1/mydir
```

The field's value (`d:3`) is read by the script and written back verbatim, so the caller needs no read before the rename. A file would instead rename its `meta` and `data` keys; a directory renames `meta` and `dir`.

Commands issued against the two children: **zero**. Their keys (`kvfs/default/3/…`) are untouched, because they are addressed by the directory's inode and the inode does not change. Only `sub1`'s own two keys move — metadata and listing — plus two fields in the root's listing. The predecessor rebuilt a child set and re-added every name here; this is two keys renamed and two fields changed. The moved directory's `st_ino` is still `3`.

**State after the session**

```
kvfs/default/0/./counter              5
kvfs/default/0/./meta                 {"id":1,"type":"dir",…}
kvfs/default/0/./dir                  README.md f:2, mydir d:3
kvfs/default/1/README.md/{meta,data}  id 2, "Hello, world!"
kvfs/default/1/mydir/meta             id 3
kvfs/default/1/mydir/dir              README.md f:4, sub2 f:5
kvfs/default/3/README.md/{meta,data}  id 4, ""
kvfs/default/3/sub2/{meta,data}       id 5, ""
```

**Cost**

| step | round trips | effect |
|---|---|---|
| bootstrap | 2 | one pipelined probe of both placements, then one script at the configured slot |
| 1 create `README.md` | 1 | id 2, root listing gains `README.md f:2` |
| 2 mkdir `sub1` | 1 | id 3, root listing gains `sub1 d:3` |
| 3 create `sub1/README.md` | 1 | id 4, `sub1`'s listing key created |
| 4 create `sub1/sub2` | 1 | id 5 |
| 5 rename `sub1` → `mydir` | 1 | two keys renamed, two fields changed, no child touched |
| **total** | **7** | 5 ids allocated (1 root + 4 creations), counter = 5 |

A listing at every level is one `HGETALL`. At `file` level the same session runs with `{default/1/README.md}`, `{default/1/sub1}` and `{default/3/README.md}` tags: steps 1–4 each cost four round trips (allocate, claim, prepare, settle) and span the namespace counter's shard, the parent's listing, and the entry; step 5 cannot rename keys at all — `sub1`'s keys are copied into the `mydir` shard in six round trips, touching three shards in total. That is the smallest case in which the finer level's price is visible. At `dir` level the keys read `kvfs/{default/1}/README.md/…` and `kvfs/{default/0}/./dir`: steps 1 to 4 cost four round trips each, and step 5 renames the two keys in one script and moves the two fields in another, four round trips, with no child touched.

### Initialization

- No compatibility path with the previous layout: it was never deployed, so the schema starts clean — no dual-read, no dual-write, no converter, no `WRONGTYPE` tolerance.
- **The sharding level is inferred, never stored.** A stamp would be a second source of truth that can disagree with the keys it describes, and it can only ever be written once — so it buys nothing the layout does not already say. Where the counter key lives *is* the level; reading it back is a verification, not an assumption.
- Bootstrap reads the counter key at **all three placements** (`namespace`, `dir` and `file`) in **one pipelined round trip** (client-side fan-out, so one round trip regardless of which level this mount was configured for), and decides from what exists:
  - **none** — fresh namespace: one script that returns early if the counter already exists, and otherwise sets the counter to `1` and writes the root metadata with that id. The script is the single-winner decision, the root id is always `1`, and the namespace ends up initialized at exactly one level.
  - **exactly one, matching the configured level** — that placement is the level; proceed.
  - **exactly one, differing from the configured level** — refuse with an error naming both levels. Adopting it would mix key placements into a namespace the mount will address differently, and re-initializing would silently fork it.
  - **more than one** — the namespace was written at two levels; refuse. There are two independent counters, so inode numbers from each can collide, and no automatic repair is safe.
- A level that names none of the three placements, including `directory`, is rejected as configuration before any key is read.
- A namespace is identified solely by its `kvfs/<ns>/` prefix. Nothing scans that prefix to interpret foreign keys: the mount reads only the keys its grammar builds, and an unexpected shape where a counter or root must be is a startup error rather than data to adopt.
- A future breaking change takes a new prefix (`kvfs_v2/`); the `kvfs/` layout is never reinterpreted in place.

## Testing Decisions

**What makes a good test here.** Tests assert externally visible behavior: the key a value is stored under, the result of a filesystem operation, the number of Redis commands a call issues, and what another client observes. Tests never assert private helper structure, and never assert the internal order of commands inside a transaction.

**Seams** (highest available, two total, one already exists):

1. **Store public surface against an in-memory Redis** — the existing seam. Prior art: the current store tests already run the real client against `miniredis` with a shared bootstrap helper. Every operation decision above (readdir result, rename semantics, exclusivity, emptiness, inode stability, level inference and both refusal cases — mismatch and fork) is asserted here. An instrumented client wrapper records issued commands so the complexity commitments can be asserted directly: `readdir` = 1 command; file move = 1 script at `namespace` level and no content command at `dir` level within one parent; directory rename = 1 script and zero per-child commands at `namespace` level; `rmdir` = no member fetch; bootstrap = one round trip of counter probes; every mutation publishes exactly one address, and a rename publishes two (old and new). Configuration is asserted separately and without Redis: `directory` and any unknown level are refused before any key is read.

   At `dir` and `file` level the same seam asserts the link protocol's states directly, since an in-process fake cannot be interrupted mid-script: a prepared entry with no listing field is invisible; a marked field with complete metadata settles to a plain field on repair; a marked field with missing metadata is removed; a marked rename destination whose source still owns the inode is discarded, and one whose source is gone is settled; unlink, rmdir and rename of a pending name are refused as busy; two renames of one source leave one winner; repair is idempotent when repeated; and a destination listing is written in one atomic `HSET`, so a partially copied listing is never observable. Each case asserts that repair reads only the affected entry.
2. **Key builder as a pure function** — new, but the highest possible place for the schema contract: a golden table of (`namespace`, level, parent id, basename, suffix) → exact byte string, plus round-trip property tests over names containing every escaped character, spaces, colons, `.`/`..`-like names, unicode, and empty suffix. This is the only acceptable place to encode schema expectations.

A crash-point test uses the same seam. A client wrapper lets a fixed number of commands through and fails every later one on a cancelled context, as a process that died between two commands would, and a `MULTI`/`EXEC` batch counts as one command. For every operation at every level, and for every step budget until the operation finishes, a second store repairs every listed name, and the result must be either the state before the operation or the state after it (or, for a rename that replaces an entry, the state without the replaced entry), with no unresolved name and no key that no reachable entry owns.

No new FUSE-level seam is proposed for automated tests: a mount-level test needs `/dev/fuse` and rootless mounting is environment-dependent. The existing README smoke checklist is extended instead (list a large directory, `stat` an entry, rename across directories, verify inode stability, verify cross-mount visibility) and treated as a release gate, not a unit test. `scripts/smoke.sh` runs it at one level against real mounts, and the slot-safety and cluster contracts have their own checks: a unit test asserts that every script and every transaction the store sends names keys from one slot, and an opt-in test runs the same operations against a live cluster.

**Modules under test:** the key builder, the store, and the filesystem layer only where it maps hash values to kernel directory entries (covered by the store seam plus the smoke checklist).

## Out of Scope

- Hardlinks (a name→inode map makes them possible later, but link counts, `Nlink` semantics, and unlink-on-last-close are not part of this change).
- Automatic rebalancing, and changing a namespace's sharding level after initialization.
- Compatibility with the previous (never-deployed) layout: no converter, no dual-read window, and no support for reading a first-generation key as a second-generation one.
- A migration path to a *successor* layout. That is deferred, not rejected — it becomes a requirement at the next breaking change (see Further Notes), which is a different boundary from the predecessor above.
- Extended attributes, ACLs, POSIX locks, quotas, and file sealing.
- Large-file chunking, compression, encryption at rest, and deduplication.
- Cross-namespace operations, snapshots, and point-in-time restore.
- Windows, macOS/FUSE-T, and FUSE 2 support.
- Changing attribute/entry timeouts or the pub-sub invalidation architecture.

## Further Notes

- Vocabulary, used the same way in code, tooling and this document:
  - **namespace**: the tenant identifier, `[A-Za-z0-9-]+`, the first field after `kvfs/`.
  - **parent id**: the inode number of the directory that owns an entry. Never a name.
  - **basename**: the one free-form field of a key, percent-encoded by the escaping rule.
  - **suffix**: the record of an entry, one of `counter`, `pubsub`, `meta`, `dir`, `data`.
  - **infix**: `<namespace>/<parent id>/<basename>`.
  - **sharding level**: `namespace`, `dir` or `file`; where the braces of the hash tag go.
  - **type code**: the one-character `f`, `d` or `l` in a listing value.
  - **link operation**: create, mkdir, symlink, unlink, rmdir or rename, a change to a listing and an entry together.
  - **pending field**: a listing value with a `:p` marker, claimed and not yet settled.
  - **repair**: the per-entry pass that settles, removes or discards a name.

- Resolved questions carried over from the design draft: the root node uses id `1` (FUSE's root convention) and lives at `<parent 0>/<name .>`; namespace bookkeeping stays under owner `0`; the schema uses `parent id` as the identifier name everywhere, and that vocabulary is recorded for tooling.
- Rejected for root identity: **`1` for both** (root's own record and its parent) would match `FUSE_ROOT_ID` on paper, but makes root its own parent — a cycle for any tool that walks upward — and merges namespace scope into root scope. **`0` for both** fails outright: inode `0` is not a valid POSIX inode (`d_ino == 0` is how readdir says "no entry"), and the FUSE root is `1` by convention. Keeping `0` as the namespace node and `1` as root leaves root an ordinary child of the namespace, with no special case anywhere in the key builder.
- Rejected: a sharding-level stamp key. It would be written once and never change, and it restates a fact the key placement already encodes; inferring the level from where the counter key exists verifies the layout instead of trusting a claim about it.
- Rejected: removing the inode and mode from the metadata JSON. The measured saving is 23 of 137 bytes and the cost is an extra round trip on `lookup` and `stat`, the two hottest operations.
- Rejected: storing permission bits in the directory hash. Permissions change under `chmod`, and the hash field is not addressable from the attribute-update path; only immutable facts belong in the value.
- Deliberate: **one infix for every suffix**, so a directory's listing is addressed exactly like its metadata and content. The alternative — keying the listing by the directory's inode — keeps a listing in one slot across renames, which is the only thing it buys at `file` level, and it costs a second addressing scheme: `<dir id>/.` would be the one key whose owner is not its parent.
- Consequence, accepted with the choice above: at `file` level a directory rename copies its listing, in one `HSET`, and a file rename copies its content, because an entry's tag contains its name. At `dir` level the same copies happen only across parents. At the default level, and within one parent at `dir` level, both are `RENAME`s in one slot.
- Rejected: **fully inode-addressed storage** (metadata, content and listing all under `<id>/…`). It turns every rename into field edits that stay in one slot, but makes `lookup` two sequential round trips — read the parent's field, then the metadata — and `lookup` is the hottest path in the filesystem.
- Deferred: **inode-tagged entry shards** at `file` level (`{<ns>/<inode>}/…`), which would make renames cheap again. Not adopted because the tag would be derived from the inode, and the inode is only known *after* reading the parent's listing — every `lookup` would cost two round trips to save a rename, the rarer operation.
- Performance commitments to verify in benchmarks: at the default level, `readdir` of a 10k-entry directory is one round trip and returns ~10 bytes per entry instead of ~137 bytes plus one round trip each, directory rename of a 10k-entry directory issues a constant number of commands, and a file move transfers no file body. At `dir` and `file` level the same reads hold; a rename that crosses slots becomes a copy of the entry: the file's content, or the directory's listing, which grows with the children of the directory being moved but not with the size of the directory it moves into or out of.
- Operational note: the server-side scripting requirement (single-slot Lua) is a deployment prerequisite for this implementation. The contract itself does not need scripts. At `namespace` level the Python implementation runs each mutation as an optimistic `WATCH` / `MULTI` / `EXEC` transaction on a server that disallows scripts (`--sss-mode=no`, or `auto` on such a server). It writes the same keys with the same values and the same publish payloads, at more round trips per mutation (see its README). A namespace written by one of them mounts unchanged with the other.
- Implementation order: `namespace` level first, because it satisfies every read-path and atomicity commitment without a repair pass. The link protocol and repair pass then serve `dir` and `file` level, which differ from each other only in the key builder and in the same-parent rename.
- Deferred requirement, next breaking change only: an **offline migration path from this layout to `kvfs_v2/`**. Its shape is the one already sketched — stop writers, unmount, read the surviving keys, rebuild each directory hash from metadata (a listing carries only immutable facts, so it can always be regenerated rather than trusted), carry inode numbers across unchanged so `st_ino` stays stable, verify entry counts, then mount the new binary. It applies **between this generation and the one after it**; there is nothing to migrate from the never-deployed legacy layout, and nothing in this phase should be built to read it.
- The `kvfs_v2/` prefix is the reserved name for that change. A future schema must never be crowded into `kvfs/` by reinterpreting existing keys in place; the migration note above assumes a clean prefix-to-prefix move.
- Publishing to the project issue tracker was not possible in this environment (the repository has no configured remote); the spec lives here until a tracker and label vocabulary are configured with `/setup-matt-pocock-skills`.
