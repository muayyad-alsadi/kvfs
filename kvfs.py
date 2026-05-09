#!/usr/bin/env python3
import argparse
import asyncio
import errno
import os
import stat
import time

import pyfuse3
import pyfuse3.asyncio
from redis import asyncio as aioredis


META_FIELDS = ("type", "mode", "uid", "gid", "size", "atime_ns", "mtime_ns",
               "ctime_ns", "nlink", "ino", "version")


def now_ns():
    return time.time_ns()


def norm(path: str) -> str:
    path = os.path.normpath(path or "/")
    if not path.startswith("/"):
        path = "/" + path
    return path


def split_parent(path: str):
    path = norm(path)
    if path == "/":
        return "/", ""
    return os.path.dirname(path) or "/", os.path.basename(path)


class KVFS(pyfuse3.Operations):
    supports_dot_lookup = True
    enable_writeback_cache = False

    def __init__(self, redis_url, entry_timeout=0.25, attr_timeout=0.25,
                 waitaof=False, wait_timeout_ms=5000):
        super().__init__()
        self.redis = aioredis.from_url(redis_url)
        self.entry_timeout = entry_timeout
        self.attr_timeout = attr_timeout
        self.waitaof = waitaof
        self.wait_timeout_ms = wait_timeout_ms

        self.path_to_ino = {"/": pyfuse3.ROOT_INODE}
        self.ino_to_path = {pyfuse3.ROOT_INODE: "/"}
        self.lookup_count = {pyfuse3.ROOT_INODE: 1}

        self.dir_handles = {}
        self.open_files = {}  # fh -> {"path","buf","dirty","lock"}
        self.next_fh = 100
        self.lock_guard = asyncio.Lock()
        self.path_locks = {}

    def _meta_key(self, path): return f"fs:meta:{norm(path)}"
    def _data_key(self, path): return f"fs:data:{norm(path)}"
    def _dir_key(self, path):  return f"fs:dir:{norm(path)}"

    async def _ensure_root(self):
        if await self.redis.exists(self._meta_key("/")):
            return
        t = now_ns()
        root = {
            "type": "dir",
            "mode": stat.S_IFDIR | 0o755,
            "uid": os.getuid(),
            "gid": os.getgid(),
            "size": 0,
            "atime_ns": t,
            "mtime_ns": t,
            "ctime_ns": t,
            "nlink": 2,
            "ino": int(pyfuse3.ROOT_INODE),
            "version": 1,
        }
        async with self.redis.pipeline(transaction=True) as pipe:
            await pipe.hset(self._meta_key("/"), mapping=root)
            await pipe.delete(self._dir_key("/"))
            await pipe.execute()

    async def _path_lock(self, path):
        path = norm(path)
        async with self.lock_guard:
            if path not in self.path_locks:
                self.path_locks[path] = asyncio.Lock()
            return self.path_locks[path]

    async def _get_meta(self, path):
        row = await self.redis.hmget(self._meta_key(path), *META_FIELDS)
        if not row or row[0] is None:
            return None
        vals = {k: row[i] for i, k in enumerate(META_FIELDS)}
        return {
            "path": norm(path),
            "type": vals["type"].decode() if isinstance(vals["type"], (bytes, bytearray)) else vals["type"],
            "mode": int(vals["mode"]),
            "uid": int(vals["uid"]),
            "gid": int(vals["gid"]),
            "size": int(vals["size"]),
            "atime_ns": int(vals["atime_ns"]),
            "mtime_ns": int(vals["mtime_ns"]),
            "ctime_ns": int(vals["ctime_ns"]),
            "nlink": int(vals["nlink"]),
            "ino": int(vals["ino"]),
            "version": int(vals["version"]),
        }

    def _mapping(self, m):
        return {k: int(v) if isinstance(v, bool) or isinstance(v, int) else v
                for k, v in m.items() if k != "path"}

    def _entry(self, m):
        e = pyfuse3.EntryAttributes()
        e.st_ino = int(m["ino"])
        e.st_mode = int(m["mode"])
        e.st_uid = int(m["uid"])
        e.st_gid = int(m["gid"])
        e.st_nlink = int(m["nlink"])
        e.st_size = int(m["size"])
        e.st_atime_ns = int(m["atime_ns"])
        e.st_mtime_ns = int(m["mtime_ns"])
        e.st_ctime_ns = int(m["ctime_ns"])
        e.st_blksize = 4096
        e.st_blocks = (int(m["size"]) + 511) // 512
        e.attr_timeout = self.attr_timeout
        e.entry_timeout = self.entry_timeout
        return e

    def _remember(self, path, ino):
        path = norm(path)
        self.path_to_ino[path] = int(ino)
        self.ino_to_path[int(ino)] = path
        self.lookup_count[int(ino)] = self.lookup_count.get(int(ino), 0)

    async def _child_path(self, parent_inode, name: bytes):
        parent = self.ino_to_path.get(int(parent_inode))
        if parent is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        text = os.fsdecode(name)
        if text == ".":
            return parent
        if text == "..":
            return "/" if parent == "/" else os.path.dirname(parent) or "/"
        return norm(os.path.join(parent, text))

    async def _alloc_fh(self, path):
        self.next_fh += 1
        fh = self.next_fh
        self.open_files[fh] = {"path": norm(path), "buf": None, "dirty": False, "lock": asyncio.Lock()}
        return fh

    async def _load_buf(self, fh):
        h = self.open_files[fh]
        if h["buf"] is None:
            blob = await self.redis.get(self._data_key(h["path"])) or b""
            h["buf"] = bytearray(blob)

    async def _flush_fh(self, fh, datasync=False):
        h = self.open_files[fh]
        async with h["lock"]:
            if not h["dirty"]:
                if self.waitaof:
                    try:
                        await self.redis.execute_command("WAITAOF", 1, 0, self.wait_timeout_ms)
                    except Exception:
                        pass
                return
            meta = await self._get_meta(h["path"])
            if meta is None:
                raise pyfuse3.FUSEError(errno.ENOENT)
            payload = bytes(h["buf"] or b"")
            t = now_ns()
            meta["size"] = len(payload)
            meta["mtime_ns"] = t
            meta["atime_ns"] = t
            if not datasync:
                meta["ctime_ns"] = t
            meta["version"] += 1
            async with self.redis.pipeline(transaction=True) as pipe:
                await pipe.set(self._data_key(h["path"]), payload)
                await pipe.hset(self._meta_key(h["path"]), mapping=self._mapping(meta))
                await pipe.execute()
            h["dirty"] = False
            if self.waitaof:
                try:
                    await self.redis.execute_command("WAITAOF", 1, 0, self.wait_timeout_ms)
                except Exception:
                    pass

    async def init(self):
        await self._ensure_root()

    async def lookup(self, parent_inode, name, ctx=None):
        path = await self._child_path(parent_inode, name)
        m = await self._get_meta(path)
        if m is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        self._remember(path, m["ino"])
        self.lookup_count[m["ino"]] = self.lookup_count.get(m["ino"], 0) + 1
        return self._entry(m)

    async def getattr(self, inode, ctx=None):
        path = self.ino_to_path.get(int(inode))
        if path is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        m = await self._get_meta(path)
        if m is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        return self._entry(m)

    async def opendir(self, inode, ctx):
        if int(inode) not in self.ino_to_path:
            raise pyfuse3.FUSEError(errno.ENOENT)
        self.next_fh += 1
        fh = self.next_fh
        self.dir_handles[fh] = self.ino_to_path[int(inode)]
        return fh

    async def readdir(self, fh, start_id, token):
        path = self.dir_handles[fh]
        names = await self.redis.smembers(self._dir_key(path))
        names = sorted(os.fsdecode(n) if isinstance(n, (bytes, bytearray)) else n for n in names)

        pipe = self.redis.pipeline(transaction=False)
        child_paths = []
        for name in names:
            child = norm(os.path.join(path, name))
            child_paths.append((name, child))
            pipe.hmget(self._meta_key(child), *META_FIELDS)
        rows = await pipe.execute()

        entries = []
        for (name, child), row in zip(child_paths, rows):
            if not row or row[0] is None:
                continue
            m = {
                "path": child,
                "type": row[0].decode() if isinstance(row[0], (bytes, bytearray)) else row[0],
                "mode": int(row[1]), "uid": int(row[2]), "gid": int(row[3]),
                "size": int(row[4]), "atime_ns": int(row[5]), "mtime_ns": int(row[6]),
                "ctime_ns": int(row[7]), "nlink": int(row[8]), "ino": int(row[9]),
                "version": int(row[10]),
            }
            entries.append((m["ino"], name, self._entry(m), child))

        entries.sort(key=lambda x: x[0])
        for ino, name, attr, child in entries:
            if ino <= start_id:
                continue
            ok = pyfuse3.readdir_reply(token, os.fsencode(name), attr, ino)
            if not ok:
                return
            self._remember(child, ino)
            self.lookup_count[ino] = self.lookup_count.get(ino, 0) + 1

    async def open(self, inode, flags, ctx):
        path = self.ino_to_path.get(int(inode))
        if path is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        fh = await self._alloc_fh(path)
        return pyfuse3.FileInfo(fh=fh)

    async def read(self, fh, off, size):
        h = self.open_files[fh]
        async with h["lock"]:
            if h["buf"] is not None:
                return bytes(h["buf"][off:off + size])
        return await self.redis.getrange(self._data_key(h["path"]), off, off + size - 1) or b""

    async def write(self, fh, off, buf):
        h = self.open_files[fh]
        async with h["lock"]:
            await self._load_buf(fh)
            end = off + len(buf)
            if end > len(h["buf"]):
                h["buf"].extend(b"\x00" * (end - len(h["buf"])))
            h["buf"][off:end] = buf
            h["dirty"] = True
            return len(buf)

    async def create(self, parent_inode, name, mode, flags, ctx):
        parent = self.ino_to_path.get(int(parent_inode))
        if parent is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        path = norm(os.path.join(parent, os.fsdecode(name)))
        lock_a = await self._path_lock(parent)
        lock_b = await self._path_lock(path)
        async with lock_a, lock_b:
            if await self.redis.exists(self._meta_key(path)):
                raise pyfuse3.FUSEError(errno.EEXIST)
            ino = abs(hash((path, now_ns()))) & ((1 << 63) - 1) or 2
            t = now_ns()
            meta = {
                "type": "file",
                "mode": int(stat.S_IFREG | mode),
                "uid": int(ctx.uid),
                "gid": int(ctx.gid),
                "size": 0,
                "atime_ns": t, "mtime_ns": t, "ctime_ns": t,
                "nlink": 1, "ino": ino, "version": 1
            }
            async with self.redis.pipeline(transaction=True) as pipe:
                await pipe.hset(self._meta_key(path), mapping=meta)
                await pipe.set(self._data_key(path), b"")
                await pipe.sadd(self._dir_key(parent), os.fsdecode(name))
                await pipe.execute()
            self._remember(path, ino)
            self.lookup_count[ino] = self.lookup_count.get(ino, 0) + 1
            fh = await self._alloc_fh(path)
            return pyfuse3.FileInfo(fh=fh), self._entry({**meta, "path": path})

    async def unlink(self, parent_inode, name, ctx):
        parent = self.ino_to_path.get(int(parent_inode))
        path = norm(os.path.join(parent, os.fsdecode(name)))
        lock_a = await self._path_lock(parent)
        lock_b = await self._path_lock(path)
        async with lock_a, lock_b:
            m = await self._get_meta(path)
            if m is None:
                raise pyfuse3.FUSEError(errno.ENOENT)
            if m["type"] != "file":
                raise pyfuse3.FUSEError(errno.EISDIR)
            async with self.redis.pipeline(transaction=True) as pipe:
                await pipe.delete(self._meta_key(path))
                await pipe.delete(self._data_key(path))
                await pipe.srem(self._dir_key(parent), os.fsdecode(name))
                await pipe.execute()
            self.path_to_ino.pop(path, None)

    async def mkdir(self, parent_inode, name, mode, ctx):
        parent = self.ino_to_path.get(int(parent_inode))
        path = norm(os.path.join(parent, os.fsdecode(name)))
        ino = abs(hash((path, now_ns()))) & ((1 << 63) - 1) or 2
        t = now_ns()
        meta = {
            "type": "dir",
            "mode": int(stat.S_IFDIR | mode),
            "uid": int(ctx.uid),
            "gid": int(ctx.gid),
            "size": 0,
            "atime_ns": t, "mtime_ns": t, "ctime_ns": t,
            "nlink": 2, "ino": ino, "version": 1
        }
        async with self.redis.pipeline(transaction=True) as pipe:
            await pipe.hset(self._meta_key(path), mapping=meta)
            await pipe.delete(self._dir_key(path))
            await pipe.sadd(self._dir_key(parent), os.fsdecode(name))
            await pipe.execute()
        self._remember(path, ino)
        self.lookup_count[ino] = self.lookup_count.get(ino, 0) + 1
        return self._entry({**meta, "path": path})

    async def rmdir(self, parent_inode, name, ctx):
        parent = self.ino_to_path.get(int(parent_inode))
        path = norm(os.path.join(parent, os.fsdecode(name)))
        if await self.redis.scard(self._dir_key(path)) != 0:
            raise pyfuse3.FUSEError(errno.ENOTEMPTY)
        async with self.redis.pipeline(transaction=True) as pipe:
            await pipe.delete(self._meta_key(path))
            await pipe.delete(self._dir_key(path))
            await pipe.srem(self._dir_key(parent), os.fsdecode(name))
            await pipe.execute()
        self.path_to_ino.pop(path, None)

    async def rename(self, parent_inode_old, name_old, parent_inode_new, name_new, flags, ctx):
        old_parent = self.ino_to_path.get(int(parent_inode_old))
        new_parent = self.ino_to_path.get(int(parent_inode_new))
        old = norm(os.path.join(old_parent, os.fsdecode(name_old)))
        new = norm(os.path.join(new_parent, os.fsdecode(name_new)))
        if old == new:
            return
        src = await self._get_meta(old)
        if src is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        dst = await self._get_meta(new)

        if src["type"] == "dir" and await self.redis.scard(self._dir_key(old)) != 0:
            raise pyfuse3.FUSEError(errno.ENOTSUP)

        if flags & pyfuse3.RENAME_NOREPLACE and dst is not None:
            raise pyfuse3.FUSEError(errno.EEXIST)
        if flags & pyfuse3.RENAME_EXCHANGE:
            raise pyfuse3.FUSEError(errno.ENOTSUP)

        async with self.redis.pipeline(transaction=True) as pipe:
            await pipe.rename(self._meta_key(old), self._meta_key(new))
            if src["type"] == "file" and await self.redis.exists(self._data_key(old)):
                await pipe.rename(self._data_key(old), self._data_key(new))
            if src["type"] == "dir":
                await pipe.rename(self._dir_key(old), self._dir_key(new))
            await pipe.srem(self._dir_key(old_parent), os.fsdecode(name_old))
            await pipe.sadd(self._dir_key(new_parent), os.fsdecode(name_new))
            await pipe.execute()

        ino = src["ino"]
        self.path_to_ino.pop(old, None)
        self._remember(new, ino)

    async def setattr(self, inode, attr, fields, fh, ctx):
        """
        In pyfuse3 this handles both truncate and utimens.
        """
        path = self.ino_to_path.get(int(inode))
        if path is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        meta = await self._get_meta(path)
        if meta is None:
            raise pyfuse3.FUSEError(errno.ENOENT)

        if fields.update_size:
            if fh is not None and fh in self.open_files:
                h = self.open_files[fh]
                async with h["lock"]:
                    await self._load_buf(fh)
                    h["buf"] = h["buf"][:attr.st_size].ljust(attr.st_size, b"\x00")
                    h["dirty"] = True
            else:
                blob = await self.redis.get(self._data_key(path)) or b""
                blob = blob[:attr.st_size].ljust(attr.st_size, b"\x00")
                meta["size"] = int(attr.st_size)
                meta["mtime_ns"] = meta["ctime_ns"] = now_ns()
                meta["version"] += 1
                async with self.redis.pipeline(transaction=True) as pipe:
                    await pipe.set(self._data_key(path), blob)
                    await pipe.hset(self._meta_key(path), mapping=self._mapping(meta))
                    await pipe.execute()

        if fields.update_atime:
            meta["atime_ns"] = int(attr.st_atime_ns)
        if fields.update_mtime:
            meta["mtime_ns"] = int(attr.st_mtime_ns)
        if fields.update_atime or fields.update_mtime:
            meta["ctime_ns"] = now_ns()
            meta["version"] += 1
            await self.redis.hset(self._meta_key(path), mapping=self._mapping(meta))

        return await self.getattr(inode, ctx)

    async def flush(self, fh):
        await self._flush_fh(fh, False)

    async def fsync(self, fh, datasync):
        await self._flush_fh(fh, bool(datasync))

    async def release(self, fh):
        try:
            await self._flush_fh(fh, False)
        finally:
            self.open_files.pop(fh, None)

    async def fsyncdir(self, fh, datasync):
        return

    async def releasedir(self, fh):
        self.dir_handles.pop(fh, None)

    async def forget(self, inode_list):
        for ino, nlookup in inode_list:
            ino = int(ino)
            self.lookup_count[ino] = self.lookup_count.get(ino, 0) - int(nlookup)
            if self.lookup_count[ino] <= 0 and ino != pyfuse3.ROOT_INODE:
                path = self.ino_to_path.pop(ino, None)
                if path:
                    self.path_to_ino.pop(path, None)
                self.lookup_count.pop(ino, None)


async def main_async(args):
    pyfuse3.asyncio.enable()
    ops = KVFS(
        redis_url=args.redis_url,
        entry_timeout=args.entry_timeout,
        attr_timeout=args.attr_timeout,
        waitaof=args.waitaof,
    )
    fuse_options = set(pyfuse3.default_options)
    fuse_options.add("fsname=kvfs")
    fuse_options.add("default_permissions")
    #fuse_options.add(f"entry_timeout={args.entry_timeout}")
    #fuse_options.add(f"attr_timeout={args.attr_timeout}")
    #fuse_options.add("negative_timeout=0")
    pyfuse3.init(ops, args.mountpoint, fuse_options)
    try:
        await ops.init()
        await pyfuse3.main()
    finally:
        pyfuse3.close()
        await ops.redis.aclose()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("mountpoint")
    ap.add_argument("--redis-url", default="redis://127.0.0.1:6379/0")
    ap.add_argument("--entry-timeout", type=float, default=0.25)
    ap.add_argument("--attr-timeout", type=float, default=0.25)
    ap.add_argument("--waitaof", action="store_true")
    args = ap.parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
