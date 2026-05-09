# kvfs
Simple networked filesystem backed by `redis` or `valkey`

It's intended for small files (like config files, `.env`, ..etc.).
Because redis/valkey is in-memory.

This project uses `fuse3` and `asyncio`

## Usage

```
pip install kvfs-fuse
mkdir ~/kvfs_mnt
kvfs ~/kvfs_mnt
```

you can specify ip, port, and db number like this

```
kvfs --redis-url='redis://127.0.0.1:6379/0' ~/kvfs_mnt
```

see [PyPI: kvfs-fuse](https://pypi.org/project/kvfs-fuse/)

# Dependencies

you need `redis` and `pyfuse3`
which might need python development and fuse3 development

```
dnf install -y python3-devel fuse3-devel
```