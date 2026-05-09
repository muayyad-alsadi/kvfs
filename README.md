# kvfs
Simple networked filesystem backed by `redis` or `valkey`

It's intended for small files (like config files, `.env`, ..etc.).
Because redis/valkey is in-memory.

This project uses `fuse3` and `asyncio`

see [PyPI: kvfs-fuse](https://pypi.org/project/kvfs-fuse/)

## Usage

```
pip install kvfs-fuse
mkdir ~/kvfs_mnt
kvfs ~/kvfs_mnt
```

you can specify ip, port, db number, and prefix like this

```
kvfs --redis-url='redis://1.2.3.4:6379/0' --prefix='kvfs' ~/kvfs_mnt
```

you can mount it on multiple machines, so that when you edit the config files or `.env`
they will be updated on all of your machines


## Dependencies

you need `redis` and `pyfuse3`
which might need python development and fuse3 development

```
dnf install -y python3-devel fuse3-devel
```

## How does it work

- `kvfs:meta:/path/to/file` stores the meta data of your file
- `kvfs:data:/path/to/file` stores the actual content
- `kvfs:dir:/path/to/dir/` stores the dir entry

