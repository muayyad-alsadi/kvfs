package kvfs

import (
	"context"
	"syscall"

	"kvfs/internal/store"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// Node is any non-root filesystem node (file, dir, or symlink).
type Node struct {
	fs.Inode
	root *Root
	ref  store.Ref
}

var (
	_ fs.NodeGetattrer   = (*Node)(nil)
	_ fs.NodeLookuper    = (*Node)(nil)
	_ fs.NodeReaddirer   = (*Node)(nil)
	_ fs.NodeCreater     = (*Node)(nil)
	_ fs.NodeMkdirer     = (*Node)(nil)
	_ fs.NodeUnlinker    = (*Node)(nil)
	_ fs.NodeRmdirer     = (*Node)(nil)
	_ fs.NodeRenamer     = (*Node)(nil)
	_ fs.NodeSymlinker   = (*Node)(nil)
	_ fs.NodeReadlinker  = (*Node)(nil)
	_ fs.NodeOpener      = (*Node)(nil)
	_ fs.NodeSetattrer   = (*Node)(nil)
	_ fs.NodeOnForgetter = (*Node)(nil)
)

func (n *Node) OnForget() {
	n.root.untrack(n.ref.ID)
}

func (n *Node) Getattr(ctx context.Context, _ fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	m, err := n.root.store.StatRef(ctx, n.ref)
	if err != nil {
		return errno(err)
	}
	fillAttr(&out.Attr, m)
	return 0
}

func (n *Node) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return doLookup(ctx, n.root, n.EmbeddedInode(), n.ref, name, out)
}

func (n *Node) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	return doReaddir(ctx, n.root, n.ref)
}

func (n *Node) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	return doCreate(ctx, n.root, n.EmbeddedInode(), n.ref, name, mode, out)
}

func (n *Node) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return doMkdir(ctx, n.root, n.EmbeddedInode(), n.ref, name, mode, out)
}

func (n *Node) Unlink(ctx context.Context, name string) syscall.Errno {
	return doUnlink(ctx, n.root, n.ref, name)
}

func (n *Node) Rmdir(ctx context.Context, name string) syscall.Errno {
	return doRmdir(ctx, n.root, n.ref, name)
}

func (n *Node) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	return doRename(ctx, n.root, n.ref, name, newParent, newName, flags)
}

func (n *Node) Symlink(ctx context.Context, target, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return doSymlink(ctx, n.root, n.EmbeddedInode(), n.ref, name, target, out)
}

func (n *Node) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	m, err := n.root.store.GetMetaRef(ctx, n.ref)
	if err != nil {
		return nil, errno(err)
	}
	if m.Type != store.TypeLink {
		return nil, syscall.EINVAL
	}
	data, err := n.root.store.GetData(ctx, n.ref.ParentID, n.ref.Name)
	if err != nil {
		return nil, errno(err)
	}
	return data, 0
}

func (n *Node) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	m, err := n.root.store.GetMetaRef(ctx, n.ref)
	if err != nil {
		return nil, 0, errno(err)
	}
	if m.Type == store.TypeDir {
		return nil, 0, syscall.EISDIR
	}
	if m.Type == store.TypeLink {
		return nil, 0, syscall.ELOOP
	}
	if flags&syscall.O_TRUNC != 0 {
		if _, err := n.root.store.Truncate(ctx, n.ref.ParentID, n.ref.Name, 0); err != nil {
			return nil, 0, errno(err)
		}
	}
	return &fileHandle{root: n.root, ref: n.ref}, openFlags(flags), 0
}

// openFlags keeps the kernel page cache for read-only opens and bypasses it for
// writable ones. A cached write is split into more requests than the caller made
// (about 1.5 per 10 KiB write), and each request is one Redis script call.
func openFlags(flags uint32) uint32 {
	if flags&syscall.O_ACCMODE == syscall.O_RDONLY {
		return fuse.FOPEN_KEEP_CACHE
	}
	return fuse.FOPEN_DIRECT_IO
}

func (n *Node) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	return doSetattr(ctx, n.root, n.ref, in, out)
}
