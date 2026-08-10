package kvfs

import (
	"context"
	"syscall"

	"kvfs/internal/store"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

func doLookup(ctx context.Context, r *Root, parentIno *fs.Inode, parent store.Ref, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	m, err := r.store.GetMeta(ctx, parent.ID, name)
	if err != nil {
		return nil, errno(err)
	}
	ref := store.Ref{ParentID: parent.ID, Name: name, ID: m.ID}
	child := &Node{root: r, ref: ref}
	fillAttr(&out.Attr, m)
	ino := parentIno.NewInode(ctx, child, fs.StableAttr{Mode: stableMode(m), Ino: m.ID})
	r.track(m.ID, ino)
	return ino, 0
}

func doReaddir(ctx context.Context, r *Root, dir store.Ref) (fs.DirStream, syscall.Errno) {
	names, err := r.store.ListChildren(ctx, dir)
	if err != nil {
		return nil, errno(err)
	}
	entries := make([]fuse.DirEntry, 0, len(names))
	for _, name := range names {
		m, err := r.store.GetMeta(ctx, dir.ID, name)
		if err != nil {
			continue
		}
		entries = append(entries, fuse.DirEntry{
			Name: name,
			Ino:  m.ID,
			Mode: stableMode(m),
		})
	}
	return fs.NewListDirStream(entries), 0
}

func doCreate(ctx context.Context, r *Root, parentIno *fs.Inode, parent store.Ref, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	m, ref, err := r.store.CreateFile(ctx, parent, name, mode, r.uid, r.gid)
	if err != nil {
		return nil, nil, 0, errno(err)
	}
	child := &Node{root: r, ref: ref}
	fillAttr(&out.Attr, m)
	ino := parentIno.NewInode(ctx, child, fs.StableAttr{Mode: fuse.S_IFREG, Ino: m.ID})
	r.track(m.ID, ino)
	fh := &fileHandle{root: r, ref: ref, data: nil, dirty: false}
	return ino, fh, fuse.FOPEN_DIRECT_IO, 0
}

func doMkdir(ctx context.Context, r *Root, parentIno *fs.Inode, parent store.Ref, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	m, ref, err := r.store.Mkdir(ctx, parent, name, mode, r.uid, r.gid)
	if err != nil {
		return nil, errno(err)
	}
	child := &Node{root: r, ref: ref}
	fillAttr(&out.Attr, m)
	ino := parentIno.NewInode(ctx, child, fs.StableAttr{Mode: fuse.S_IFDIR, Ino: m.ID})
	r.track(m.ID, ino)
	return ino, 0
}

func doSymlink(ctx context.Context, r *Root, parentIno *fs.Inode, parent store.Ref, name, target string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	m, ref, err := r.store.Symlink(ctx, parent, name, target, r.uid, r.gid)
	if err != nil {
		return nil, errno(err)
	}
	child := &Node{root: r, ref: ref}
	fillAttr(&out.Attr, m)
	ino := parentIno.NewInode(ctx, child, fs.StableAttr{Mode: fuse.S_IFLNK, Ino: m.ID})
	r.track(m.ID, ino)
	return ino, 0
}

func doUnlink(ctx context.Context, r *Root, parent store.Ref, name string) syscall.Errno {
	if err := r.store.Unlink(ctx, parent, name); err != nil {
		return errno(err)
	}
	return 0
}

func doRmdir(ctx context.Context, r *Root, parent store.Ref, name string) syscall.Errno {
	if err := r.store.Rmdir(ctx, parent, name); err != nil {
		return errno(err)
	}
	return 0
}

func doRename(ctx context.Context, r *Root, oldParent store.Ref, oldName string, newParentEmbed fs.InodeEmbedder, newName string) syscall.Errno {
	newParentRef, ok := refOf(newParentEmbed)
	if !ok {
		return syscall.EXDEV
	}
	oldMeta, err := r.store.GetMeta(ctx, oldParent.ID, oldName)
	if err != nil {
		return errno(err)
	}
	if err := r.store.Rename(ctx, oldParent, oldName, newParentRef, newName); err != nil {
		return errno(err)
	}
	if ino := r.inodeByID(oldMeta.ID); ino != nil {
		if n, ok := ino.Operations().(*Node); ok {
			n.ref = store.Ref{ParentID: newParentRef.ID, Name: newName, ID: oldMeta.ID}
		}
	}
	return 0
}

func refOf(embed fs.InodeEmbedder) (store.Ref, bool) {
	switch n := embed.(type) {
	case *Root:
		return n.ref(), true
	case *Node:
		return n.ref, true
	default:
		return store.Ref{}, false
	}
}

func doSetattr(ctx context.Context, r *Root, ref store.Ref, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	if size, ok := in.GetSize(); ok {
		if _, err := r.store.Truncate(ctx, ref.ParentID, ref.Name, size); err != nil {
			return errno(err)
		}
	}
	if mode, ok := in.GetMode(); ok {
		if _, err := r.store.UpdateMeta(ctx, ref.ParentID, ref.Name, func(m *store.Meta) {
			m.Mode = mode & 0777
		}); err != nil {
			return errno(err)
		}
	}
	if uid, ok := in.GetUID(); ok {
		if _, err := r.store.UpdateMeta(ctx, ref.ParentID, ref.Name, func(m *store.Meta) {
			m.UID = uid
		}); err != nil {
			return errno(err)
		}
	}
	if gid, ok := in.GetGID(); ok {
		if _, err := r.store.UpdateMeta(ctx, ref.ParentID, ref.Name, func(m *store.Meta) {
			m.GID = gid
		}); err != nil {
			return errno(err)
		}
	}
	if atime, ok := in.GetATime(); ok {
		if _, err := r.store.UpdateMeta(ctx, ref.ParentID, ref.Name, func(m *store.Meta) {
			m.Atime = atime.Unix()
		}); err != nil {
			return errno(err)
		}
	}
	if mtime, ok := in.GetMTime(); ok {
		if _, err := r.store.UpdateMeta(ctx, ref.ParentID, ref.Name, func(m *store.Meta) {
			m.Mtime = mtime.Unix()
		}); err != nil {
			return errno(err)
		}
	}
	m, err := r.store.GetMetaRef(ctx, ref)
	if err != nil {
		return errno(err)
	}
	fillAttr(&out.Attr, m)
	return 0
}
