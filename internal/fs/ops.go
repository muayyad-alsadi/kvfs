package kvfs

import (
	"context"
	"errors"
	"syscall"

	"kvfs/internal/store"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

func doLookup(ctx context.Context, r *Root, parentIno *fs.Inode, parent store.Ref, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	m, err := r.store.Stat(ctx, parent.ID, name)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			// A link operation can stop between the parent's field and the
			// entry. Repairing here is best effort: the name is a miss either
			// way, but the next readdir stops listing a ghost.
			r.repairMiss(ctx, parent, name)
		}
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
	children, err := r.store.ListChildren(ctx, dir)
	if err != nil {
		return nil, errno(err)
	}
	r.noteListing(dir, children)
	entries := make([]fuse.DirEntry, 0, len(children))
	for _, c := range children {
		// The hash carries the type and inode: no per-child metadata read.
		// An unknown code still lists the name, with DT_UNKNOWN mode.
		entries = append(entries, fuse.DirEntry{
			Name: c.Name,
			Ino:  c.ID,
			Mode: store.FuseModeType(c.Type),
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
	fh := &fileHandle{root: r, ref: ref}
	return ino, fh, fuse.FOPEN_DIRECT_IO, 0 // created for writing: see openFlags
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
		r.noteBusy(ctx, err, parent, name)
		return errno(err)
	}
	return 0
}

func doRmdir(ctx context.Context, r *Root, parent store.Ref, name string) syscall.Errno {
	if err := r.store.Rmdir(ctx, parent, name); err != nil {
		r.noteBusy(ctx, err, parent, name)
		return errno(err)
	}
	return 0
}

// Linux rename2 flags. RENAME_EXCHANGE and RENAME_WHITEOUT are not supported.
const renameNoReplace = 1

// renameStore picks the store call for the rename flags, nil when unsupported.
func renameStore(r *Root, flags uint32) func(context.Context, store.Ref, string, store.Ref, string) (uint64, error) {
	switch flags {
	case 0:
		return r.store.Rename
	case renameNoReplace:
		return r.store.RenameNoReplace
	default:
		return nil
	}
}

func doRename(ctx context.Context, r *Root, oldParent store.Ref, oldName string, newParentEmbed fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	rename := renameStore(r, flags)
	if rename == nil {
		return syscall.EINVAL
	}
	newParentRef, ok := refOf(newParentEmbed)
	if !ok {
		return syscall.EXDEV
	}
	id, err := rename(ctx, oldParent, oldName, newParentRef, newName)
	if err != nil {
		r.noteBusy(ctx, err, oldParent, oldName)
		return errno(err)
	}
	if id == 0 {
		// Same name, same parent: nothing moved, so nothing to rewrite.
		return 0
	}
	if ino := r.inodeByID(id); ino != nil {
		if n, ok := ino.Operations().(*Node); ok {
			n.ref = store.Ref{ParentID: newParentRef.ID, Name: newName, ID: id}
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
	m, err := applySetattr(ctx, r, ref, in)
	if err != nil {
		return errno(err)
	}
	fillAttr(&out.Attr, m)
	return 0
}

// applySetattr resizes first, then writes every metadata field the request
// sets in one update, and returns the attributes the last write produced.
func applySetattr(ctx context.Context, r *Root, ref store.Ref, in *fuse.SetAttrIn) (*store.Meta, error) {
	var resized *store.Meta
	if size, ok := in.GetSize(); ok {
		m, err := r.store.Truncate(ctx, ref.ParentID, ref.Name, size)
		if err != nil {
			return nil, err
		}
		resized = m
	}
	if mut := metaChanges(in); mut != nil {
		return r.store.UpdateMeta(ctx, ref.ParentID, ref.Name, mut)
	}
	if resized != nil {
		return resized, nil
	}
	return r.store.StatRef(ctx, ref)
}

// metaChanges folds the metadata fields a setattr carries into one mutation,
// nil when it carries none.
func metaChanges(in *fuse.SetAttrIn) func(*store.Meta) {
	var muts []func(*store.Meta)
	if mode, ok := in.GetMode(); ok {
		muts = append(muts, func(m *store.Meta) { m.Mode = mode & 0777 })
	}
	if uid, ok := in.GetUID(); ok {
		muts = append(muts, func(m *store.Meta) { m.UID = uid })
	}
	if gid, ok := in.GetGID(); ok {
		muts = append(muts, func(m *store.Meta) { m.GID = gid })
	}
	if atime, ok := in.GetATime(); ok {
		muts = append(muts, func(m *store.Meta) { m.Atime = atime.Unix() })
	}
	if mtime, ok := in.GetMTime(); ok {
		muts = append(muts, func(m *store.Meta) { m.Mtime = mtime.Unix() })
	}
	if len(muts) == 0 {
		return nil
	}
	return func(m *store.Meta) {
		for _, mut := range muts {
			mut(m)
		}
	}
}
