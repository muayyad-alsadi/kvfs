package kvfs

import (
	"context"
	"sync"
	"syscall"

	"kvfs/internal/store"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// fileHandle holds no content. Every read and write goes to the content key, so
// two handles of one file, on one mount or on two, see the same bytes. Read-only
// handles let the kernel cache those bytes (FOPEN_KEEP_CACHE, see openFlags); the
// invalidate loop drops them when a write on another mount is published. A write
// is one SETRANGE; the modification time is stamped once, when the handle
// flushes or closes.
type fileHandle struct {
	root *Root
	ref  store.Ref

	mu    sync.Mutex
	wrote bool
}

var (
	_ fs.FileReader    = (*fileHandle)(nil)
	_ fs.FileWriter    = (*fileHandle)(nil)
	_ fs.FileFlusher   = (*fileHandle)(nil)
	_ fs.FileFsyncer   = (*fileHandle)(nil)
	_ fs.FileReleaser  = (*fileHandle)(nil)
	_ fs.FileGetattrer = (*fileHandle)(nil)
	_ fs.FileSetattrer = (*fileHandle)(nil)
)

func (fh *fileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	data, err := fh.root.store.ReadAt(ctx, fh.ref.ParentID, fh.ref.Name, off, len(dest))
	if err != nil {
		return nil, errno(err)
	}
	return fuse.ReadResultData(data), 0
}

func (fh *fileHandle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	if err := fh.root.store.WriteAt(ctx, fh.ref.ParentID, fh.ref.Name, off, data); err != nil {
		return 0, errno(err)
	}
	fh.mu.Lock()
	fh.wrote = true
	fh.mu.Unlock()
	return uint32(len(data)), 0
}

// flush stamps the file modified if this handle wrote to it.
func (fh *fileHandle) flush(ctx context.Context) syscall.Errno {
	fh.mu.Lock()
	wrote := fh.wrote
	fh.wrote = false
	fh.mu.Unlock()
	if !wrote {
		return 0
	}
	if err := fh.root.store.MarkModified(ctx, fh.ref.ParentID, fh.ref.Name); err != nil {
		return errno(err)
	}
	return 0
}

func (fh *fileHandle) Flush(ctx context.Context) syscall.Errno { return fh.flush(ctx) }

// Fsync has nothing to push: every write already reached the server.
func (fh *fileHandle) Fsync(context.Context, uint32) syscall.Errno { return 0 }

func (fh *fileHandle) Release(ctx context.Context) syscall.Errno { return fh.flush(ctx) }

func (fh *fileHandle) Getattr(ctx context.Context, out *fuse.AttrOut) syscall.Errno {
	m, err := fh.root.store.StatRef(ctx, fh.ref)
	if err != nil {
		return errno(err)
	}
	fillAttr(&out.Attr, m)
	return 0
}

func (fh *fileHandle) Setattr(ctx context.Context, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	return doSetattr(ctx, fh.root, fh.ref, in, out)
}
