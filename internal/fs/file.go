package kvfs

import (
	"context"
	"sync"
	"syscall"

	"kvfs/internal/store"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type fileHandle struct {
	root  *Root
	ref   store.Ref
	mu    sync.Mutex
	data  []byte
	dirty bool
}

var (
	_ fs.FileReader      = (*fileHandle)(nil)
	_ fs.FileWriter      = (*fileHandle)(nil)
	_ fs.FileFlusher     = (*fileHandle)(nil)
	_ fs.FileFsyncer     = (*fileHandle)(nil)
	_ fs.FileReleaser    = (*fileHandle)(nil)
	_ fs.FileGetattrer   = (*fileHandle)(nil)
	_ fs.FileSetattrer   = (*fileHandle)(nil)
)

func (fh *fileHandle) Read(_ context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	fh.mu.Lock()
	defer fh.mu.Unlock()
	if off >= int64(len(fh.data)) {
		return fuse.ReadResultData(nil), 0
	}
	end := int(off) + len(dest)
	if end > len(fh.data) {
		end = len(fh.data)
	}
	return fuse.ReadResultData(fh.data[off:end]), 0
}

func (fh *fileHandle) Write(_ context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	fh.mu.Lock()
	defer fh.mu.Unlock()
	end := int(off) + len(data)
	if end > len(fh.data) {
		nd := make([]byte, end)
		copy(nd, fh.data)
		fh.data = nd
	}
	copy(fh.data[off:], data)
	fh.dirty = true
	return uint32(len(data)), 0
}

func (fh *fileHandle) flush(ctx context.Context, publishAlways bool) syscall.Errno {
	fh.mu.Lock()
	defer fh.mu.Unlock()
	if fh.dirty {
		if _, err := fh.root.store.WriteData(ctx, fh.ref.ParentID, fh.ref.Name, fh.data, true); err != nil {
			return errno(err)
		}
		fh.dirty = false
		fh.root.notifyLocal(fh.ref.ParentID, fh.ref.Name)
		return 0
	}
	if publishAlways {
		if err := fh.root.store.Publish(ctx, fh.ref.ParentID, fh.ref.Name); err != nil {
			return errno(err)
		}
		fh.root.notifyLocal(fh.ref.ParentID, fh.ref.Name)
	}
	return 0
}

func (fh *fileHandle) Flush(ctx context.Context) syscall.Errno {
	return fh.flush(ctx, false)
}

func (fh *fileHandle) Fsync(ctx context.Context, flags uint32) syscall.Errno {
	return fh.flush(ctx, false)
}

func (fh *fileHandle) Release(ctx context.Context) syscall.Errno {
	return fh.flush(ctx, true)
}

func (fh *fileHandle) Getattr(ctx context.Context, out *fuse.AttrOut) syscall.Errno {
	m, err := fh.root.store.GetMetaRef(ctx, fh.ref)
	if err != nil {
		return errno(err)
	}
	fh.mu.Lock()
	if fh.dirty {
		m.Size = uint64(len(fh.data))
	}
	fh.mu.Unlock()
	fillAttr(&out.Attr, m)
	return 0
}

func (fh *fileHandle) Setattr(ctx context.Context, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	if size, ok := in.GetSize(); ok {
		fh.mu.Lock()
		if uint64(len(fh.data)) > size {
			fh.data = fh.data[:size]
		} else if uint64(len(fh.data)) < size {
			nd := make([]byte, size)
			copy(nd, fh.data)
			fh.data = nd
		}
		fh.dirty = true
		fh.mu.Unlock()
	}
	return doSetattr(ctx, fh.root, fh.ref, in, out)
}
