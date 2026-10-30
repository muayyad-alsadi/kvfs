package kvfs

import (
	"context"
	"errors"
	"log"
	"sync"
	"syscall"
	"time"

	"kvfs/internal/store"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/redis/rueidis"
)

type invalidateReq struct {
	parentID uint64
	name     string
}

// Root is the FUSE filesystem root and owns Redis + inode registry state.
type Root struct {
	fs.Inode

	store     *store.Store
	subClient rueidis.Client // dedicated client for SUBSCRIBE only
	uid       uint32
	gid       uint32
	cancel    context.CancelFunc

	mu   sync.RWMutex
	byID map[uint64]*fs.Inode

	invCh    chan invalidateReq
	repairCh chan repairReq
	health   healthCounters
	grace    time.Duration // how long a pending field must stay unchanged before repair
	wg       sync.WaitGroup
}

func NewRoot(st *store.Store, subClient rueidis.Client, uid, gid uint32) *Root {
	return &Root{
		store:     st,
		subClient: subClient,
		uid:       uid,
		gid:       gid,
		byID:      make(map[uint64]*fs.Inode),
		invCh:     make(chan invalidateReq, 256),
		repairCh:  make(chan repairReq, 256),
		grace:     graceFactor * st.OpTimeout(),
	}
}

// Close stops the background loops and waits for them to return.
func (r *Root) Close() {
	if r.cancel != nil {
		r.cancel()
	}
	r.wg.Wait()
}

func (r *Root) OnAdd(ctx context.Context) {
	r.track(store.RootID, r.EmbeddedInode())
	subCtx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	r.goLoop(func() { r.invalidateLoop(subCtx) })
	r.goLoop(func() { r.subscribeLoop(subCtx) })
	r.goLoop(func() { r.repairLoop(subCtx) })
}

// goLoop runs one background loop; Close waits for it.
func (r *Root) goLoop(fn func()) {
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		fn()
	}()
}

func (r *Root) track(id uint64, ino *fs.Inode) {
	r.mu.Lock()
	r.byID[id] = ino
	r.mu.Unlock()
}

func (r *Root) untrack(id uint64) {
	r.mu.Lock()
	delete(r.byID, id)
	r.mu.Unlock()
}

func (r *Root) inodeByID(id uint64) *fs.Inode {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.byID[id]
}

// enqueueInvalidate hands one published address to the invalidate loop. It
// waits when the loop is busy rather than dropping: with the kernel cache on,
// a dropped message would leave the cached bytes stale. Only the subscriber
// calls it, never a FUSE request, so the wait cannot deadlock with the kernel.
func (r *Root) enqueueInvalidate(ctx context.Context, parentID uint64, name string) {
	select {
	case r.invCh <- invalidateReq{parentID: parentID, name: name}:
	case <-ctx.Done():
	}
}

func (r *Root) invalidateLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case req := <-r.invCh:
			r.invalidate(req)
		}
	}
}

// invalidate drops the kernel's dentry for the name and the cached attributes
// and bytes of the child. It runs on the invalidate loop, never on a request.
func (r *Root) invalidate(req invalidateReq) {
	parent := r.inodeByID(req.parentID)
	if parent == nil {
		return // the kernel cannot hold anything under an unknown parent
	}
	parent.NotifyEntry(req.name)
	if child := parent.GetChild(req.name); child != nil {
		child.NotifyContent(0, 0) // length 0: attributes and every cached page
	}
}

func (r *Root) subscribeLoop(ctx context.Context) {
	client := r.subClient
	if client == nil {
		client = r.store.Client()
	}
	ch := r.store.Channel()
	for {
		if ctx.Err() != nil {
			return
		}
		err := client.Receive(ctx, client.B().Subscribe().Channel(ch).Build(), func(msg rueidis.PubSubMessage) {
			parentID, name, err := store.ParseInvalidate(msg.Message)
			if err != nil {
				return
			}
			r.enqueueInvalidate(ctx, parentID, name)
		})
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			log.Printf("kvfs: pubsub: %v; retrying", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
		}
	}
}

func errno(err error) syscall.Errno {
	switch {
	case err == nil:
		return 0
	case errors.Is(err, store.ErrNotFound):
		return syscall.ENOENT
	case errors.Is(err, store.ErrExist):
		return syscall.EEXIST
	case errors.Is(err, store.ErrNotEmpty):
		return syscall.ENOTEMPTY
	case errors.Is(err, store.ErrNotDir):
		return syscall.ENOTDIR
	case errors.Is(err, store.ErrIsDir):
		return syscall.EISDIR
	case errors.Is(err, store.ErrBusy):
		return syscall.EBUSY
	case errors.Is(err, store.ErrTimeout):
		return syscall.ETIMEDOUT
	case errors.Is(err, context.DeadlineExceeded):
		return syscall.ETIMEDOUT
	default:
		log.Printf("kvfs: %v", err)
		return syscall.EIO
	}
}

func fillAttr(out *fuse.Attr, m *store.Meta) {
	out.Ino = m.ID
	out.Size = m.Size
	out.Mode = store.FuseMode(m)
	out.Uid = m.UID
	out.Gid = m.GID
	out.Nlink = m.Nlink
	if out.Nlink == 0 {
		out.Nlink = 1
	}
	out.Atime = uint64(m.Atime)
	out.Mtime = uint64(m.Mtime)
	out.Ctime = uint64(m.Ctime)
}

func stableMode(m *store.Meta) uint32 {
	return store.FuseModeType(m.Type)
}

func (r *Root) ref() store.Ref { return store.RootRef() }

var (
	_ fs.NodeOnAdder   = (*Root)(nil)
	_ fs.NodeGetattrer = (*Root)(nil)
	_ fs.NodeLookuper  = (*Root)(nil)
	_ fs.NodeReaddirer = (*Root)(nil)
	_ fs.NodeCreater   = (*Root)(nil)
	_ fs.NodeMkdirer   = (*Root)(nil)
	_ fs.NodeUnlinker  = (*Root)(nil)
	_ fs.NodeRmdirer   = (*Root)(nil)
	_ fs.NodeRenamer   = (*Root)(nil)
	_ fs.NodeSymlinker = (*Root)(nil)
	_ fs.NodeSetattrer = (*Root)(nil)
)

func (r *Root) Getattr(ctx context.Context, _ fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	m, err := r.store.StatRef(ctx, r.ref())
	if err != nil {
		return errno(err)
	}
	fillAttr(&out.Attr, m)
	return 0
}

func (r *Root) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return doLookup(ctx, r, r.EmbeddedInode(), r.ref(), name, out)
}

func (r *Root) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	return doReaddir(ctx, r, r.ref())
}

func (r *Root) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	return doCreate(ctx, r, r.EmbeddedInode(), r.ref(), name, mode, out)
}

func (r *Root) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return doMkdir(ctx, r, r.EmbeddedInode(), r.ref(), name, mode, out)
}

func (r *Root) Unlink(ctx context.Context, name string) syscall.Errno {
	return doUnlink(ctx, r, r.ref(), name)
}

func (r *Root) Rmdir(ctx context.Context, name string) syscall.Errno {
	return doRmdir(ctx, r, r.ref(), name)
}

func (r *Root) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	return doRename(ctx, r, r.ref(), name, newParent, newName, flags)
}

func (r *Root) Symlink(ctx context.Context, target, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return doSymlink(ctx, r, r.EmbeddedInode(), r.ref(), name, target, out)
}

func (r *Root) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	return doSetattr(ctx, r, r.ref(), in, out)
}
