package store_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"kvfs/internal/store"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/rueidis"
)

// testClient starts an in-memory Redis and a real client against it.
func testClient(t testing.TB) (*miniredis.Miniredis, rueidis.Client) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mr.Close)
	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress:       []string{mr.Addr()},
		DisableCache:      true,
		ForceSingleClient: true,
		ConnWriteTimeout:  time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return mr, client
}

// bootstrapped builds a store for a test and creates its root.
func bootstrapped(t testing.TB, client rueidis.Client) *store.Store {
	t.Helper()
	return bootstrappedAt(t, client, store.LevelNamespace)
}

// bootstrappedAt builds a store at a given sharding level and creates its root.
func bootstrappedAt(t testing.TB, client rueidis.Client, level store.Level) *store.Store {
	t.Helper()
	st, err := store.New(client, "test", level)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.BootstrapRoot(context.Background(), 1000, 1000); err != nil {
		t.Fatal(err)
	}
	return st
}

func newTestStore(t *testing.T) (*store.Store, *miniredis.Miniredis) {
	t.Helper()
	return newTestStoreAt(t, store.LevelNamespace)
}

// newTestStoreAt builds a store at a level, with its root already created.
func newTestStoreAt(t testing.TB, level store.Level) (*store.Store, *miniredis.Miniredis) {
	t.Helper()
	mr, client := testClient(t)
	return bootstrappedAt(t, client, level), mr
}

func TestBootstrapAndCreate(t *testing.T) {
	st, _ := newTestStore(t)
	ctx := context.Background()

	root, err := st.GetMetaRef(ctx, store.RootRef())
	if err != nil {
		t.Fatal(err)
	}
	if root.ID != 1 || root.Type != store.TypeDir {
		t.Fatalf("root meta: %+v", root)
	}

	m, ref, err := st.CreateFile(ctx, store.RootRef(), ".env", 0644, 1000, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if m.ID != 2 || ref.ParentID != 1 || ref.Name != ".env" {
		t.Fatalf("file ref/meta: meta=%+v ref=%+v", m, ref)
	}

	if _, err := st.WriteData(ctx, ref.ParentID, ref.Name, []byte("FOO=bar"), true); err != nil {
		t.Fatal(err)
	}
	data, err := st.GetData(ctx, ref.ParentID, ref.Name)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "FOO=bar" {
		t.Fatalf("data=%q", data)
	}

	children, err := st.ListChildren(ctx, store.RootRef())
	if err != nil {
		t.Fatal(err)
	}
	if len(children) != 1 {
		t.Fatalf("children=%v", children)
	}
	got := children[0]
	if got.Name != ".env" || got.Type != store.TypeFile || got.ID != m.ID {
		t.Fatalf("child=%+v, want name .env type file id %d", got, m.ID)
	}
}

func TestBootstrapIsIdempotent(t *testing.T) {
	st, _ := newTestStore(t)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if err := st.BootstrapRoot(ctx, 1000, 1000); err != nil {
			t.Fatal(err)
		}
	}
	root, err := st.GetMetaRef(ctx, store.RootRef())
	if err != nil {
		t.Fatal(err)
	}
	if root.ID != store.RootID {
		t.Fatalf("root id=%d, want %d", root.ID, store.RootID)
	}
}

func TestBootstrapRefusesLevelMismatch(t *testing.T) {
	st, mr := newTestStore(t)
	ctx := context.Background()
	// A namespace initialized at the file level: only that counter exists.
	fileCounter := store.Key("test", store.LevelFile, store.NamespaceID, store.RootName, store.SuffixCounter)
	if err := mr.Set(fileCounter, "1"); err != nil {
		t.Fatal(err)
	}
	// Remove the namespace-level counter the helper created.
	nsCounter := store.Key("test", store.LevelNamespace, store.NamespaceID, store.RootName, store.SuffixCounter)
	mr.Del(nsCounter)

	err := st.BootstrapRoot(ctx, 1000, 1000)
	if err == nil || !strings.Contains(err.Error(), "initialized at") {
		t.Fatalf("want level mismatch, got %v", err)
	}
}

func TestBootstrapRefusesSplitNamespace(t *testing.T) {
	st, mr := newTestStore(t)
	ctx := context.Background()
	fileCounter := store.Key("test", store.LevelFile, store.NamespaceID, store.RootName, store.SuffixCounter)
	if err := mr.Set(fileCounter, "1"); err != nil {
		t.Fatal(err)
	}
	err := st.BootstrapRoot(ctx, 1000, 1000)
	if err == nil || !strings.Contains(err.Error(), "more than one") {
		t.Fatalf("want split-namespace refusal, got %v", err)
	}
}

func TestBootstrapRefusesCounterWithoutRoot(t *testing.T) {
	st, mr := newTestStore(t)
	ctx := context.Background()
	rootMeta := store.Key("test", store.LevelNamespace, store.NamespaceID, store.RootName, store.SuffixMeta)
	mr.Del(rootMeta)
	err := st.BootstrapRoot(ctx, 1000, 1000)
	if err == nil || !strings.Contains(err.Error(), "no root") {
		t.Fatalf("want a counter-without-root refusal, got %v", err)
	}
}

func TestMkdirAndSymlink(t *testing.T) {
	st, _ := newTestStore(t)
	ctx := context.Background()

	if _, _, err := st.Mkdir(ctx, store.RootRef(), "conf", 0755, 1000, 1000); err != nil {
		t.Fatal(err)
	}
	_, linkRef, err := st.Symlink(ctx, store.RootRef(), "link", "conf/.env", 1000, 1000)
	if err != nil {
		t.Fatal(err)
	}
	target, err := st.GetData(ctx, linkRef.ParentID, linkRef.Name)
	if err != nil {
		t.Fatal(err)
	}
	if string(target) != "conf/.env" {
		t.Fatalf("target=%q", target)
	}
	lm, err := st.GetMetaRef(ctx, linkRef)
	if err != nil || lm.Type != store.TypeLink {
		t.Fatalf("link meta=%+v err=%v", lm, err)
	}
	if err := st.Unlink(ctx, store.RootRef(), "link"); err != nil {
		t.Fatal(err)
	}
	if err := st.Rmdir(ctx, store.RootRef(), "conf"); err != nil {
		t.Fatal(err)
	}
}

func TestRenameKeepsInodeAndMovesContent(t *testing.T) {
	st, _ := newTestStore(t)
	ctx := context.Background()

	_, confRef, err := st.Mkdir(ctx, store.RootRef(), "conf", 0755, 1000, 1000)
	if err != nil {
		t.Fatal(err)
	}
	_, fileRef, err := st.CreateFile(ctx, confRef, "app.toml", 0644, 1000, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.WriteData(ctx, confRef.ID, "app.toml", []byte("x=1"), true); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Rename(ctx, confRef, "app.toml", store.RootRef(), "app.toml"); err != nil {
		t.Fatal(err)
	}

	moved, err := st.GetMeta(ctx, store.RootID, "app.toml")
	if err != nil {
		t.Fatal(err)
	}
	if moved.ID != fileRef.ID {
		t.Fatalf("inode changed on rename: %d -> %d", fileRef.ID, moved.ID)
	}
	if data, err := st.GetData(ctx, store.RootID, "app.toml"); err != nil || string(data) != "x=1" {
		t.Fatalf("content did not move: %q err=%v", data, err)
	}
	if _, err := st.GetMeta(ctx, confRef.ID, "app.toml"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("old key still present: %v", err)
	}
	if children, err := st.ListChildren(ctx, store.RootRef()); err != nil {
		t.Fatal(err)
	} else if len(children) != 2 {
		t.Fatalf("root listing after rename=%v", children)
	}
	if children, err := st.ListChildren(ctx, confRef); err != nil {
		t.Fatal(err)
	} else if len(children) != 0 {
		t.Fatalf("old parent listing still has entries: %v", children)
	}
}

func TestRenameDirectoryMovesItsListing(t *testing.T) {
	st, _ := newTestStore(t)
	ctx := context.Background()

	_, sub, err := st.Mkdir(ctx, store.RootRef(), "sub1", 0755, 1000, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.CreateFile(ctx, sub, "README.md", 0644, 1000, 1000); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Rename(ctx, store.RootRef(), "sub1", store.RootRef(), "mydir"); err != nil {
		t.Fatal(err)
	}
	moved := store.Ref{ParentID: store.RootID, Name: "mydir", ID: sub.ID}
	children, err := st.ListChildren(ctx, moved)
	if err != nil {
		t.Fatal(err)
	}
	if len(children) != 1 || children[0].Name != "README.md" {
		t.Fatalf("moved listing=%v", children)
	}
	// The children are addressed by the directory's inode, which did not
	// change: their keys never moved.
	if _, err := st.GetMeta(ctx, sub.ID, "README.md"); err != nil {
		t.Fatal(err)
	}
}

func TestRenameConflictAndMissingSource(t *testing.T) {
	st, _ := newTestStore(t)
	ctx := context.Background()

	if _, _, err := st.CreateFile(ctx, store.RootRef(), "a", 0644, 1, 1); err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.CreateFile(ctx, store.RootRef(), "b", 0644, 1, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := st.RenameNoReplace(ctx, store.RootRef(), "a", store.RootRef(), "b"); !errors.Is(err, store.ErrExist) {
		t.Fatalf("want ErrExist, got %v", err)
	}
	if _, err := st.Rename(ctx, store.RootRef(), "nope", store.RootRef(), "c"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
	// A no-op rename must not fail or publish.
	if _, err := st.Rename(ctx, store.RootRef(), "a", store.RootRef(), "a"); err != nil {
		t.Fatalf("no-op rename: %v", err)
	}
}

func TestRmdirNotEmpty(t *testing.T) {
	st, _ := newTestStore(t)
	ctx := context.Background()
	_, dir, err := st.Mkdir(ctx, store.RootRef(), "d", 0755, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.CreateFile(ctx, dir, "f", 0644, 1, 1); err != nil {
		t.Fatal(err)
	}
	if err := st.Rmdir(ctx, store.RootRef(), "d"); !errors.Is(err, store.ErrNotEmpty) {
		t.Fatalf("want ErrNotEmpty, got %v", err)
	}
	if err := st.Unlink(ctx, store.RootRef(), "d"); !errors.Is(err, store.ErrIsDir) {
		t.Fatalf("unlink of a dir: want ErrIsDir, got %v", err)
	}
	if _, _, err := st.CreateFile(ctx, store.RootRef(), "top", 0644, 1, 1); err != nil {
		t.Fatal(err)
	}
	if err := st.Rmdir(ctx, store.RootRef(), "top"); !errors.Is(err, store.ErrNotDir) {
		t.Fatalf("rmdir of a file: want ErrNotDir, got %v", err)
	}
}

func TestPublish(t *testing.T) {
	st, _ := newTestStore(t)
	ctx := context.Background()
	if err := st.Publish(ctx, 1, ".env"); err != nil {
		t.Fatal(err)
	}
}

func TestCreateConflict(t *testing.T) {
	st, _ := newTestStore(t)
	ctx := context.Background()
	if _, _, err := st.CreateFile(ctx, store.RootRef(), "x", 0644, 1, 1); err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.CreateFile(ctx, store.RootRef(), "x", 0644, 1, 1); !errors.Is(err, store.ErrExist) {
		t.Fatalf("want ErrExist, got %v", err)
	}
	// Mkdir racing the same name fails the same way.
	if _, _, err := st.Mkdir(ctx, store.RootRef(), "x", 0755, 1, 1); !errors.Is(err, store.ErrExist) {
		t.Fatalf("mkdir over a file: want ErrExist, got %v", err)
	}
}

func TestListChildrenReadsTypeFromHash(t *testing.T) {
	st, _ := newTestStore(t)
	ctx := context.Background()
	if _, _, err := st.CreateFile(ctx, store.RootRef(), "f", 0644, 1, 1); err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.Mkdir(ctx, store.RootRef(), "d", 0755, 1, 1); err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.Symlink(ctx, store.RootRef(), "l", "f", 1, 1); err != nil {
		t.Fatal(err)
	}
	children, err := st.ListChildren(ctx, store.RootRef())
	if err != nil {
		t.Fatal(err)
	}
	if len(children) != 3 {
		t.Fatalf("children=%v", children)
	}
	// Sorted by name; one HGETALL, no metadata reads.
	want := []struct {
		name string
		typ  string
		id   uint64
	}{{"d", store.TypeDir, 3}, {"f", store.TypeFile, 2}, {"l", store.TypeLink, 4}}
	for i, w := range want {
		if children[i].Name != w.name || children[i].Type != w.typ || children[i].ID != w.id {
			t.Fatalf("child[%d]=%+v, want %+v", i, children[i], w)
		}
	}
}

func TestParseInvalidate(t *testing.T) {
	pid, name, err := store.ParseInvalidate(store.InvalidatePayload(1, ".env"))
	if err != nil || pid != 1 || name != ".env" {
		t.Fatalf("got %d %q %v", pid, name, err)
	}
	pid, name, err = store.ParseInvalidate(store.InvalidatePayload(3, "a/b c"))
	if err != nil || pid != 3 || name != "a/b c" {
		t.Fatalf("escaped name: %d %q %v", pid, name, err)
	}
	if _, _, err := store.ParseInvalidate("nonsense"); err == nil {
		t.Fatal("want an error for a payload without a separator")
	}
}

func TestParseLevel(t *testing.T) {
	for _, ok := range []struct {
		flag  string
		level store.Level
	}{{"namespace", store.LevelNamespace}, {"dir", store.LevelDir}, {"file", store.LevelFile}} {
		if lvl, err := store.ParseLevel(ok.flag); err != nil || lvl != ok.level {
			t.Fatalf("ParseLevel(%q)=%v,%v", ok.flag, lvl, err)
		}
	}
	for _, bad := range []string{"directory", "", "whatever", "File"} {
		if _, err := store.ParseLevel(bad); err == nil {
			t.Fatalf("ParseLevel(%q) accepted an unusable level", bad)
		}
	}
}

func TestNewValidatesNamespaceAndLevel(t *testing.T) {
	_, client := testClient(t)

	if _, err := store.New(client, "bad/ns", store.LevelNamespace); err == nil {
		t.Fatal("namespace with a separator accepted")
	}
	if _, err := store.New(client, "", store.LevelNamespace); err != nil {
		t.Fatalf("empty namespace should default: %v", err)
	}
	for _, level := range store.Levels() {
		if _, err := store.New(client, "ok", level); err != nil {
			t.Fatalf("%s level refused: %v", level, err)
		}
	}
	if _, err := store.New(client, "ok", store.Level("directory")); err == nil {
		t.Fatal("level outside the schema accepted")
	}
}

// countingClient counts round trips: one Do is one command, one DoMulti is one
// pipelined batch, and one MULTI/EXEC batch on a dedicated connection is one.
type countingClient struct {
	rueidis.Client
	mu    sync.Mutex
	do    int
	multi int
}

func (c *countingClient) Do(ctx context.Context, cmd rueidis.Completed) rueidis.RedisResult {
	c.mu.Lock()
	c.do++
	c.mu.Unlock()
	return c.Client.Do(ctx, cmd)
}

func (c *countingClient) DoMulti(ctx context.Context, cmds ...rueidis.Completed) []rueidis.RedisResult {
	c.mu.Lock()
	c.multi++
	c.mu.Unlock()
	return c.Client.DoMulti(ctx, cmds...)
}

func (c *countingClient) Dedicated(fn func(rueidis.DedicatedClient) error) error {
	return c.Client.Dedicated(func(d rueidis.DedicatedClient) error {
		return fn(&countingDedicated{DedicatedClient: d, owner: c})
	})
}

// countingDedicated counts the batches a transaction sends on its dedicated
// connection, so a MULTI with several writes is one round trip.
type countingDedicated struct {
	rueidis.DedicatedClient
	owner *countingClient
}

func (d *countingDedicated) Do(ctx context.Context, cmd rueidis.Completed) rueidis.RedisResult {
	d.owner.mu.Lock()
	d.owner.do++
	d.owner.mu.Unlock()
	return d.DedicatedClient.Do(ctx, cmd)
}

func (d *countingDedicated) DoMulti(ctx context.Context, cmds ...rueidis.Completed) []rueidis.RedisResult {
	d.owner.mu.Lock()
	d.owner.multi++
	d.owner.mu.Unlock()
	return d.DedicatedClient.DoMulti(ctx, cmds...)
}

func (c *countingClient) reset() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := c.do + c.multi
	c.do, c.multi = 0, 0
	return n
}

func newlyCountingStore(t *testing.T, level store.Level) (*store.Store, *countingClient) {
	t.Helper()
	_, client := testClient(t)
	counting := &countingClient{Client: client}
	return bootstrappedAt(t, counting, level), counting
}

// TestComplexityCommitments asserts the round-trip promises from SPEC.md
// directly, rather than inferring them from the implementation.
// TestBootstrapCommitments pins the two-round-trip bootstrap: one pipelined
// probe of the three counter placements, then either the init script or the root read.
func TestBootstrapCommitments(t *testing.T) {
	_, client := testClient(t)
	counting := &countingClient{Client: client}
	st, err := store.New(counting, "test", store.LevelFile)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	counting.reset()
	if err := st.BootstrapRoot(ctx, 1000, 1000); err != nil {
		t.Fatal(err)
	}
	if got := counting.reset(); got != 2 {
		t.Fatalf("fresh bootstrap took %d round trips, want 2", got)
	}
	counting.reset()
	if err := st.BootstrapRoot(ctx, 1000, 1000); err != nil {
		t.Fatal(err)
	}
	if got := counting.reset(); got != 2 {
		t.Fatalf("existing-namespace bootstrap took %d round trips, want 2", got)
	}
}

func TestComplexityCommitments(t *testing.T) {
	st, counting := newlyCountingStore(t, store.LevelNamespace)
	ctx := context.Background()

	tests := []struct {
		name string
		op   func() error
		want int
	}{
		{
			name: "create",
			op: func() error {
				_, _, err := st.CreateFile(ctx, store.RootRef(), "f", 0644, 1, 1)
				return err
			},
			want: 1,
		},
		{
			name: "mkdir",
			op: func() error {
				_, _, err := st.Mkdir(ctx, store.RootRef(), "d", 0755, 1, 1)
				return err
			},
			want: 1,
		},
		{
			name: "update metadata",
			op: func() error {
				_, err := st.UpdateMeta(ctx, store.RootID, "f", func(m *store.Meta) {
					m.Mode = 0600
					m.Mtime = 1
				})
				return err
			},
			want: 2,
		},
		{
			name: "truncate",
			op: func() error {
				_, err := st.Truncate(ctx, store.RootID, "f", 10)
				return err
			},
			want: 3,
		},
		{
			name: "readdir",
			op: func() error {
				_, err := st.ListChildren(ctx, store.RootRef())
				return err
			},
			want: 1,
		},
		{
			name: "rename",
			op: func() error {
				_, err := st.Rename(ctx, store.RootRef(), "f", store.RootRef(), "f2")
				return err
			},
			want: 1,
		},
		{
			name: "unlink",
			op: func() error {
				return st.Unlink(ctx, store.RootRef(), "f2")
			},
			want: 1,
		},
		{
			name: "rmdir",
			op: func() error {
				return st.Rmdir(ctx, store.RootRef(), "d")
			},
			want: 1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			counting.reset()
			if err := tc.op(); err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
			if got := counting.reset(); got != tc.want {
				t.Fatalf("%s took %d round trips, want %d", tc.name, got, tc.want)
			}
		})
	}

	// A directory rename never touches its children: build a directory with
	// several entries and assert the rename stays one round trip.
	_, sub, err := st.Mkdir(ctx, store.RootRef(), "sub", 0755, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"a", "b", "c", "d", "e"} {
		if _, _, err := st.CreateFile(ctx, sub, name, 0644, 1, 1); err != nil {
			t.Fatal(err)
		}
	}
	counting.reset()
	if _, err := st.Rename(ctx, store.RootRef(), "sub", store.RootRef(), "renamed"); err != nil {
		t.Fatal(err)
	}
	if got := counting.reset(); got != 1 {
		t.Fatalf("directory rename took %d round trips, want 1", got)
	}
}

func TestUpdateMetaKeepsAChangeThatLandsBetweenItsReadAndWrite(t *testing.T) {
	st, _ := newTestStore(t)
	ctx := context.Background()
	if _, _, err := st.CreateFile(ctx, store.RootRef(), "f", 0644, 1, 1); err != nil {
		t.Fatal(err)
	}
	attempts := 0
	got, err := st.UpdateMeta(ctx, store.RootID, "f", func(m *store.Meta) {
		attempts++
		if attempts == 1 {
			chown(t, st, "f", 42)
		}
		m.Mode = 0600
	})
	if err != nil {
		t.Fatal(err)
	}
	if attempts != 2 {
		t.Fatalf("mutation ran %d times, want 2", attempts)
	}
	if got.UID != 42 || got.Mode != 0600 || got.Size != 0 {
		t.Fatalf("returned %+v, want uid 42, mode 0600, size 0", got)
	}
	stored, err := st.Stat(ctx, store.RootID, "f")
	if err != nil {
		t.Fatal(err)
	}
	if stored.UID != 42 || stored.Mode != 0600 {
		t.Fatalf("stored %+v: one of the two updates was lost", stored)
	}
}

func TestUpdateMetaGivesUpAsBusyWhenTheDocumentNeverSettles(t *testing.T) {
	st, _ := newTestStore(t)
	ctx := context.Background()
	if _, _, err := st.CreateFile(ctx, store.RootRef(), "f", 0644, 1, 1); err != nil {
		t.Fatal(err)
	}
	uid := uint32(100)
	_, err := st.UpdateMeta(ctx, store.RootID, "f", func(m *store.Meta) {
		uid++
		chown(t, st, "f", uid)
	})
	if !errors.Is(err, store.ErrBusy) {
		t.Fatalf("want ErrBusy, got %v", err)
	}
}

func TestUpdateMetaOfAMissingEntryLeavesTheConnectionUsable(t *testing.T) {
	st, _ := newTestStore(t)
	ctx := context.Background()
	if _, err := st.UpdateMeta(ctx, store.RootID, "nope", func(*store.Meta) {}); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
	// Creating the name changes the key the miss watched: a watch left on the
	// pooled connection would abort the next transaction once.
	if _, _, err := st.CreateFile(ctx, store.RootRef(), "nope", 0644, 1, 1); err != nil {
		t.Fatal(err)
	}
	attempts := 0
	if _, err := st.UpdateMeta(ctx, store.RootID, "nope", func(m *store.Meta) {
		attempts++
		m.Mode = 0600
	}); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 {
		t.Fatalf("mutation ran %d times after a miss, want 1: the miss left its watch behind", attempts)
	}
}

// chown changes an entry's owner as a second client would.
func chown(t *testing.T, st *store.Store, name string, uid uint32) {
	t.Helper()
	if _, err := st.UpdateMeta(context.Background(), store.RootID, name, func(m *store.Meta) { m.UID = uid }); err != nil {
		t.Fatal(err)
	}
}
