package store_test

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"testing"

	"kvfs/internal/store"

	"github.com/alicebob/miniredis/v2"
)

// fileStore builds a store at `file` level and creates its root.
func fileStore(t *testing.T) (*store.Store, *miniredis.Miniredis) {
	t.Helper()
	return newTestStoreAt(t, store.LevelFile)
}

func fileKey(parent uint64, name string, sfx store.Suffix) string {
	return store.Key("test", store.LevelFile, parent, name, sfx)
}

func rootListing() string { return fileKey(0, ".", store.SuffixDir) }

func assertKeys(t *testing.T, mr *miniredis.Miniredis, want ...string) {
	t.Helper()
	got := mr.Keys()
	sort.Strings(got)
	sort.Strings(want)
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("keys:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

func TestFileLevelLayout(t *testing.T) {
	st, mr := fileStore(t)
	ctx := context.Background()

	if got, err := mr.Get(fileKey(0, ".", store.SuffixCounter)); err != nil || got != "1" {
		t.Fatalf("counter=%q err=%v, want 1", got, err)
	}
	if mr.Exists(store.Key("test", store.LevelNamespace, 0, ".", store.SuffixCounter)) {
		t.Fatal("a namespace-level counter exists in a file-level namespace")
	}

	m, ref, err := st.CreateFile(ctx, store.RootRef(), "README.md", 0644, 1000, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if m.ID != 2 || ref.ID != 2 {
		t.Fatalf("meta=%+v ref=%+v", m, ref)
	}
	if got := mr.HGet(rootListing(), "README.md"); got != "f:2" {
		t.Fatalf("root field=%q, want a settled f:2", got)
	}
	// The entry's own shard is tagged with its name, so all three records live
	// in `{test/1/README.md}` and nowhere else.
	assertKeys(t, mr,
		"kvfs/{test/0/.}/counter",
		"kvfs/{test/0/.}/meta",
		"kvfs/{test/0/.}/dir",
		"kvfs/{test/1/README.md}/meta",
		"kvfs/{test/1/README.md}/data",
	)
}

func TestFileLevelEscapedNameTagsTheShard(t *testing.T) {
	st, mr := fileStore(t)
	ctx := context.Background()
	if _, _, err := st.CreateFile(ctx, store.RootRef(), "a/b c", 0644, 1, 1); err != nil {
		t.Fatal(err)
	}
	// The listing field is the raw basename; the key tag is the escaped one.
	if got := mr.HGet(rootListing(), "a/b c"); got != "f:2" {
		t.Fatalf("field=%q", got)
	}
	if !mr.Exists("kvfs/{test/1/a%2Fb c}/meta") {
		t.Fatalf("escaped tag missing, keys=%v", mr.Keys())
	}
}

// TestFileLevelProtocolStates walks the states a crash can leave at file level.
func TestFileLevelProtocolStates(t *testing.T) {
	st, mr := fileStore(t)
	ctx := context.Background()
	root := store.RootRef()

	if _, _, err := st.CreateFile(ctx, root, "half", 0644, 1, 1); err != nil {
		t.Fatal(err)
	}
	// 1. A prepared entry with no listing field is invisible to readdir.
	mr.HDel(rootListing(), "half")
	if children, err := st.ListChildren(ctx, root); err != nil {
		t.Fatal(err)
	} else if len(children) != 0 {
		t.Fatalf("a prepared entry was listed: %v", children)
	}
	if _, err := st.GetMeta(ctx, root.ID, "half"); err != nil {
		t.Fatalf("the prepared entry is gone: %v", err)
	}

	// 2. A pending field with complete metadata is reported as unknown type,
	// then settled by repair.
	mr.HSet(rootListing(), "half", "f:2:p")
	children, err := st.ListChildren(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	if len(children) != 1 || children[0].Type != "" || children[0].ID != 2 {
		t.Fatalf("pending field listed as %+v, want unknown type with inode 2", children)
	}
	if _, err := st.RepairEntry(ctx, root, "half"); err != nil {
		t.Fatal(err)
	}
	if got := mr.HGet(rootListing(), "half"); got != "f:2" {
		t.Fatalf("repair did not settle the field: %q", got)
	}

	// Repair is idempotent.
	if _, err := st.RepairEntry(ctx, root, "half"); err != nil {
		t.Fatal(err)
	}
	if got := mr.HGet(rootListing(), "half"); got != "f:2" {
		t.Fatalf("second repair changed the field: %q", got)
	}

	// 3. A field whose metadata is missing is removed.
	mr.HSet(rootListing(), "ghost", "f:77:p")
	if _, err := st.RepairEntry(ctx, root, "ghost"); err != nil {
		t.Fatal(err)
	}
	if mr.HGet(rootListing(), "ghost") != "" {
		t.Fatal("repair left a name pointing at nothing")
	}
	// Repairing a name that is not listed is a no-op.
	if _, err := st.RepairEntry(ctx, root, "ghost"); err != nil {
		t.Fatal(err)
	}
}

func TestFileLevelRepairReadsOnlyTheEntry(t *testing.T) {
	st, counting := newlyCountingStore(t, store.LevelFile)
	ctx := context.Background()

	for _, name := range []string{"a", "b", "c"} {
		if _, _, err := st.CreateFile(ctx, store.RootRef(), name, 0644, 1, 1); err != nil {
			t.Fatal(err)
		}
	}
	counting.reset()
	if _, err := st.RepairEntry(ctx, store.RootRef(), "a"); err != nil {
		t.Fatal(err)
	}
	// One HGET for the field and one GET for the metadata: no scan of the
	// listing and no read of any other entry.
	if got := counting.reset(); got != 2 {
		t.Fatalf("repair took %d round trips, want 2", got)
	}
}

func TestFileLevelRenameCopiesEntry(t *testing.T) {
	st, mr := fileStore(t)
	ctx := context.Background()

	_, conf, err := st.Mkdir(ctx, store.RootRef(), "conf", 0755, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	_, file, err := st.CreateFile(ctx, conf, "app.toml", 0644, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.WriteData(ctx, conf.ID, "app.toml", []byte("x=1"), true); err != nil {
		t.Fatal(err)
	}

	if _, err := st.Rename(ctx, conf, "app.toml", store.RootRef(), "app.toml"); err != nil {
		t.Fatal(err)
	}
	moved, err := st.GetMeta(ctx, store.RootID, "app.toml")
	if err != nil {
		t.Fatal(err)
	}
	if moved.ID != file.ID {
		t.Fatalf("inode changed on rename: %d -> %d", file.ID, moved.ID)
	}
	if data, err := st.GetData(ctx, store.RootID, "app.toml"); err != nil || string(data) != "x=1" {
		t.Fatalf("content did not move: %q err=%v", data, err)
	}
	if got := mr.HGet(rootListing(), "app.toml"); got != "f:3" {
		t.Fatalf("destination field=%q, want a settled f:3", got)
	}
	if got := mr.HGet(fileKey(conf.ParentID, conf.Name, store.SuffixDir), "app.toml"); got != "" {
		t.Fatalf("source field survived: %q", got)
	}
	// The source keys are gone: a stale dentry must not resurrect the old name.
	if mr.Exists(fileKey(conf.ID, "app.toml", store.SuffixMeta)) {
		t.Fatal("source metadata survived the move")
	}
	if _, err := st.GetMeta(ctx, conf.ID, "app.toml"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("old address still resolves: %v", err)
	}
}

func TestFileLevelRenameDirectoryCopiesListing(t *testing.T) {
	st, mr := fileStore(t)
	ctx := context.Background()

	_, sub, err := st.Mkdir(ctx, store.RootRef(), "sub1", 0755, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.CreateFile(ctx, sub, "README.md", 0644, 1, 1); err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.CreateFile(ctx, sub, "sub2", 0644, 1, 1); err != nil {
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
	if len(children) != 2 || children[0].Name != "README.md" || children[1].Name != "sub2" {
		t.Fatalf("copied listing=%v", children)
	}
	if got := mr.HGet(rootListing(), "mydir"); got != "d:2" {
		t.Fatalf("directory field=%q", got)
	}
	// The listing moved to the new shard; the children's keys did not move,
	// because they are addressed by the directory's inode.
	if !mr.Exists(fileKey(1, "mydir", store.SuffixDir)) {
		t.Fatal("the destination listing was not created")
	}
	if mr.Exists(fileKey(1, "sub1", store.SuffixDir)) {
		t.Fatal("the source listing survived the move")
	}
	if _, err := st.GetMeta(ctx, sub.ID, "README.md"); err != nil {
		t.Fatalf("a child key moved with the directory: %v", err)
	}

	// An empty directory has no listing key, and renaming it must not create one.
	if _, _, err := st.Mkdir(ctx, store.RootRef(), "empty", 0755, 1, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Rename(ctx, store.RootRef(), "empty", store.RootRef(), "void"); err != nil {
		t.Fatal(err)
	}
	if mr.Exists(fileKey(1, "void", store.SuffixDir)) {
		t.Fatal("renaming an empty directory created a listing")
	}
	if _, err := st.GetMeta(ctx, store.RootID, "void"); err != nil {
		t.Fatalf("the empty directory did not move: %v", err)
	}
}

func TestFileLevelRenamePendingDirectory(t *testing.T) {
	st, mr := fileStore(t)
	ctx := context.Background()

	_, sub, err := st.Mkdir(ctx, store.RootRef(), "sub", 0755, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.CreateFile(ctx, sub, "child", 0644, 1, 1); err != nil {
		t.Fatal(err)
	}
	// A crash left the field pending: another client may still be committing it,
	// so a rename refuses it until repair has settled the name.
	mr.HSet(rootListing(), "sub", "d:2:p")
	if _, err := st.Rename(ctx, store.RootRef(), "sub", store.RootRef(), "moved"); !errors.Is(err, store.ErrBusy) {
		t.Fatalf("rename of a pending name: want ErrBusy, got %v", err)
	}
	if _, err := st.RepairEntry(ctx, store.RootRef(), "sub"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Rename(ctx, store.RootRef(), "sub", store.RootRef(), "moved"); err != nil {
		t.Fatal(err)
	}
	if got := mr.HGet(rootListing(), "moved"); got != "d:2" {
		t.Fatalf("destination field=%q", got)
	}
	children, err := st.ListChildren(ctx, store.Ref{ParentID: store.RootID, Name: "moved", ID: sub.ID})
	if err != nil {
		t.Fatal(err)
	}
	if len(children) != 1 || children[0].Name != "child" {
		t.Fatalf("listing was not copied: %v", children)
	}
	if mr.Exists(fileKey(1, "sub", store.SuffixDir)) {
		t.Fatal("the source listing survived")
	}
}

func TestFileLevelRenameRefusals(t *testing.T) {
	st, mr := fileStore(t)
	ctx := context.Background()

	if _, _, err := st.CreateFile(ctx, store.RootRef(), "a", 0644, 1, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := st.WriteData(ctx, store.RootID, "a", []byte("payload"), true); err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.CreateFile(ctx, store.RootRef(), "b", 0644, 1, 1); err != nil {
		t.Fatal(err)
	}

	if _, err := st.RenameNoReplace(ctx, store.RootRef(), "a", store.RootRef(), "b"); !errors.Is(err, store.ErrExist) {
		t.Fatalf("want ErrExist, got %v", err)
	}
	// The refused move wrote nothing: the source is intact and the destination
	// field is still the plain value it had.
	if data, err := st.GetData(ctx, store.RootID, "a"); err != nil || string(data) != "payload" {
		t.Fatalf("source damaged by a refused rename: %q err=%v", data, err)
	}
	if got := mr.HGet(rootListing(), "b"); got != "f:3" {
		t.Fatalf("destination field=%q", got)
	}
	if _, err := st.Rename(ctx, store.RootRef(), "missing", store.RootRef(), "c"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
	if mr.HGet(rootListing(), "c") != "" {
		t.Fatal("a failed rename left a destination field")
	}
	if id, err := st.Rename(ctx, store.RootRef(), "a", store.RootRef(), "a"); err != nil || id != 0 {
		t.Fatalf("no-op rename: id=%d err=%v", id, err)
	}
}

func TestFileLevelUnlinkAndRmdir(t *testing.T) {
	st, mr := fileStore(t)
	ctx := context.Background()
	root := store.RootRef()

	if _, _, err := st.CreateFile(ctx, root, "f", 0644, 1, 1); err != nil {
		t.Fatal(err)
	}
	if err := st.Unlink(ctx, root, "f"); err != nil {
		t.Fatal(err)
	}
	if mr.Exists(fileKey(root.ID, "f", store.SuffixMeta)) || mr.Exists(fileKey(root.ID, "f", store.SuffixData)) {
		t.Fatal("unlink left the entry's keys behind")
	}
	if mr.HGet(rootListing(), "f") != "" {
		t.Fatal("unlink left the listing field behind")
	}
	if err := st.Unlink(ctx, root, "f"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("unlink of a missing name: %v", err)
	}

	_, dir, err := st.Mkdir(ctx, root, "d", 0755, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.CreateFile(ctx, dir, "child", 0644, 1, 1); err != nil {
		t.Fatal(err)
	}
	if err := st.Rmdir(ctx, root, "d"); !errors.Is(err, store.ErrNotEmpty) {
		t.Fatalf("want ErrNotEmpty, got %v", err)
	}
	if err := st.Unlink(ctx, root, "d"); !errors.Is(err, store.ErrIsDir) {
		t.Fatalf("unlink of a dir gave %v", err)
	}
	if err := st.Rmdir(ctx, root, "f2"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("rmdir of a missing name gave %v", err)
	}
	if _, _, err := st.CreateFile(ctx, root, "plain", 0644, 1, 1); err != nil {
		t.Fatal(err)
	}
	if err := st.Rmdir(ctx, root, "plain"); !errors.Is(err, store.ErrNotDir) {
		t.Fatalf("rmdir of a file gave %v", err)
	}
	if err := st.Unlink(ctx, dir, "child"); err != nil {
		t.Fatal(err)
	}
	if err := st.Rmdir(ctx, root, "d"); err != nil {
		t.Fatal(err)
	}
	if mr.Exists(fileKey(root.ID, "d", store.SuffixMeta)) || mr.Exists(fileKey(root.ID, "d", store.SuffixDir)) {
		t.Fatal("rmdir left the directory's keys behind")
	}
	if mr.HGet(rootListing(), "d") != "" {
		t.Fatal("rmdir left the listing field behind")
	}
}

func TestFileLevelCreateIsExclusiveUnderConcurrency(t *testing.T) {
	st, mr := fileStore(t)
	ctx := context.Background()

	const n = 8
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, err := st.CreateFile(ctx, store.RootRef(), "race", 0644, 1, 1)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)

	wins, refused := 0, 0
	for err := range errs {
		switch {
		case err == nil:
			wins++
		case errors.Is(err, store.ErrExist):
			refused++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if wins != 1 || refused != n-1 {
		t.Fatalf("wins=%d refused=%d, want 1 and %d", wins, refused, n-1)
	}
	// The losers wrote nothing, but the counter is spent: the allocator lives in
	// another shard, so the name cannot be tested before the allocation.
	if got, _ := mr.Get(fileKey(0, ".", store.SuffixCounter)); got != "9" {
		t.Fatalf("counter=%q, want 9 after 8 allocations", got)
	}
	children, err := st.ListChildren(ctx, store.RootRef())
	if err != nil {
		t.Fatal(err)
	}
	if len(children) != 1 || children[0].Type != store.TypeFile {
		t.Fatalf("listing=%v", children)
	}
	meta, err := st.GetMeta(ctx, store.RootID, "race")
	if err != nil {
		t.Fatal(err)
	}
	if meta.ID != children[0].ID {
		t.Fatalf("field points at %d, metadata at %d", children[0].ID, meta.ID)
	}
}

func TestFileLevelLevelInference(t *testing.T) {
	_, client := testClient(t)
	bootstrappedAt(t, client, store.LevelFile)

	ns, err := store.New(client, "test", store.LevelNamespace)
	if err != nil {
		t.Fatal(err)
	}
	err = ns.BootstrapRoot(context.Background(), 1000, 1000)
	if err == nil || !strings.Contains(err.Error(), "initialized at") {
		t.Fatalf("want a level-mismatch refusal, got %v", err)
	}
}

func TestFileLevelCosts(t *testing.T) {
	st, counting := newlyCountingStore(t, store.LevelFile)
	ctx := context.Background()
	root := store.RootRef()

	_, dir, err := st.Mkdir(ctx, root, "d", 0755, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.CreateFile(ctx, dir, "child", 0644, 1, 1); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		op   func() error
		want int
	}{
		{
			name: "create",
			op: func() error {
				_, _, err := st.CreateFile(ctx, root, "f", 0644, 1, 1)
				return err
			},
			want: 4, // allocate, claim, prepare, settle
		},
		{
			name: "mkdir",
			op: func() error {
				_, _, err := st.Mkdir(ctx, root, "empty", 0755, 1, 1)
				return err
			},
			want: 4,
		},
		{
			name: "readdir",
			op: func() error {
				_, err := st.ListChildren(ctx, root)
				return err
			},
			want: 1,
		},
		{
			name: "rename, same parent",
			op: func() error {
				_, err := st.Rename(ctx, root, "f", root, "f2")
				return err
			},
			want: 6, // field, claim, read, write, unlink source keys, commit and settle in one script
		},
		{
			name: "rename, across parents",
			op: func() error {
				_, err := st.Rename(ctx, root, "f2", dir, "moved")
				return err
			},
			want: 7,
		},
		{
			name: "unlink",
			op: func() error {
				return st.Unlink(ctx, dir, "moved")
			},
			want: 3, // field, delete keys, commit
		},
		{
			name: "rmdir",
			op: func() error {
				return st.Rmdir(ctx, root, "empty")
			},
			want: 3,
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

	// A directory rename copies its listing and touches no child: the cost is
	// bounded by the entry, never by the number of children.
	for _, name := range []string{"a", "b", "c", "d", "e"} {
		if _, _, err := st.CreateFile(ctx, dir, name, 0644, 1, 1); err != nil {
			t.Fatal(err)
		}
	}
	counting.reset()
	if _, err := st.Rename(ctx, root, "d", root, "renamed"); err != nil {
		t.Fatal(err)
	}
	if got := counting.reset(); got != 6 {
		t.Fatalf("directory rename took %d round trips, want 6", got)
	}
}
