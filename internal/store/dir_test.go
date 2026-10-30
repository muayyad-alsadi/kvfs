package store_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"kvfs/internal/store"
)

func dirKeyOf(parent uint64, name string, sfx store.Suffix) string {
	return store.Key("test", store.LevelDir, parent, name, sfx)
}

func TestDirLevelLayout(t *testing.T) {
	st, mr := newTestStoreAt(t, store.LevelDir)
	ctx := context.Background()

	if got, err := mr.Get("kvfs/{test/0}/./counter"); err != nil || got != "1" {
		t.Fatalf("counter=%q err=%v, want 1", got, err)
	}
	if _, _, err := st.CreateFile(ctx, store.RootRef(), "README.md", 0644, 1000, 1000); err != nil {
		t.Fatal(err)
	}
	if got := mr.HGet(dirKeyOf(0, ".", store.SuffixDir), "README.md"); got != "f:2" {
		t.Fatalf("root field=%q, want a settled f:2", got)
	}
	// The tag holds the parent id and not the basename: siblings share a slot.
	assertKeys(t, mr,
		"kvfs/{test/0}/./counter",
		"kvfs/{test/0}/./meta",
		"kvfs/{test/0}/./dir",
		"kvfs/{test/1}/README.md/meta",
		"kvfs/{test/1}/README.md/data",
	)
}

func TestDirLevelRenameWithinParentMovesKeys(t *testing.T) {
	st, rec := newRecordingStore(t, store.LevelDir)
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

	rec.take()
	id, err := st.Rename(ctx, conf, "app.toml", conf, "app.cfg")
	if err != nil {
		t.Fatal(err)
	}
	if id != file.ID {
		t.Fatalf("inode changed on rename: %d -> %d", file.ID, id)
	}
	for _, c := range rec.take() {
		switch strings.ToUpper(c.args[0]) {
		case "GET", "SET", "GETRANGE", "SETRANGE":
			t.Fatalf("a file move transferred a body: %v", c.args)
		}
	}
	if data, err := st.GetData(ctx, conf.ID, "app.cfg"); err != nil || string(data) != "x=1" {
		t.Fatalf("content did not move: %q err=%v", data, err)
	}
	if _, err := st.GetMeta(ctx, conf.ID, "app.toml"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("old address still resolves: %v", err)
	}
	children, err := st.ListChildren(ctx, conf)
	if err != nil || len(children) != 1 || children[0].Name != "app.cfg" || children[0].Type != store.TypeFile {
		t.Fatalf("listing=%v err=%v", children, err)
	}
}

func TestDirLevelRenameDirectoryWithinParentMovesListing(t *testing.T) {
	st, rec := newRecordingStore(t, store.LevelDir)
	ctx := context.Background()

	_, sub, err := st.Mkdir(ctx, store.RootRef(), "sub1", 0755, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range []string{"a", "b", "c", "d"} {
		if _, _, err := st.CreateFile(ctx, sub, n, 0644, 1, 1); err != nil {
			t.Fatal(err)
		}
	}
	rec.take()
	if _, err := st.Rename(ctx, store.RootRef(), "sub1", store.RootRef(), "mydir"); err != nil {
		t.Fatal(err)
	}
	for _, c := range rec.take() {
		switch strings.ToUpper(c.args[0]) {
		case "HGETALL", "HSET":
			t.Fatalf("a same-parent directory rename copied its listing: %v", c.args)
		}
	}
	moved := store.Ref{ParentID: store.RootID, Name: "mydir", ID: sub.ID}
	children, err := st.ListChildren(ctx, moved)
	if err != nil || len(children) != 4 {
		t.Fatalf("moved listing=%v err=%v", children, err)
	}
	if _, err := st.GetMeta(ctx, sub.ID, "a"); err != nil {
		t.Fatalf("a child key moved with the directory: %v", err)
	}
}

func TestDirLevelRenameAcrossParentsCopiesTheEntry(t *testing.T) {
	st, mr := newTestStoreAt(t, store.LevelDir)
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
	if err != nil || moved.ID != file.ID {
		t.Fatalf("moved=%+v err=%v, want inode %d", moved, err, file.ID)
	}
	if data, err := st.GetData(ctx, store.RootID, "app.toml"); err != nil || string(data) != "x=1" {
		t.Fatalf("content did not move: %q err=%v", data, err)
	}
	if mr.Exists(dirKeyOf(conf.ID, "app.toml", store.SuffixMeta)) {
		t.Fatal("source metadata survived the move")
	}
	if got := mr.HGet(dirKeyOf(0, ".", store.SuffixDir), "app.toml"); got != "f:3" {
		t.Fatalf("destination field=%q, want a settled f:3", got)
	}
}

func TestDirLevelCosts(t *testing.T) {
	st, counting := newlyCountingStore(t, store.LevelDir)
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
		{"create", func() error { _, _, err := st.CreateFile(ctx, root, "f", 0644, 1, 1); return err }, 4},
		{"mkdir", func() error { _, _, err := st.Mkdir(ctx, root, "empty", 0755, 1, 1); return err }, 4},
		{"readdir", func() error { _, err := st.ListChildren(ctx, root); return err }, 1},
		// field, claim, move keys, commit and settle in one script
		{"rename, same parent", func() error { _, err := st.Rename(ctx, root, "f", root, "f2"); return err }, 4},
		{"rename, across parents", func() error { _, err := st.Rename(ctx, root, "f2", dir, "moved"); return err }, 7},
		{"unlink", func() error { return st.Unlink(ctx, dir, "moved") }, 3},
		{"rmdir", func() error { return st.Rmdir(ctx, root, "empty") }, 3},
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
}

func TestDirLevelUnlinkAndRmdir(t *testing.T) {
	st, mr := newTestStoreAt(t, store.LevelDir)
	ctx := context.Background()
	root := store.RootRef()

	if _, _, err := st.CreateFile(ctx, root, "f", 0644, 1, 1); err != nil {
		t.Fatal(err)
	}
	if err := st.Unlink(ctx, root, "f"); err != nil {
		t.Fatal(err)
	}
	if mr.Exists(dirKeyOf(1, "f", store.SuffixMeta)) || mr.HGet(dirKeyOf(0, ".", store.SuffixDir), "f") != "" {
		t.Fatal("unlink left keys behind")
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
	if err := st.Unlink(ctx, dir, "child"); err != nil {
		t.Fatal(err)
	}
	if err := st.Rmdir(ctx, root, "d"); err != nil {
		t.Fatal(err)
	}
	if mr.Exists(dirKeyOf(1, "d", store.SuffixMeta)) || mr.Exists(dirKeyOf(1, "d", store.SuffixDir)) {
		t.Fatal("rmdir left the directory's keys behind")
	}
}

func TestLevelInferenceAcrossAllLevels(t *testing.T) {
	for _, written := range store.Levels() {
		for _, mounted := range store.Levels() {
			if written == mounted {
				continue
			}
			t.Run(string(written)+" read as "+string(mounted), func(t *testing.T) {
				_, client := testClient(t)
				bootstrappedAt(t, client, written)
				other, err := store.New(client, "test", mounted)
				if err != nil {
					t.Fatal(err)
				}
				err = other.BootstrapRoot(context.Background(), 1000, 1000)
				if err == nil || !strings.Contains(err.Error(), "initialized at") {
					t.Fatalf("want a level-mismatch refusal, got %v", err)
				}
			})
		}
	}
}

func TestSplitNamespaceAcrossDirAndFileIsRefused(t *testing.T) {
	st, mr := newTestStoreAt(t, store.LevelDir)
	if err := mr.Set(store.Key("test", store.LevelFile, 0, ".", store.SuffixCounter), "1"); err != nil {
		t.Fatal(err)
	}
	err := st.BootstrapRoot(context.Background(), 1000, 1000)
	if err == nil || !strings.Contains(err.Error(), "more than one") {
		t.Fatalf("want split-namespace refusal, got %v", err)
	}
}

// TestFileMoveTransfersNoBody covers the spec's "a file move transfers no file
// body": at the default level, and within one parent at `dir` level, no command
// of the move reads or writes content.
func TestFileMoveTransfersNoBody(t *testing.T) {
	for _, level := range []store.Level{store.LevelNamespace, store.LevelDir} {
		t.Run(string(level), func(t *testing.T) {
			st, rec := newRecordingStore(t, level)
			ctx := context.Background()
			root := store.RootRef()
			if _, _, err := st.CreateFile(ctx, root, "big", 0644, 1, 1); err != nil {
				t.Fatal(err)
			}
			if _, err := st.WriteData(ctx, store.RootID, "big", []byte(strings.Repeat("x", 1<<20)), true); err != nil {
				t.Fatal(err)
			}
			rec.take()
			if _, err := st.Rename(ctx, root, "big", root, "bigger"); err != nil {
				t.Fatal(err)
			}
			for _, c := range rec.take() {
				for _, a := range c.args {
					if len(a) > 1<<10 && strings.ToUpper(c.args[0]) != "EVAL" {
						t.Fatalf("a move carried %d bytes in %s", len(a), c.args[0])
					}
				}
				switch strings.ToUpper(c.args[0]) {
				case "GET", "SET", "GETRANGE", "SETRANGE":
					t.Fatalf("a file move touched content: %v", c.args[0])
				}
			}
		})
	}
}
