package store_test

import (
	"context"
	"errors"
	"testing"

	"kvfs/internal/store"
)

// Each case builds a source and a destination at the root, renames the source
// onto the destination, and states the POSIX outcome.
func TestRenameReplacesLikePOSIX(t *testing.T) {
	type entry struct {
		name string
		kind string // "file", "empty-file", "dir", "full-dir", "link"
	}
	cases := []struct {
		name     string
		src, dst entry
		want     error
	}{
		{"file over file", entry{"src", "file"}, entry{"dst", "file"}, nil},
		{"file over empty file", entry{"src", "file"}, entry{"dst", "empty-file"}, nil},
		{"empty file over file", entry{"src", "empty-file"}, entry{"dst", "file"}, nil},
		{"file over link", entry{"src", "file"}, entry{"dst", "link"}, nil},
		{"link over file", entry{"src", "link"}, entry{"dst", "file"}, nil},
		{"dir over empty dir", entry{"src", "full-dir"}, entry{"dst", "dir"}, nil},
		{"empty dir over empty dir", entry{"src", "dir"}, entry{"dst", "dir"}, nil},
		{"file over dir", entry{"src", "file"}, entry{"dst", "dir"}, store.ErrIsDir},
		{"dir over file", entry{"src", "dir"}, entry{"dst", "file"}, store.ErrNotDir},
		{"dir over non-empty dir", entry{"src", "dir"}, entry{"dst", "full-dir"}, store.ErrNotEmpty},
	}
	for _, level := range store.Levels() {
		for _, tc := range cases {
			t.Run(string(level)+"/"+tc.name, func(t *testing.T) {
				st, _ := newTestStoreAt(t, level)
				ctx := context.Background()
				srcID := build(t, st, tc.src.name, tc.src.kind)
				dstID := build(t, st, tc.dst.name, tc.dst.kind)

				id, err := st.Rename(ctx, store.RootRef(), "src", store.RootRef(), "dst")
				if tc.want != nil {
					assertRefused(t, st, tc.want, err, srcID, dstID)
					return
				}
				assertReplaced(t, st, tc.src.kind, id, err, srcID)
			})
		}
	}
}

func build(t *testing.T, st *store.Store, name, kind string) uint64 {
	t.Helper()
	ctx := context.Background()
	root := store.RootRef()
	switch kind {
	case "dir", "full-dir":
		m, ref, err := st.Mkdir(ctx, root, name, 0755, 1, 1)
		if err != nil {
			t.Fatal(err)
		}
		if kind == "full-dir" {
			if _, _, err := st.CreateFile(ctx, ref, "child", 0644, 1, 1); err != nil {
				t.Fatal(err)
			}
		}
		return m.ID
	case "link":
		m, _, err := st.Symlink(ctx, root, name, "target", 1, 1)
		if err != nil {
			t.Fatal(err)
		}
		return m.ID
	}
	m, _, err := st.CreateFile(ctx, root, name, 0644, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if kind == "file" {
		if _, err := st.WriteData(ctx, store.RootID, name, []byte("payload-"+name), true); err != nil {
			t.Fatal(err)
		}
	}
	return m.ID
}

func assertRefused(t *testing.T, st *store.Store, want, got error, srcID, dstID uint64) {
	t.Helper()
	ctx := context.Background()
	if !errors.Is(got, want) {
		t.Fatalf("want %v, got %v", want, got)
	}
	// A refused rename changes nothing: both names still hold their own inode.
	for name, id := range map[string]uint64{"src": srcID, "dst": dstID} {
		kids, err := st.ListChildren(ctx, store.RootRef())
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, k := range kids {
			found = found || (k.Name == name && k.ID == id)
		}
		if !found {
			t.Fatalf("%s lost its inode %d after a refused rename: %v", name, id, kids)
		}
	}
}

func assertReplaced(t *testing.T, st *store.Store, kind string, id uint64, err error, srcID uint64) {
	t.Helper()
	ctx := context.Background()
	if err != nil {
		t.Fatalf("rename: %v", err)
	}
	if id != srcID {
		t.Fatalf("inode changed: %d -> %d", srcID, id)
	}
	if _, err := st.GetMeta(ctx, store.RootID, "src"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("source survived: %v", err)
	}
	m, err := st.GetMeta(ctx, store.RootID, "dst")
	if err != nil || m.ID != srcID {
		t.Fatalf("destination meta: %+v err=%v", m, err)
	}
	assertContent(t, st, kind, srcID)
	kids, err := st.ListChildren(ctx, store.RootRef())
	if err != nil || len(kids) != 1 || kids[0].Name != "dst" || kids[0].ID != srcID {
		t.Fatalf("listing: %v err=%v", kids, err)
	}
}

// assertContent checks the replaced name carries the source's content and
// nothing of the entry it replaced.
func assertContent(t *testing.T, st *store.Store, kind string, srcID uint64) {
	t.Helper()
	ctx := context.Background()
	data, err := st.GetData(ctx, store.RootID, "dst")
	switch kind {
	case "file":
		if err != nil || string(data) != "payload-src" {
			t.Fatalf("content %q err=%v", data, err)
		}
	case "link":
		if err != nil || string(data) != "target" {
			t.Fatalf("link target %q err=%v", data, err)
		}
	case "empty-file", "dir", "full-dir":
		if err == nil && len(data) != 0 {
			t.Fatalf("stale content survived: %q", data)
		}
	}
	if kind == "full-dir" {
		kids, err := st.ListChildren(ctx, store.Ref{ParentID: store.RootID, Name: "dst", ID: srcID})
		if err != nil || len(kids) != 1 || kids[0].Name != "child" {
			t.Fatalf("children of the moved directory: %v err=%v", kids, err)
		}
	}
}

func TestRenameNoReplaceRefusesEveryLevel(t *testing.T) {
	for _, level := range store.Levels() {
		t.Run(string(level), func(t *testing.T) {
			st, _ := newTestStoreAt(t, level)
			ctx := context.Background()
			srcID := build(t, st, "src", "file")
			dstID := build(t, st, "dst", "file")
			_, err := st.RenameNoReplace(ctx, store.RootRef(), "src", store.RootRef(), "dst")
			assertRefused(t, st, store.ErrExist, err, srcID, dstID)
		})
	}
}
