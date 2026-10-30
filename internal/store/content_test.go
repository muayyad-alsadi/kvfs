package store_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"kvfs/internal/store"
)

func newContentStore(t *testing.T, level store.Level, name string) (*store.Store, store.Ref) {
	t.Helper()
	st, _ := newTestStoreAt(t, level)
	if _, _, err := st.CreateFile(context.Background(), store.RootRef(), name, 0644, 1, 1); err != nil {
		t.Fatal(err)
	}
	return st, store.RootRef()
}

func TestRangeReadsAndWrites(t *testing.T) {
	steps := []struct {
		name string
		off  int64
		data string
		want string // the whole content afterwards
	}{
		{"first write", 0, "hello", "hello"},
		{"overwrite in the middle", 1, "EY", "hEYlo"},
		{"append", 5, " world", "hEYlo world"},
		{"write past the end pads with zeros", 14, "!", "hEYlo world\x00\x00\x00!"},
		{"empty write changes nothing", 3, "", "hEYlo world\x00\x00\x00!"},
	}
	for _, level := range store.Levels() {
		t.Run(string(level), func(t *testing.T) {
			st, root := newContentStore(t, level, "f")
			ctx := context.Background()
			for _, s := range steps {
				if err := st.WriteAt(ctx, root.ID, "f", s.off, []byte(s.data)); err != nil {
					t.Fatalf("%s: %v", s.name, err)
				}
				got, err := st.ReadAt(ctx, root.ID, "f", 0, 1<<10)
				if err != nil || string(got) != s.want {
					t.Fatalf("%s: content %q err=%v, want %q", s.name, got, err, s.want)
				}
				m, err := st.Stat(ctx, root.ID, "f")
				if err != nil || m.Size != uint64(len(s.want)) {
					t.Fatalf("%s: size %v err=%v, want %d", s.name, m, err, len(s.want))
				}
			}
			if got, _ := st.ReadAt(ctx, root.ID, "f", 3, 4); string(got) != "lo w" {
				t.Fatalf("a range read returned %q", got)
			}
			if got, _ := st.ReadAt(ctx, root.ID, "f", 100, 4); len(got) != 0 {
				t.Fatalf("a read past the end returned %q", got)
			}
		})
	}
}

func TestTruncate(t *testing.T) {
	cases := []struct {
		name string
		size uint64
		want string
	}{
		{"shrink", 3, "abc"},
		{"to zero", 0, ""},
		{"same size", 6, "abcdef"},
		{"grow pads with zeros", 8, "abcdef\x00\x00"},
	}
	for _, level := range store.Levels() {
		for _, tc := range cases {
			t.Run(string(level)+"/"+tc.name, func(t *testing.T) {
				st, root := newContentStore(t, level, "f")
				ctx := context.Background()
				if err := st.WriteAt(ctx, root.ID, "f", 0, []byte("abcdef")); err != nil {
					t.Fatal(err)
				}
				m, err := st.Truncate(ctx, root.ID, "f", tc.size)
				if err != nil || m.Size != tc.size {
					t.Fatalf("truncate: %+v err=%v", m, err)
				}
				got, _ := st.ReadAt(ctx, root.ID, "f", 0, 100)
				if string(got) != tc.want {
					t.Fatalf("content %q, want %q", got, tc.want)
				}
			})
		}
	}
}

// A handle whose file was unlinked must not recreate the content key.
func TestWriteToUnlinkedFileIsRefused(t *testing.T) {
	for _, level := range store.Levels() {
		t.Run(string(level), func(t *testing.T) {
			st, mr := newTestStoreAt(t, level)
			ctx := context.Background()
			if _, _, err := st.CreateFile(ctx, store.RootRef(), "gone", 0644, 1, 1); err != nil {
				t.Fatal(err)
			}
			if err := st.Unlink(ctx, store.RootRef(), "gone"); err != nil {
				t.Fatal(err)
			}
			if err := st.WriteAt(ctx, store.RootID, "gone", 0, []byte("x")); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("write: want ErrNotFound, got %v", err)
			}
			if _, err := st.Truncate(ctx, store.RootID, "gone", 4); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("truncate: want ErrNotFound, got %v", err)
			}
			for _, k := range mr.Keys() {
				if strings.Contains(k, "gone") {
					t.Fatalf("a refused write left %q", k)
				}
			}
		})
	}
}

// The size is the content's length: the metadata document stores none.
func TestMetadataStoresNoSize(t *testing.T) {
	st, mr := newTestStoreAt(t, store.LevelNamespace)
	ctx := context.Background()
	if _, _, err := st.CreateFile(ctx, store.RootRef(), "f", 0644, 1, 1); err != nil {
		t.Fatal(err)
	}
	if err := st.WriteAt(ctx, store.RootID, "f", 0, []byte("12345")); err != nil {
		t.Fatal(err)
	}
	raw, err := mr.Get(store.Key("test", store.LevelNamespace, store.RootID, "f", store.SuffixMeta))
	if err != nil || strings.Contains(raw, "size") {
		t.Fatalf("metadata %q err=%v", raw, err)
	}
}

// Two clients are two mounts, possibly on two hosts: a write by one is read by
// the other with no reopen, because neither holds a copy.
func TestWritesAreSeenByAnotherClient(t *testing.T) {
	for _, level := range store.Levels() {
		t.Run(string(level), func(t *testing.T) {
			st, mr := newTestStoreAt(t, level)
			ctx := context.Background()
			if _, _, err := st.CreateFile(ctx, store.RootRef(), "db", 0644, 1, 1); err != nil {
				t.Fatal(err)
			}
			other := bootstrappedAt(t, clientAt(t, mr), level)
			if err := st.WriteAt(ctx, store.RootID, "db", 0, []byte("page1")); err != nil {
				t.Fatal(err)
			}
			if err := other.WriteAt(ctx, store.RootID, "db", 5, []byte("page2")); err != nil {
				t.Fatal(err)
			}
			for name, s := range map[string]*store.Store{"first": st, "second": other} {
				got, err := s.ReadAt(ctx, store.RootID, "db", 0, 100)
				if err != nil || string(got) != "page1page2" {
					t.Fatalf("%s client read %q err=%v", name, got, err)
				}
			}
		})
	}
}

// The cost of a write is the bytes written, not the size of the file, and it
// is one command with no metadata read.
func TestWriteCostDoesNotDependOnFileSize(t *testing.T) {
	for _, level := range store.Levels() {
		t.Run(string(level), func(t *testing.T) {
			st, rec := newRecordingStore(t, level)
			ctx := context.Background()
			if _, _, err := st.CreateFile(ctx, store.RootRef(), "big", 0644, 1, 1); err != nil {
				t.Fatal(err)
			}
			if err := st.WriteAt(ctx, store.RootID, "big", 0, bytes.Repeat([]byte("x"), 1<<20)); err != nil {
				t.Fatal(err)
			}
			rec.take()

			if err := st.WriteAt(ctx, store.RootID, "big", 512, []byte("abcd")); err != nil {
				t.Fatal(err)
			}
			cmds := rec.take()
			if len(cmds) != 1 || !strings.EqualFold(cmds[0].args[0], "EVAL") {
				t.Fatalf("a write sent %d commands: %v", len(cmds), cmds)
			}
			if n := commandBytes(cmds[0]); n > 600 {
				t.Fatalf("a 4-byte write sent %d bytes", n)
			}

			if _, err := st.ReadAt(ctx, store.RootID, "big", 0, 16); err != nil {
				t.Fatal(err)
			}
			if _, err := st.Stat(ctx, store.RootID, "big"); err != nil {
				t.Fatal(err)
			}
			cmds = rec.take()
			if len(cmds) != 3 {
				t.Fatalf("a range read and a stat sent %d commands, want 3: %v", len(cmds), cmds)
			}
		})
	}
}

func commandBytes(c sent) int {
	n := 0
	for _, a := range c.args {
		n += len(a)
	}
	return n
}
