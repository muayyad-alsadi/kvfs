package store_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"kvfs/internal/store"

	"github.com/alicebob/miniredis/v2"
)

const bigListing = 10000

// seedDirectory creates a directory under the root and fills its listing with n
// entries by writing the hash directly, so setup does not dominate the run.
func seedDirectory(b testing.TB, st *store.Store, mr *miniredis.Miniredis, level store.Level, name string, n int) store.Ref {
	b.Helper()
	_, ref, err := st.Mkdir(context.Background(), store.RootRef(), name, 0755, 1, 1)
	if err != nil {
		b.Fatal(err)
	}
	listing := store.Key("test", level, ref.ParentID, ref.Name, store.SuffixDir)
	for i := 0; i < n; i++ {
		mr.HSet(listing, fmt.Sprintf("entry-%05d", i), fmt.Sprintf("f:%d", i+100))
	}
	return ref
}

func BenchmarkReaddir10k(b *testing.B) {
	for _, level := range store.Levels() {
		b.Run(string(level), func(b *testing.B) {
			st, mr := newTestStoreAt(b, level)
			dir := seedDirectory(b, st, mr, level, "big", bigListing)
			ctx := context.Background()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				children, err := st.ListChildren(ctx, dir)
				if err != nil || len(children) != bigListing {
					b.Fatalf("children=%d err=%v", len(children), err)
				}
			}
		})
	}
}

func BenchmarkRenameDirectory10k(b *testing.B) {
	for _, level := range store.Levels() {
		b.Run(string(level), func(b *testing.B) {
			st, mr := newTestStoreAt(b, level)
			seedDirectory(b, st, mr, level, "big", bigListing)
			ctx := context.Background()
			root := store.RootRef()
			names := [2]string{"big", "big2"}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := st.Rename(ctx, root, names[i%2], root, names[(i+1)%2]); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkRenameFile1MiB(b *testing.B) {
	for _, level := range store.Levels() {
		b.Run(string(level), func(b *testing.B) {
			st, _ := newTestStoreAt(b, level)
			ctx := context.Background()
			root := store.RootRef()
			if _, _, err := st.CreateFile(ctx, root, "f0", 0644, 1, 1); err != nil {
				b.Fatal(err)
			}
			if _, err := st.WriteData(ctx, store.RootID, "f0", []byte(strings.Repeat("x", 1<<20)), true); err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := st.Rename(ctx, root, fmt.Sprintf("f%d", i), root, fmt.Sprintf("f%d", i+1)); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// TestPerformanceCommitments asserts the structural half of the spec's
// benchmark promises, which a timing run cannot: a 10k readdir is one round
// trip and a directory rename costs the same as an empty one's. That a file
// move transfers no body is asserted in TestFileMoveTransfersNoBody.
func TestPerformanceCommitments(t *testing.T) {
	for _, level := range []store.Level{store.LevelNamespace, store.LevelDir} {
		t.Run(string(level), func(t *testing.T) {
			mr, client := testClient(t)
			counting := &countingClient{Client: client}
			st := bootstrappedAt(t, counting, level)
			big := seedDirectory(t, st, mr, level, "big", bigListing)
			ctx := context.Background()
			root := store.RootRef()

			counting.reset()
			children, err := st.ListChildren(ctx, big)
			if err != nil || len(children) != bigListing {
				t.Fatalf("children=%d err=%v", len(children), err)
			}
			if got := counting.reset(); got != 1 {
				t.Fatalf("a 10k readdir took %d round trips, want 1", got)
			}

			if _, _, err := st.Mkdir(ctx, root, "empty", 0755, 1, 1); err != nil {
				t.Fatal(err)
			}
			counting.reset()
			if _, err := st.Rename(ctx, root, "empty", root, "empty2"); err != nil {
				t.Fatal(err)
			}
			emptyCost := counting.reset()
			if _, err := st.Rename(ctx, root, "big", root, "big2"); err != nil {
				t.Fatal(err)
			}
			if got := counting.reset(); got != emptyCost {
				t.Fatalf("a 10k directory rename took %d round trips, an empty one %d", got, emptyCost)
			}
		})
	}
}
