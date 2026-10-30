package store_test

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"

	"kvfs/internal/store"
)

// A creator that holds a stale reference to a directory must be refused after
// the directory is removed or replaced, and must write nothing under the old
// inode. The levels that run the link protocol are the ones that need the check.
func TestStaleParentIsRefused(t *testing.T) {
	cases := []struct {
		name    string
		retire  func(t *testing.T, st *store.Store, victim store.Ref)
		attempt func(st *store.Store, victim store.Ref) error
	}{
		{"create after rmdir", rmdirVictim, createInto},
		{"mkdir after rmdir", rmdirVictim, mkdirInto},
		{"rename into after rmdir", rmdirVictim, renameInto},
		{"create after the directory was replaced by a rename", replaceVictim, createInto},
	}
	for _, level := range store.Levels() {
		for _, tc := range cases {
			t.Run(string(level)+"/"+tc.name, func(t *testing.T) {
				st, mr := newTestStoreAt(t, level)
				ctx := context.Background()
				_, victim, err := st.Mkdir(ctx, store.RootRef(), "victim", 0755, 1, 1)
				if err != nil {
					t.Fatal(err)
				}
				tc.retire(t, st, victim)
				if err := tc.attempt(st, victim); !errors.Is(err, store.ErrNotFound) {
					t.Fatalf("want ErrNotFound, got %v", err)
				}
				assertNothingUnderInode(t, mr.Keys(), victim.ID)
			})
		}
	}
}

func rmdirVictim(t *testing.T, st *store.Store, victim store.Ref) {
	t.Helper()
	if err := st.Rmdir(context.Background(), store.RootRef(), "victim"); err != nil {
		t.Fatal(err)
	}
}

// replaceVictim renames another empty directory over the victim.
func replaceVictim(t *testing.T, st *store.Store, victim store.Ref) {
	t.Helper()
	ctx := context.Background()
	if _, _, err := st.Mkdir(ctx, store.RootRef(), "other", 0755, 1, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Rename(ctx, store.RootRef(), "other", store.RootRef(), "victim"); err != nil {
		t.Fatal(err)
	}
}

func createInto(st *store.Store, victim store.Ref) error {
	_, _, err := st.CreateFile(context.Background(), victim, "late", 0644, 1, 1)
	return err
}

func mkdirInto(st *store.Store, victim store.Ref) error {
	_, _, err := st.Mkdir(context.Background(), victim, "late", 0755, 1, 1)
	return err
}

func renameInto(st *store.Store, victim store.Ref) error {
	ctx := context.Background()
	if _, _, err := st.CreateFile(ctx, store.RootRef(), "mover", 0644, 1, 1); err != nil {
		return err
	}
	_, err := st.Rename(ctx, store.RootRef(), "mover", victim, "late")
	return err
}

// assertNothingUnderInode fails if any key names a child of the retired inode.
func assertNothingUnderInode(t *testing.T, keys []string, id uint64) {
	t.Helper()
	for _, k := range keys {
		for _, tag := range []string{"/" + itoa(id) + "/late/", "/" + itoa(id) + "/late}/", "/" + itoa(id) + "}/late/"} {
			if contains(k, tag) {
				t.Fatalf("a refused creator left %q", k)
			}
		}
	}
}

func itoa(n uint64) string { return strconv.FormatUint(n, 10) }

func contains(s, sub string) bool { return strings.Contains(s, sub) }

// A create racing an rmdir has exactly two outcomes, and neither leaves keys
// under a removed directory: the create wins and rmdir sees a child, or rmdir
// wins and the create is refused.
func TestCreateRacingRmdirLeavesNoOrphans(t *testing.T) {
	for _, level := range store.Levels() {
		t.Run(string(level), func(t *testing.T) {
			st, mr := newTestStoreAt(t, level)
			ctx := context.Background()
			for i := 0; i < 40; i++ {
				_, victim, err := st.Mkdir(ctx, store.RootRef(), "victim", 0755, 1, 1)
				if err != nil {
					t.Fatal(err)
				}
				var createErr, rmdirErr error
				var wg sync.WaitGroup
				wg.Add(2)
				go func() { defer wg.Done(); _, _, createErr = st.CreateFile(ctx, victim, "late", 0644, 1, 1) }()
				go func() { defer wg.Done(); rmdirErr = st.Rmdir(ctx, store.RootRef(), "victim") }()
				wg.Wait()

				removed := rmdirErr == nil
				if removed != errors.Is(createErr, store.ErrNotFound) {
					t.Fatalf("round %d: rmdir=%v create=%v: exactly one must win", i, rmdirErr, createErr)
				}
				if removed {
					assertNothingUnderInode(t, mr.Keys(), victim.ID)
					continue
				}
				if !errors.Is(rmdirErr, store.ErrNotEmpty) {
					t.Fatalf("round %d: rmdir=%v", i, rmdirErr)
				}
				if _, err := st.GetMeta(ctx, victim.ID, "late"); err != nil {
					t.Fatalf("round %d: the winning create is unreadable: %v", i, err)
				}
				cleanup(t, st, victim)
			}
		})
	}
}

// cleanup empties and removes the victim so the next round can reuse the name.
func cleanup(t *testing.T, st *store.Store, victim store.Ref) {
	t.Helper()
	ctx := context.Background()
	if err := st.Unlink(ctx, victim, "late"); err != nil {
		t.Fatal(err)
	}
	if err := st.Rmdir(ctx, store.RootRef(), "victim"); err != nil {
		t.Fatal(err)
	}
}
