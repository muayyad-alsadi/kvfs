package store_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"kvfs/internal/store"

	"github.com/alicebob/miniredis/v2"
)

// linkedLevels are the levels whose link operations span slots.
func linkedLevels() []store.Level { return []store.Level{store.LevelDir, store.LevelFile} }

func listingOf(level store.Level, dir store.Ref) string {
	return store.Key("test", level, dir.ParentID, dir.Name, store.SuffixDir)
}

func forLinkedLevels(t *testing.T, fn func(t *testing.T, level store.Level)) {
	t.Helper()
	for _, level := range linkedLevels() {
		t.Run(string(level), func(t *testing.T) { fn(t, level) })
	}
}

func TestLinkedPendingNamesAreBusy(t *testing.T) {
	forLinkedLevels(t, func(t *testing.T, level store.Level) {
		st, mr := newTestStoreAt(t, level)
		ctx := context.Background()
		root := store.RootRef()

		if _, _, err := st.CreateFile(ctx, root, "f", 0644, 1, 1); err != nil {
			t.Fatal(err)
		}
		if _, err := st.WriteData(ctx, store.RootID, "f", []byte("keep"), true); err != nil {
			t.Fatal(err)
		}
		if _, _, err := st.Mkdir(ctx, root, "d", 0755, 1, 1); err != nil {
			t.Fatal(err)
		}
		listing := listingOf(level, root)
		mr.HSet(listing, "f", "f:2:p")
		mr.HSet(listing, "d", "d:3:p")

		if err := st.Unlink(ctx, root, "f"); !errors.Is(err, store.ErrBusy) {
			t.Fatalf("unlink of a pending name: %v", err)
		}
		if err := st.Rmdir(ctx, root, "d"); !errors.Is(err, store.ErrBusy) {
			t.Fatalf("rmdir of a pending name: %v", err)
		}
		if _, err := st.Rename(ctx, root, "f", root, "g"); !errors.Is(err, store.ErrBusy) {
			t.Fatalf("rename of a pending name: %v", err)
		}
		if data, err := st.GetData(ctx, store.RootID, "f"); err != nil || string(data) != "keep" {
			t.Fatalf("a refused operation damaged the entry: %q err=%v", data, err)
		}
		if _, err := st.GetMeta(ctx, store.RootID, "d"); err != nil {
			t.Fatalf("a refused rmdir removed the directory: %v", err)
		}
	})
}

// halfMovedRename builds the state a crash leaves after a rename copied the
// entry into the destination and before the source keys were removed.
func halfMovedRename(t *testing.T, level store.Level) (*store.Store, *miniredis.Miniredis, store.Ref, store.Ref) {
	t.Helper()
	st, mr := newTestStoreAt(t, level)
	ctx := context.Background()
	root := store.RootRef()
	_, conf, err := st.Mkdir(ctx, root, "conf", 0755, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.CreateFile(ctx, root, "a", 0644, 1, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := st.WriteData(ctx, store.RootID, "a", []byte("payload"), true); err != nil {
		t.Fatal(err)
	}
	copyKey := func(sfx store.Suffix) {
		v, err := mr.Get(store.Key("test", level, store.RootID, "a", sfx))
		if err != nil {
			t.Fatal(err)
		}
		if err := mr.Set(store.Key("test", level, conf.ID, "b", sfx), v); err != nil {
			t.Fatal(err)
		}
	}
	copyKey(store.SuffixMeta)
	copyKey(store.SuffixData)
	pending := store.Child{Type: store.TypeFile, ID: 3}.MovePendingValue(store.InvalidatePayload(store.RootID, "a"))
	mr.HSet(listingOf(level, conf), "b", pending)
	return st, mr, root, conf
}

func TestRepairDiscardsARenameThatNeverTookTheSource(t *testing.T) {
	forLinkedLevels(t, func(t *testing.T, level store.Level) {
		st, mr, root, conf := halfMovedRename(t, level)
		ctx := context.Background()

		got, err := st.RepairEntry(ctx, conf, "b")
		if err != nil || got != store.RepairDiscarded {
			t.Fatalf("repair=%q err=%v, want discarded", got, err)
		}
		if mr.HGet(listingOf(level, conf), "b") != "" || mr.Exists(store.Key("test", level, conf.ID, "b", store.SuffixMeta)) {
			t.Fatal("the destination copy survived")
		}
		if data, err := st.GetData(ctx, root.ID, "a"); err != nil || string(data) != "payload" {
			t.Fatalf("the source lost its content: %q err=%v", data, err)
		}
		if got := mr.HGet(listingOf(level, root), "a"); got != "f:3" {
			t.Fatalf("source field=%q", got)
		}
	})
}

func TestRepairSettlesARenameThatTookTheSource(t *testing.T) {
	forLinkedLevels(t, func(t *testing.T, level store.Level) {
		st, mr, root, conf := halfMovedRename(t, level)
		ctx := context.Background()

		// The source keys are gone but its field is still listed.
		mr.Del(store.Key("test", level, root.ID, "a", store.SuffixMeta))
		mr.Del(store.Key("test", level, root.ID, "a", store.SuffixData))

		if got, err := st.RepairEntry(ctx, conf, "b"); err != nil || got != store.RepairSettled {
			t.Fatalf("repair=%q err=%v, want settled", got, err)
		}
		if got, err := st.RepairEntry(ctx, root, "a"); err != nil || got != store.RepairRemoved {
			t.Fatalf("source repair=%q err=%v, want removed", got, err)
		}
		if data, err := st.GetData(ctx, conf.ID, "b"); err != nil || string(data) != "payload" {
			t.Fatalf("the destination lost its content: %q err=%v", data, err)
		}
		// Exactly one name owns inode 3.
		if mr.HGet(listingOf(level, root), "a") != "" || mr.HGet(listingOf(level, conf), "b") != "f:3" {
			t.Fatalf("names after repair: a=%q b=%q", mr.HGet(listingOf(level, root), "a"), mr.HGet(listingOf(level, conf), "b"))
		}
	})
}

func TestConcurrentRenamesOfOneSourceHaveOneWinner(t *testing.T) {
	forLinkedLevels(t, func(t *testing.T, level store.Level) {
		st, mr := newTestStoreAt(t, level)
		ctx := context.Background()
		root := store.RootRef()
		if _, _, err := st.CreateFile(ctx, root, "src", 0644, 1, 1); err != nil {
			t.Fatal(err)
		}
		if _, err := st.WriteData(ctx, store.RootID, "src", []byte("once"), true); err != nil {
			t.Fatal(err)
		}

		const n = 6
		var wg sync.WaitGroup
		errs := make(chan error, n)
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := st.Rename(ctx, root, "src", root, fmt.Sprintf("dst%d", i))
				errs <- err
			}()
		}
		wg.Wait()
		close(errs)

		wins := 0
		for err := range errs {
			switch {
			case err == nil:
				wins++
			case errors.Is(err, store.ErrNotFound):
			default:
				t.Fatalf("unexpected error: %v", err)
			}
		}
		if wins != 1 {
			t.Fatalf("wins=%d, want exactly one", wins)
		}
		children, err := st.ListChildren(ctx, root)
		if err != nil || len(children) != 1 || children[0].Pending {
			t.Fatalf("listing=%v err=%v, want one settled name", children, err)
		}
		data, err := st.GetData(ctx, store.RootID, children[0].Name)
		if err != nil || string(data) != "once" {
			t.Fatalf("the surviving name lost its content: %q err=%v", data, err)
		}
		bodies := 0
		for _, k := range mr.Keys() {
			if len(k) > 5 && k[len(k)-5:] == "/data" {
				bodies++
			}
		}
		if bodies != 1 {
			t.Fatalf("%d content keys survive, want 1: %v", bodies, mr.Keys())
		}
	})
}

func TestUnlinkRacingRenameKeepsDataReachableOrGone(t *testing.T) {
	forLinkedLevels(t, func(t *testing.T, level store.Level) {
		for i := 0; i < 20; i++ {
			st, _ := newTestStoreAt(t, level)
			ctx := context.Background()
			root := store.RootRef()
			if _, _, err := st.CreateFile(ctx, root, "a", 0644, 1, 1); err != nil {
				t.Fatal(err)
			}
			if _, _, err := st.CreateFile(ctx, root, "b", 0644, 1, 1); err != nil {
				t.Fatal(err)
			}
			// Rename a over b's name while b is being unlinked: after a, the
			// listing must hold no field without keys and no keys without field.
			var wg sync.WaitGroup
			wg.Add(2)
			go func() { defer wg.Done(); _ = st.Unlink(ctx, root, "b") }()
			go func() { defer wg.Done(); _, _ = st.Rename(ctx, root, "a", root, "b") }()
			wg.Wait()

			children, err := st.ListChildren(ctx, root)
			if err != nil {
				t.Fatal(err)
			}
			for _, c := range children {
				if c.Pending {
					t.Fatalf("iteration %d: %s left pending", i, c.Name)
				}
				if _, err := st.GetMeta(ctx, store.RootID, c.Name); err != nil {
					t.Fatalf("iteration %d: %s is listed without metadata: %v", i, c.Name, err)
				}
			}
		}
	})
}

func TestRepairPathWalksFromTheRoot(t *testing.T) {
	forLinkedLevels(t, func(t *testing.T, level store.Level) {
		st, mr := newTestStoreAt(t, level)
		ctx := context.Background()
		_, conf, err := st.Mkdir(ctx, store.RootRef(), "conf", 0755, 1, 1)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := st.CreateFile(ctx, conf, "x", 0644, 1, 1); err != nil {
			t.Fatal(err)
		}
		mr.HSet(listingOf(level, conf), "x", "f:3:p")
		mr.HSet(listingOf(level, conf), "ghost", "f:99")

		got, err := st.RepairPath(ctx, "/conf/x")
		if err != nil {
			t.Fatal(err)
		}
		want := []store.PathRepair{{Path: "/conf", Result: store.RepairNone}, {Path: "/conf/x", Result: store.RepairSettled}}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("repairs=%v, want %v", got, want)
		}
		got, err = st.RepairPath(ctx, "/conf/ghost")
		if err != nil || len(got) != 2 || got[1].Result != store.RepairRemoved {
			t.Fatalf("ghost repairs=%v err=%v", got, err)
		}
		if _, err := st.RepairPath(ctx, "/conf/never"); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("missing path: %v", err)
		}
	})
}

func TestRepairPendingWaitsForTheSameValue(t *testing.T) {
	forLinkedLevels(t, func(t *testing.T, level store.Level) {
		st, mr := newTestStoreAt(t, level)
		ctx := context.Background()
		root := store.RootRef()
		if _, _, err := st.CreateFile(ctx, root, "f", 0644, 1, 1); err != nil {
			t.Fatal(err)
		}
		listing := listingOf(level, root)
		mr.HSet(listing, "f", "f:2:p")

		if got, err := st.RepairPending(ctx, root, "f", "f:2:p:1/other"); err != nil || got != store.RepairNone {
			t.Fatalf("a changed field was repaired: %q err=%v", got, err)
		}
		if got, err := st.RepairPending(ctx, root, "f", "f:2:p"); err != nil || got != store.RepairSettled {
			t.Fatalf("repair=%q err=%v, want settled", got, err)
		}
	})
}

func TestDirLevelCreateIsExclusiveUnderConcurrency(t *testing.T) {
	st, _ := newTestStoreAt(t, store.LevelDir)
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
	wins := 0
	for err := range errs {
		switch {
		case err == nil:
			wins++
		case errors.Is(err, store.ErrExist):
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if wins != 1 {
		t.Fatalf("wins=%d, want 1", wins)
	}
}
