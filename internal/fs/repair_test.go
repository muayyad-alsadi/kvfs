package kvfs

import (
	"context"
	"fmt"
	"runtime"
	"syscall"
	"testing"
	"time"

	"kvfs/internal/store"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/rueidis"
)

func fileRoot(t *testing.T) (*Root, *store.Store, *miniredis.Miniredis) {
	t.Helper()
	return rootAt(t, store.LevelFile)
}

func rootAt(t *testing.T, level store.Level) (*Root, *store.Store, *miniredis.Miniredis) {
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
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	st, err := store.New(client, "test", level)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.BootstrapRoot(context.Background(), 1, 1); err != nil {
		t.Fatal(err)
	}
	return NewRoot(st, nil, 1, 1), st, mr
}

func waitFor(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		runtime.Gosched()
	}
}

func TestPendingFieldIsRepairedAfterTheGracePeriod(t *testing.T) {
	r, st, mr := fileRoot(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	root := store.RootRef()
	if _, _, err := st.CreateFile(ctx, root, "f", 0644, 1, 1); err != nil {
		t.Fatal(err)
	}
	listing := store.Key("test", store.LevelFile, 0, ".", store.SuffixDir)
	mr.HSet(listing, "f", "f:2:p")

	r.grace = 40 * time.Millisecond
	r.goLoop(func() { r.repairLoop(ctx) })

	children, err := st.ListChildren(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	r.noteListing(root, children)
	waitFor(t, "the pending field to settle", func() bool { return mr.HGet(listing, "f") == "f:2" })
	waitFor(t, "the repair to be counted", func() bool { return r.Health().Repaired == 1 })

	h := r.Health()
	if h.Pending != 1 || h.Unknown != 1 {
		t.Fatalf("health=%+v, want one pending and one unknown observation", h)
	}
	cancel()
	r.Close()
}

func TestAPendingFieldThatChangesIsLeftAlone(t *testing.T) {
	r, st, mr := fileRoot(t)
	ctx := context.Background()
	root := store.RootRef()
	if _, _, err := st.CreateFile(ctx, root, "f", 0644, 1, 1); err != nil {
		t.Fatal(err)
	}
	listing := store.Key("test", store.LevelFile, 0, ".", store.SuffixDir)
	mr.HSet(listing, "f", "f:2:p")

	// The repair was queued against a value the field no longer holds.
	r.repairOne(ctx, repairReq{parent: root, name: "f", raw: "f:2:p:1/other"})
	if got := mr.HGet(listing, "f"); got != "f:2:p" {
		t.Fatalf("a field that changed was repaired anyway: %q", got)
	}
	if r.Health().Repaired != 0 {
		t.Fatalf("health=%+v", r.Health())
	}
}

func TestLookupMissRemovesAGhostButWaitsOnAPendingName(t *testing.T) {
	r, _, mr := fileRoot(t)
	ctx := context.Background()
	root := store.RootRef()
	listing := store.Key("test", store.LevelFile, 0, ".", store.SuffixDir)

	mr.HSet(listing, "ghost", "f:99")
	r.repairMiss(ctx, root, "ghost")
	if mr.HGet(listing, "ghost") != "" {
		t.Fatal("a settled field with no metadata survived the miss")
	}
	if got := r.Health().Dangling; got != 1 {
		t.Fatalf("dangling=%d, want 1", got)
	}

	// A pending name may be a create between its claim and its prepare.
	mr.HSet(listing, "claiming", "f:98:p")
	r.repairMiss(ctx, root, "claiming")
	if mr.HGet(listing, "claiming") != "f:98:p" {
		t.Fatal("a lookup miss removed another client's claim")
	}
	if len(r.repairCh) != 1 {
		t.Fatalf("the pending name was not queued for the grace-period repair")
	}
}

func TestNamespaceLookupMissRepairsNothing(t *testing.T) {
	r, _, mr := rootAt(t, store.LevelNamespace)
	listing := store.Key("test", store.LevelNamespace, 0, ".", store.SuffixDir)

	mr.HSet(listing, "ghost", "f:99")
	r.repairMiss(context.Background(), store.RootRef(), "ghost")
	if got := mr.HGet(listing, "ghost"); got != "f:99" {
		t.Fatalf("a namespace-level miss rewrote the listing: %q", got)
	}
	if h := r.Health(); h.Dangling != 0 || h.Repaired != 0 {
		t.Fatalf("health=%+v, want no repair at namespace level", h)
	}
}

func TestErrnoMapsBusy(t *testing.T) {
	if got := errno(fmt.Errorf("x: %w", store.ErrBusy)); got != syscall.EBUSY {
		t.Fatalf("errno=%v, want EBUSY", got)
	}
}
