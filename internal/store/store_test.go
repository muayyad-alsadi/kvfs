package store_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"kvfs/internal/store"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/rueidis"
)

func newTestStore(t *testing.T) (*store.Store, *miniredis.Miniredis) {
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
	st := store.New(client, "test")
	ctx := context.Background()
	if err := st.BootstrapRoot(ctx, 1000, 1000); err != nil {
		t.Fatal(err)
	}
	return st, mr
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
	if m.ID < 2 || ref.ParentID != 1 || ref.Name != ".env" {
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
	if len(children) != 1 || children[0] != ".env" {
		t.Fatalf("children=%v", children)
	}
}

func TestMkdirSymlinkRenameUnlink(t *testing.T) {
	st, _ := newTestStore(t)
	ctx := context.Background()

	_, confRef, err := st.Mkdir(ctx, store.RootRef(), "conf", 0755, 1000, 1000)
	if err != nil {
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

	_, fileRef, err := st.CreateFile(ctx, confRef, "app.toml", 0644, 1000, 1000)
	if err != nil {
		t.Fatal(err)
	}
	idBefore := fileRef.ID
	if err := st.Rename(ctx, confRef, "app.toml", store.RootRef(), "app.toml"); err != nil {
		t.Fatal(err)
	}
	moved, err := st.GetMeta(ctx, store.RootID, "app.toml")
	if err != nil {
		t.Fatal(err)
	}
	if moved.ID != idBefore {
		t.Fatalf("inode changed on rename: %d -> %d", idBefore, moved.ID)
	}
	if _, err := st.GetMeta(ctx, confRef.ID, "app.toml"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("old key still present: %v", err)
	}

	if err := st.Unlink(ctx, store.RootRef(), "link"); err != nil {
		t.Fatal(err)
	}
	if err := st.Rmdir(ctx, store.RootRef(), "conf"); err != nil {
		t.Fatal(err)
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
}

func TestParseInvalidate(t *testing.T) {
	pid, name, err := store.ParseInvalidate("1:.env")
	if err != nil || pid != 1 || name != ".env" {
		t.Fatalf("got %d %q %v", pid, name, err)
	}
	pid, name, err = store.ParseInvalidate("0:")
	if err != nil || pid != 0 || name != "" {
		t.Fatalf("root payload: %d %q %v", pid, name, err)
	}
}
