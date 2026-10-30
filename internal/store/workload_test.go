package store_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"kvfs/internal/store"

	"github.com/redis/rueidis"
)

// runWorkload drives every mutation and read the mount issues through the
// store's public surface. It is shared by the slot-safety test on miniredis and
// by the live cluster test, so both exercise the same commands.
func runWorkload(t testing.TB, st *store.Store) {
	t.Helper()
	ctx := context.Background()
	root := store.RootRef()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}

	_, conf, err := st.Mkdir(ctx, root, "conf", 0755, 1, 1)
	must(err)
	_, _, err = st.CreateFile(ctx, conf, "app.toml", 0644, 1, 1)
	must(err)
	_, err = st.WriteData(ctx, conf.ID, "app.toml", []byte("x=1"), true)
	must(err)
	must(st.WriteAt(ctx, conf.ID, "app.toml", 3, []byte("\ny=2")))
	_, err = st.ReadAt(ctx, conf.ID, "app.toml", 0, 16)
	must(err)
	_, err = st.Stat(ctx, conf.ID, "app.toml")
	must(err)
	must(st.MarkModified(ctx, conf.ID, "app.toml"))
	_, _, err = st.Symlink(ctx, root, "link", "conf/app.toml", 1, 1)
	must(err)
	_, err = st.UpdateMeta(ctx, conf.ID, "app.toml", func(m *store.Meta) { m.Mode = 0600 })
	must(err)
	_, err = st.Truncate(ctx, conf.ID, "app.toml", 2)
	must(err)
	_, err = st.ListChildren(ctx, conf)
	must(err)
	_, err = st.Rename(ctx, conf, "app.toml", conf, "app.cfg")
	must(err)
	_, err = st.Rename(ctx, conf, "app.cfg", root, "app.cfg")
	must(err)
	_, err = st.Rename(ctx, root, "conf", root, "settings")
	must(err)
	settings := store.Ref{ParentID: store.RootID, Name: "settings", ID: conf.ID}
	_, _, err = st.CreateFile(ctx, settings, "x", 0644, 1, 1)
	must(err)
	_, err = st.Rename(ctx, settings, "x", root, "x")
	must(err)
	must(st.Unlink(ctx, root, "x"))
	must(st.Unlink(ctx, root, "app.cfg"))
	must(st.Unlink(ctx, root, "link"))
	must(st.Rmdir(ctx, root, "settings"))
	_, err = st.RepairEntry(ctx, root, "nothing")
	must(err)
	_, err = st.RepairPath(ctx, "/")
	must(err)
	must(st.Publish(ctx, store.RootID, "settings"))
}

// assertSlotSafe checks the cluster contract: every script names keys from one
// slot, and every transaction touches one slot. A transaction opens at WATCH,
// so the watched read and the batch that commits it must agree, or at a MULTI
// no WATCH opened. miniredis is single-node and rueidis disables its own slot
// check for a forced single client, so the unit suite has to check the command
// stream itself.
func assertSlotSafe(t *testing.T, cmds []sent) {
	t.Helper()
	txTag := ""
	watching := false
	for _, c := range cmds {
		switch strings.ToUpper(c.args[0]) {
		case "WATCH":
			txTag, watching = "", true
		case "MULTI":
			if !watching {
				txTag = ""
			}
			watching = false
		case "UNWATCH":
			watching = false
		}
		tag := ""
		for _, k := range keysOf(c.args) {
			kt := hashTag(k)
			switch {
			case kt == "":
				t.Fatalf("key %q has no hash tag: %v", k, c.args[0])
			case tag != "" && kt != tag:
				t.Fatalf("%s spans slots %q and %q", c.args[0], tag, kt)
			}
			tag = kt
		}
		if !c.tx || tag == "" {
			continue
		}
		if txTag != "" && tag != txTag {
			t.Fatalf("a transaction spans slots %q and %q", txTag, tag)
		}
		txTag = tag
	}
}

func TestEveryCommandStaysInOneSlot(t *testing.T) {
	for _, level := range store.Levels() {
		t.Run(string(level), func(t *testing.T) {
			st, rec := newRecordingStore(t, level)
			runWorkload(t, st)
			assertSlotSafe(t, rec.take())
		})
	}
}

// TestLiveCluster runs the workload against a real cluster at every level. Set
// KVFS_TEST_CLUSTER to a comma-separated list of node addresses to enable it.
func TestLiveCluster(t *testing.T) {
	addrs := os.Getenv("KVFS_TEST_CLUSTER")
	if addrs == "" {
		t.Skip("KVFS_TEST_CLUSTER is not set")
	}
	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress:  strings.Split(addrs, ","),
		DisableCache: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if n := len(client.Nodes()); n < 2 {
		t.Fatalf("KVFS_TEST_CLUSTER reached %d node(s): not a cluster", n)
	}

	run := time.Now().UnixNano()
	for _, level := range store.Levels() {
		t.Run(string(level), func(t *testing.T) {
			ns := "live-" + string(level) + "-" + strings.ReplaceAll(time.Unix(0, run).Format("150405.000000"), ".", "")
			st, err := store.New(client, ns, level)
			if err != nil {
				t.Fatal(err)
			}
			if err := st.BootstrapRoot(context.Background(), 1000, 1000); err != nil {
				t.Fatal(err)
			}
			runWorkload(t, st)
			other := store.LevelFile
			if level == store.LevelFile {
				other = store.LevelDir
			}
			wrong, err := store.New(client, ns, other)
			if err != nil {
				t.Fatal(err)
			}
			if err := wrong.BootstrapRoot(context.Background(), 1000, 1000); err == nil {
				t.Fatalf("mounting a %s namespace at %s was not refused", level, other)
			}
		})
	}
}
