package store_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"kvfs/internal/store"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/rueidis"
)

// doer is what a client and a dedicated connection have in common.
type doer interface {
	Do(ctx context.Context, cmd rueidis.Completed) rueidis.RedisResult
	DoMulti(ctx context.Context, cmds ...rueidis.Completed) []rueidis.RedisResult
}

// crashClient lets a fixed number of commands reach the server and fails every
// one after that, as a process that died between two commands would. A
// MULTI/EXEC batch counts as one command: the server applies it whole or not at
// all. A failed command is sent on an already cancelled context, so it never
// reaches the server.
type crashClient struct {
	rueidis.Client
	mu      sync.Mutex
	budget  int // commands still allowed; negative means unlimited
	tripped bool
}

func (c *crashClient) arm(budget int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.budget, c.tripped = budget, false
}

func (c *crashClient) didTrip() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.tripped
}

// take reserves up to want commands and returns how many may run.
func (c *crashClient) take(want int) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.budget < 0 {
		return want
	}
	n := min(want, c.budget)
	c.budget -= n
	c.tripped = c.tripped || n < want
	return n
}

func deadContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func (c *crashClient) Do(ctx context.Context, cmd rueidis.Completed) rueidis.RedisResult {
	return limitedDo(c, c.Client, ctx, cmd)
}

func (c *crashClient) DoMulti(ctx context.Context, cmds ...rueidis.Completed) []rueidis.RedisResult {
	return limitedDoMulti(c, c.Client, ctx, cmds)
}

func (c *crashClient) Dedicated(fn func(rueidis.DedicatedClient) error) error {
	return c.Client.Dedicated(func(d rueidis.DedicatedClient) error {
		return fn(&crashDedicated{DedicatedClient: d, owner: c})
	})
}

type crashDedicated struct {
	rueidis.DedicatedClient
	owner *crashClient
}

func (d *crashDedicated) Do(ctx context.Context, cmd rueidis.Completed) rueidis.RedisResult {
	return limitedDo(d.owner, d.DedicatedClient, ctx, cmd)
}

func (d *crashDedicated) DoMulti(ctx context.Context, cmds ...rueidis.Completed) []rueidis.RedisResult {
	return limitedDoMulti(d.owner, d.DedicatedClient, ctx, cmds)
}

func limitedDo(c *crashClient, d doer, ctx context.Context, cmd rueidis.Completed) rueidis.RedisResult {
	if c.take(1) == 0 {
		return d.Do(deadContext(), cmd)
	}
	return d.Do(ctx, cmd)
}

func limitedDoMulti(c *crashClient, d doer, ctx context.Context, cmds []rueidis.Completed) []rueidis.RedisResult {
	if len(cmds) > 0 && strings.EqualFold(cmds[0].Commands()[0], "MULTI") {
		if c.take(1) == 0 {
			return d.DoMulti(deadContext(), cmds...)
		}
		return d.DoMulti(ctx, cmds...)
	}
	n := c.take(len(cmds))
	out := make([]rueidis.RedisResult, 0, len(cmds))
	if n > 0 {
		out = append(out, d.DoMulti(ctx, cmds[:n]...)...)
	}
	for _, cmd := range cmds[n:] {
		out = append(out, d.Do(deadContext(), cmd))
	}
	return out
}

// clientAt opens another client on a running in-memory server: a second process.
func clientAt(t testing.TB, mr *miniredis.Miniredis) rueidis.Client {
	t.Helper()
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
	return client
}

// crashTree is the namespace every scenario starts from.
type crashTree struct {
	root, d, e, sub store.Ref
}

// buildCrashTree creates: a (file), d (directory holding the file c), e and sub
// (empty directories), and v (file).
func buildCrashTree(t testing.TB, st *store.Store) crashTree {
	t.Helper()
	ctx := context.Background()
	root := store.RootRef()
	tree := crashTree{root: root}
	file := func(parent store.Ref, name, body string) {
		if _, _, err := st.CreateFile(ctx, parent, name, 0644, 1, 1); err != nil {
			t.Fatal(err)
		}
		if _, err := st.WriteData(ctx, parent.ID, name, []byte(body), true); err != nil {
			t.Fatal(err)
		}
	}
	dir := func(name string) store.Ref {
		_, ref, err := st.Mkdir(ctx, root, name, 0755, 1, 1)
		if err != nil {
			t.Fatal(err)
		}
		return ref
	}
	file(root, "a", "AAA")
	tree.d = dir("d")
	file(tree.d, "c", "CCC")
	tree.e = dir("e")
	tree.sub = dir("sub")
	file(root, "v", "VVV")
	return tree
}

// crashSnapshot lists every reachable path with its inode and content, and
// fails the test if a listed name is still unresolved. It returns the entries'
// own addresses too, so the caller can look for keys nothing owns.
type crashSnapshot struct {
	lines   []string
	entries [][2]any // parent id, name
}

func snapshotTree(t testing.TB, st *store.Store) crashSnapshot {
	t.Helper()
	ctx := context.Background()
	snap := crashSnapshot{entries: [][2]any{{store.RootParentID, store.RootName}}}
	var walk func(dir store.Ref, path string)
	walk = func(dir store.Ref, path string) {
		kids, err := st.ListChildren(ctx, dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, k := range kids {
			if k.Pending || k.Type == "" {
				t.Fatalf("%s/%s is still unresolved after repair: %q", path, k.Name, k.Raw)
			}
			snap.entries = append(snap.entries, [2]any{dir.ID, k.Name})
			full := path + "/" + k.Name
			if k.Type == store.TypeDir {
				snap.lines = append(snap.lines, fmt.Sprintf("%s dir#%d", full, k.ID))
				walk(store.Ref{ParentID: dir.ID, Name: k.Name, ID: k.ID}, full)
				continue
			}
			m, err := st.GetMeta(ctx, dir.ID, k.Name)
			if err != nil || m.ID != k.ID {
				t.Fatalf("%s: listed as inode %d but metadata says %+v err=%v", full, k.ID, m, err)
			}
			data, err := st.GetData(ctx, dir.ID, k.Name)
			if err != nil {
				t.Fatal(err)
			}
			snap.lines = append(snap.lines, fmt.Sprintf("%s %s#%d=%q", full, k.Type, k.ID, data))
		}
	}
	walk(store.RootRef(), "")
	slices.Sort(snap.lines)
	return snap
}

// repairTree is what a restarted mount does: resolve every listed name.
func repairTree(t testing.TB, st *store.Store) {
	t.Helper()
	ctx := context.Background()
	queue := []store.Ref{store.RootRef()}
	for len(queue) > 0 {
		dir := queue[0]
		queue = queue[1:]
		kids, err := st.ListChildren(ctx, dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, k := range kids {
			if _, err := st.RepairEntry(ctx, dir, k.Name); err != nil {
				t.Fatalf("repair %d/%s: %v", dir.ID, k.Name, err)
			}
		}
		kids, err = st.ListChildren(ctx, dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, k := range kids {
			if k.Type == store.TypeDir {
				queue = append(queue, store.Ref{ParentID: dir.ID, Name: k.Name, ID: k.ID})
			}
		}
	}
}

// assertNoStrayKeys fails when the server holds a key that no reachable entry
// and no namespace record accounts for.
func assertNoStrayKeys(t testing.TB, mr *miniredis.Miniredis, level store.Level, snap crashSnapshot) {
	t.Helper()
	owned := map[string]bool{
		store.Key("test", level, store.NamespaceID, store.RootName, store.SuffixCounter): true,
	}
	for _, e := range snap.entries {
		for _, sfx := range []store.Suffix{store.SuffixMeta, store.SuffixData, store.SuffixDir} {
			owned[store.Key("test", level, e[0].(uint64), e[1].(string), sfx)] = true
		}
	}
	for _, k := range mr.Keys() {
		if !owned[k] {
			t.Fatalf("stray key after repair: %q", k)
		}
	}
}

type crashScenario struct {
	name string
	op   func(ctx context.Context, st *store.Store, tree crashTree) error
	// alsoWithout names a path whose absence is an acceptable outcome next to
	// "before" and "after": a rename that replaces an entry destroys it on the
	// way, and repair discards the half-made destination.
	alsoWithout string
}

func crashScenarios() []crashScenario {
	rename := func(from string, to string) func(context.Context, *store.Store, crashTree) error {
		return func(ctx context.Context, st *store.Store, tree crashTree) error {
			_, err := st.Rename(ctx, tree.root, from, tree.root, to)
			return err
		}
	}
	return []crashScenario{
		{name: "create", op: func(ctx context.Context, st *store.Store, tree crashTree) error {
			_, _, err := st.CreateFile(ctx, tree.root, "new", 0644, 1, 1)
			return err
		}},
		{name: "mkdir", op: func(ctx context.Context, st *store.Store, tree crashTree) error {
			_, _, err := st.Mkdir(ctx, tree.root, "newdir", 0755, 1, 1)
			return err
		}},
		{name: "unlink", op: func(ctx context.Context, st *store.Store, tree crashTree) error {
			return st.Unlink(ctx, tree.root, "a")
		}},
		{name: "rmdir", op: func(ctx context.Context, st *store.Store, tree crashTree) error {
			return st.Rmdir(ctx, tree.root, "e")
		}},
		{name: "rename file, same parent", op: rename("a", "a2")},
		{name: "rename directory, same parent", op: rename("d", "d2")},
		{name: "rename file, across parents", op: func(ctx context.Context, st *store.Store, tree crashTree) error {
			_, err := st.Rename(ctx, tree.root, "a", tree.sub, "a")
			return err
		}},
		{name: "rename directory, across parents", op: func(ctx context.Context, st *store.Store, tree crashTree) error {
			_, err := st.Rename(ctx, tree.root, "d", tree.sub, "d")
			return err
		}},
		{name: "rename replaces a file", op: rename("a", "v"), alsoWithout: "/v"},
		{name: "rename replaces an empty directory", op: rename("d", "e"), alsoWithout: "/e"},
	}
}

// TestCrashAtEveryStep stops every operation after each command it sends, as
// if the process died there, then lets a second process repair the namespace.
// The result must be a state a client could have seen without the crash:
// nothing, or the whole operation, with no unresolved name and no stray key.
func TestCrashAtEveryStep(t *testing.T) {
	for _, level := range store.Levels() {
		for _, sc := range crashScenarios() {
			t.Run(string(level)+"/"+sc.name, func(t *testing.T) {
				runCrashScenario(t, level, sc)
			})
		}
	}
}

func runCrashScenario(t *testing.T, level store.Level, sc crashScenario) {
	before, after := crashReference(t, level, sc)
	allowed := map[string]string{"before": strings.Join(before, "\n"), "after": strings.Join(after, "\n")}
	if sc.alsoWithout != "" {
		allowed["without "+sc.alsoWithout] = strings.Join(without(before, sc.alsoWithout), "\n")
	}

	const maxSteps = 40
	for k := 0; k < maxSteps; k++ {
		mr, crash, victim, tree := crashFixture(t, level)
		crash.arm(k)
		err := sc.op(context.Background(), victim, tree)
		completed := !crash.didTrip()
		if completed && err != nil {
			t.Fatalf("step budget %d: the operation failed without a crash: %v", k, err)
		}

		restarted := bootstrappedAt(t, clientAt(t, mr), level)
		repairTree(t, restarted)
		snap := snapshotTree(t, restarted)
		assertNoStrayKeys(t, mr, level, snap)
		got := strings.Join(snap.lines, "\n")

		if completed {
			t.Logf("%d crash points checked", k)
			if got != allowed["after"] {
				t.Fatalf("finished operation left the wrong state:\n%s\nwant:\n%s", got, allowed["after"])
			}
			return
		}
		if !matchesAny(got, allowed) {
			t.Fatalf("crash after %d commands left a state that is neither before nor after:\n%s\nbefore:\n%s\nafter:\n%s",
				k, got, allowed["before"], allowed["after"])
		}
	}
	t.Fatalf("the operation did not finish within %d commands", maxSteps)
}

func matchesAny(got string, allowed map[string]string) bool {
	for _, want := range allowed {
		if got == want {
			return true
		}
	}
	return false
}

// without drops one path (and anything under it) from a snapshot.
func without(lines []string, path string) []string {
	var out []string
	for _, l := range lines {
		name, _, _ := strings.Cut(l, " ")
		if name == path || strings.HasPrefix(name, path+"/") {
			continue
		}
		out = append(out, l)
	}
	return out
}

// crashFixture builds the tree through a normal store, then returns a second
// store, on its own connection, whose commands the test can cut off.
func crashFixture(t *testing.T, level store.Level) (*miniredis.Miniredis, *crashClient, *store.Store, crashTree) {
	t.Helper()
	mr, client := testClient(t)
	tree := buildCrashTree(t, bootstrappedAt(t, client, level))

	crash := &crashClient{Client: clientAt(t, mr), budget: -1}
	st, err := store.New(crash, "test", level)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.BootstrapRoot(context.Background(), 1000, 1000); err != nil {
		t.Fatal(err)
	}
	return mr, crash, st, tree
}

// crashReference returns the snapshots before the operation and after a clean run.
func crashReference(t *testing.T, level store.Level, sc crashScenario) (before, after []string) {
	t.Helper()
	mr, crash, st, tree := crashFixture(t, level)
	_ = crash
	reader := bootstrappedAt(t, clientAt(t, mr), level)
	before = snapshotTree(t, reader).lines
	if err := sc.op(context.Background(), st, tree); err != nil {
		t.Fatalf("clean run of %q failed: %v", sc.name, err)
	}
	return before, snapshotTree(t, reader).lines
}
