package store_test

import (
	"context"
	"sort"
	"strings"
	"testing"

	"kvfs/internal/store"
)

// TestWorkedExample replays SPEC.md's worked example and pins the state it
// documents: the same five steps must leave exactly these keys, with these
// values. If the schema changes, this test and the spec must change together.
func TestWorkedExample(t *testing.T) {
	st, mr := newTestStore(t)
	ctx := context.Background()

	if _, _, err := st.CreateFile(ctx, store.RootRef(), "README.md", 0644, 1000, 1000); err != nil {
		t.Fatal(err)
	}
	if _, err := st.WriteData(ctx, store.RootID, "README.md", []byte("Hello, world!"), true); err != nil {
		t.Fatal(err)
	}
	_, sub1, err := st.Mkdir(ctx, store.RootRef(), "sub1", 0755, 1000, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.CreateFile(ctx, sub1, "README.md", 0644, 1000, 1000); err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.CreateFile(ctx, sub1, "sub2", 0644, 1000, 1000); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Rename(ctx, store.RootRef(), "sub1", store.RootRef(), "mydir"); err != nil {
		t.Fatal(err)
	}

	wantKeys := []string{
		"kvfs/{test}/0/./counter",
		"kvfs/{test}/0/./dir",
		"kvfs/{test}/0/./meta",
		"kvfs/{test}/1/README.md/data",
		"kvfs/{test}/1/README.md/meta",
		"kvfs/{test}/1/mydir/dir",
		"kvfs/{test}/1/mydir/meta",
		"kvfs/{test}/3/README.md/data",
		"kvfs/{test}/3/README.md/meta",
		"kvfs/{test}/3/sub2/data",
		"kvfs/{test}/3/sub2/meta",
	}
	gotKeys := mr.Keys()
	sort.Strings(gotKeys)
	sort.Strings(wantKeys)
	if strings.Join(gotKeys, "\n") != strings.Join(wantKeys, "\n") {
		t.Fatalf("keys after the session:\n%s\nwant:\n%s", strings.Join(gotKeys, "\n"), strings.Join(wantKeys, "\n"))
	}

	if got, _ := mr.Get("kvfs/{test}/0/./counter"); got != "5" {
		t.Fatalf("counter=%q, want 5 (1 root + 4 creations)", got)
	}
	hashes := []struct {
		key    string
		fields map[string]string
	}{
		{"kvfs/{test}/0/./dir", map[string]string{"README.md": "f:2", "mydir": "d:3"}},
		{"kvfs/{test}/1/mydir/dir", map[string]string{"README.md": "f:4", "sub2": "f:5"}},
	}
	for _, h := range hashes {
		for name, want := range h.fields {
			if got := mr.HGet(h.key, name); got != want {
				t.Fatalf("%s[%s]=%q, want %q", h.key, name, got, want)
			}
		}
	}
	if got, _ := mr.Get("kvfs/{test}/1/README.md/data"); got != "Hello, world!" {
		t.Fatalf("content=%q", got)
	}
	for _, empty := range []string{"kvfs/{test}/3/README.md/data", "kvfs/{test}/3/sub2/data"} {
		if got, _ := mr.Get(empty); got != "" {
			t.Fatalf("%s=%q, want the empty string", empty, got)
		}
	}
	moved, err := st.GetMeta(ctx, store.RootID, "mydir")
	if err != nil {
		t.Fatal(err)
	}
	if moved.ID != 3 {
		t.Fatalf("renamed directory inode=%d, want 3 (unchanged)", moved.ID)
	}
}
