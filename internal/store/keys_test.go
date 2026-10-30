package store_test

import (
	"strings"
	"testing"

	"kvfs/internal/store"
)

func TestKeyGrammar(t *testing.T) {
	tests := []struct {
		name   string
		ns     string
		level  store.Level
		parent uint64
		base   string
		sfx    store.Suffix
		want   string
	}{
		{
			name: "namespace level entry",
			ns:   "default", level: store.LevelNamespace,
			parent: 1, base: "README.md", sfx: store.SuffixMeta,
			want: "kvfs/{default}/1/README.md/meta",
		},
		{
			name: "namespace level root",
			ns:   "default", level: store.LevelNamespace,
			parent: store.NamespaceID, base: store.RootName, sfx: store.SuffixCounter,
			want: "kvfs/{default}/0/./counter",
		},
		{
			name: "file level entry",
			ns:   "default", level: store.LevelFile,
			parent: 1, base: "README.md", sfx: store.SuffixMeta,
			want: "kvfs/{default/1/README.md}/meta",
		},
		{
			name: "file level root",
			ns:   "default", level: store.LevelFile,
			parent: store.NamespaceID, base: store.RootName, sfx: store.SuffixPubsub,
			want: "kvfs/{default/0/.}/pubsub",
		},
		{
			name: "dir level entry",
			ns:   "default", level: store.LevelDir,
			parent: 1, base: "README.md", sfx: store.SuffixData,
			want: "kvfs/{default/1}/README.md/data",
		},
		{
			name: "dir level root counter",
			ns:   "default", level: store.LevelDir,
			parent: store.NamespaceID, base: store.RootName, sfx: store.SuffixCounter,
			want: "kvfs/{default/0}/./counter",
		},
		{
			name: "dir level escaped name stays outside the tag",
			ns:   "ns", level: store.LevelDir,
			parent: 3, base: "a{b}/c", sfx: store.SuffixDir,
			want: "kvfs/{ns/3}/a%7Bb%7D%2Fc/dir",
		},
		{
			name: "namespace level counter probe",
			ns:   "default", level: store.LevelNamespace,
			parent: store.NamespaceID, base: store.RootName, sfx: store.SuffixPubsub,
			want: "kvfs/{default}/0/./pubsub",
		},
		{
			name: "slash is escaped",
			ns:   "ns", level: store.LevelNamespace,
			parent: 2, base: "a/b", sfx: store.SuffixDir,
			want: "kvfs/{ns}/2/a%2Fb/dir",
		},
		{
			name: "glob and brace bytes are escaped",
			ns:   "ns", level: store.LevelFile,
			parent: 3, base: "a{b}*c?d[e]", sfx: store.SuffixData,
			want: "kvfs/{ns/3/a%7Bb%7D%2Ac%3Fd%5Be%5D}/data",
		},
		{
			name: "quote, percent and backslash are escaped",
			ns:   "ns", level: store.LevelNamespace,
			parent: 4, base: `a"b%c\d`, sfx: store.SuffixMeta,
			want: `kvfs/{ns}/4/a%22b%25c%5Cd/meta`,
		},
		{
			name: "space and plus are literal",
			ns:   "ns", level: store.LevelNamespace,
			parent: 5, base: "a b+c", sfx: store.SuffixMeta,
			want: "kvfs/{ns}/5/a b+c/meta",
		},
		{
			name: "control byte is escaped",
			ns:   "ns", level: store.LevelNamespace,
			parent: 6, base: "a\nb", sfx: store.SuffixMeta,
			want: "kvfs/{ns}/6/a%0Ab/meta",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := store.Key(tc.ns, tc.level, tc.parent, tc.base, tc.sfx)
			if got != tc.want {
				t.Fatalf("Key()=%q, want %q", got, tc.want)
			}
		})
	}
}

func TestEscapeNameRoundTrip(t *testing.T) {
	names := []string{
		"",
		"plain",
		"README.md",
		".env",
		"with space",
		"plus+sign",
		"only/slash",
		"percent%here",
		"quote\"d",
		"glob*[]{}?",
		"back\\slash",
		"tab\there",
		"newline\nhere",
		"nul\x00here",
		"ünïcode-日本語-🚀",
		"..",
		".",
	}
	// Every byte value must survive the round trip unchanged.
	var all strings.Builder
	for i := 0; i < 256; i++ {
		all.WriteByte(byte(i))
	}
	names = append(names, all.String())

	for _, name := range names {
		escaped := store.EscapeName(name)
		if !strings.ContainsAny(name, `%/"*?[]{}`+"\\") && len(name) > 0 {
			// Control bytes are the other escaped class; check them too.
			clean := true
			for i := 0; i < len(name); i++ {
				if name[i] < 0x20 {
					clean = false
					break
				}
			}
			if clean && escaped != name {
				t.Fatalf("EscapeName(%q) rewrote a name it should leave alone: %q", name, escaped)
			}
		}
		got, err := store.UnescapeName(escaped)
		if err != nil {
			t.Fatalf("UnescapeName(%q): %v", escaped, err)
		}
		if got != name {
			t.Fatalf("round trip: %q -> %q -> %q", name, escaped, got)
		}
	}
}

func TestEscapeNameLeavesNoGrammarBytes(t *testing.T) {
	var raw strings.Builder
	for i := 0; i < 256; i++ {
		raw.WriteByte(byte(i))
	}
	escaped := store.EscapeName(raw.String())
	for i := 0; i < len(escaped); i++ {
		c := escaped[i]
		if c < 0x20 || strings.IndexByte("/*?[]{}\"\\", c) >= 0 {
			t.Fatalf("escaped name kept byte %#x at %d: %q", c, i, escaped)
		}
	}
	// Every '%' introduces exactly two hex digits.
	for i := 0; i < len(escaped); i++ {
		if escaped[i] != '%' {
			continue
		}
		if i+2 >= len(escaped) || !isHex(escaped[i+1]) || !isHex(escaped[i+2]) {
			t.Fatalf("truncated escape at %d in %q", i, escaped)
		}
	}
}

func isHex(c byte) bool {
	return c >= '0' && c <= '9' || c >= 'A' && c <= 'F' || c >= 'a' && c <= 'f'
}

func TestUnescapeNameRejectsBadInput(t *testing.T) {
	for _, bad := range []string{"%", "%2", "%zz", "a%2"} {
		if got, err := store.UnescapeName(bad); err == nil {
			t.Fatalf("UnescapeName(%q)=%q, want an error", bad, got)
		}
	}
}

func TestInvalidatePayloadRoundTrip(t *testing.T) {
	for _, name := range []string{".env", "a/b", `q"q`, "ünïcode", "with space", "%2F"} {
		payload := store.InvalidatePayload(7, name)
		if strings.Count(payload, "/") != 1 {
			t.Fatalf("payload %q does not have exactly one separator", payload)
		}
		pid, got, err := store.ParseInvalidate(payload)
		if err != nil {
			t.Fatalf("ParseInvalidate(%q): %v", payload, err)
		}
		if pid != 7 || got != name {
			t.Fatalf("round trip: %q -> %q", name, got)
		}
	}
}

func TestChildValueRoundTrip(t *testing.T) {
	tests := []struct {
		child store.Child
		value string
	}{
		{store.Child{Type: store.TypeFile, ID: 2}, "f:2"},
		{store.Child{Type: store.TypeDir, ID: 3}, "d:3"},
		{store.Child{Type: store.TypeLink, ID: 4}, "l:4"},
	}
	for _, tc := range tests {
		if got := tc.child.Value(); got != tc.value {
			t.Fatalf("Value()=%q, want %q", got, tc.value)
		}
		got := store.ParseChild("n", tc.value)
		if got.Name != "n" || got.Type != tc.child.Type || got.ID != tc.child.ID {
			t.Fatalf("ParseChild(%q)=%+v, want %+v", tc.value, got, tc.child)
		}
	}
}

func TestParseChildToleratesUnknownAndMarkers(t *testing.T) {
	if c := store.ParseChild("n", "x:9"); c.Type != "" || c.ID != 9 {
		t.Fatalf("unknown code: %+v", c)
	}
	// A pending field keeps the inode but never claims a type: the kernel must
	// stat the entry instead of trusting a half-written record.
	if c := store.ParseChild("n", "f:9:p"); c.Type != "" || c.ID != 9 {
		t.Fatalf("pending field: %+v", c)
	}
	if got := (store.Child{Type: store.TypeDir, ID: 3}).PendingValue(); got != "d:3:p" {
		t.Fatalf("PendingValue()=%q", got)
	}
	if c := store.ParseChild("n", ""); c.Name != "n" || c.Type != "" || c.ID != 0 {
		t.Fatalf("empty value: %+v", c)
	}
	if c := store.ParseChild("n", "f:notanumber"); c.Type != store.TypeFile || c.ID != 0 {
		t.Fatalf("bad id: %+v", c)
	}
}

func TestValidNamespace(t *testing.T) {
	tests := []struct {
		ns   string
		want bool
	}{
		{"default", true},
		{"a", true},
		{"A-9", true},
		{"", false},
		{"a/b", false},
		{"a.b", false},
		{"a b", false},
		{"näme", false},
		{strings.Repeat("x", store.MaxNamespaceLen), true},
		{strings.Repeat("x", store.MaxNamespaceLen+1), false},
	}
	for _, tc := range tests {
		if got := store.ValidNamespace(tc.ns); got != tc.want {
			t.Fatalf("ValidNamespace(%q)=%v, want %v", tc.ns, got, tc.want)
		}
	}
}
