package store

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

// Level selects where the hash tag goes. It changes the tag, never the grammar.
type Level string

const (
	// LevelNamespace tags the namespace: every key of the filesystem shares a slot.
	LevelNamespace Level = "namespace"
	// LevelDir tags the namespace and the parent id: the entries of one
	// directory share a slot.
	LevelDir Level = "dir"
	// LevelFile tags the namespace, the parent id and the basename: every
	// entry has its own slot.
	LevelFile Level = "file"
)

// Levels lists every sharding level this schema defines, in grammar order.
func Levels() []Level {
	return []Level{LevelNamespace, LevelDir, LevelFile}
}

// ParseLevel validates a -shard-level value. Anything outside the three levels
// of the schema fails here, before any key is read.
func ParseLevel(s string) (Level, error) {
	for _, l := range Levels() {
		if s == string(l) {
			return l, nil
		}
	}
	return "", fmt.Errorf("unknown sharding level %q: want namespace, dir or file", s)
}

// Suffix identifies one record of an entry.
type Suffix string

const (
	SuffixMeta    Suffix = "meta"
	SuffixData    Suffix = "data"
	SuffixDir     Suffix = "dir"
	SuffixCounter Suffix = "counter"
	SuffixPubsub  Suffix = "pubsub"
)

const (
	// Magic is the key prefix. A future breaking change takes a new one
	// (kvfs_v2/); the layout under kvfs/ is never reinterpreted in place.
	Magic = "kvfs"
	// NamespaceID is the pseudo-inode that owns the root. It is never a FUSE
	// inode and is never handed out by the counter.
	NamespaceID uint64 = 0
	// RootName is the root's basename under the namespace node.
	RootName = "."
	// MaxNamespaceLen bounds a namespace so a key cannot grow without limit.
	MaxNamespaceLen = 64
)

// ValidNamespace reports whether ns may be used as a namespace.
func ValidNamespace(ns string) bool {
	if ns == "" || len(ns) > MaxNamespaceLen {
		return false
	}
	for i := 0; i < len(ns); i++ {
		c := ns[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' {
			continue
		}
		return false
	}
	return true
}

// Key builds the key for one record of the entry (parent, name).
// One infix rule covers every suffix: all records of an entry share
// <parent id>/<basename>, and no key needs the entry's own inode.
//
// The infix is <namespace>/<parentId>/<basename> at every level; the level only
// decides how much of it the braces wrap.
func Key(ns string, level Level, parent uint64, name string, sfx Suffix) string {
	base := EscapeName(name)
	switch level {
	case LevelFile:
		return fmt.Sprintf("%s/{%s/%d/%s}/%s", Magic, ns, parent, base, sfx)
	case LevelDir:
		return fmt.Sprintf("%s/{%s/%d}/%s/%s", Magic, ns, parent, base, sfx)
	default:
		return fmt.Sprintf("%s/{%s}/%d/%s/%s", Magic, ns, parent, base, sfx)
	}
}

func (s *Store) key(parent uint64, name string, sfx Suffix) string {
	return Key(s.ns, s.level, parent, name, sfx)
}

func (s *Store) metaKey(parent uint64, name string) string {
	return s.key(parent, name, SuffixMeta)
}

func (s *Store) dirKey(parent uint64, name string) string {
	return s.key(parent, name, SuffixDir)
}

func (s *Store) dataKey(parent uint64, name string) string {
	return s.key(parent, name, SuffixData)
}

// counterKey is the namespace-wide inode allocator.
func (s *Store) counterKey() string {
	return s.key(NamespaceID, RootName, SuffixCounter)
}

// Channel is the namespace-wide invalidation channel.
func (s *Store) Channel() string {
	return s.key(NamespaceID, RootName, SuffixPubsub)
}

// InvalidatePayload addresses one entry in the same encoding as its keys, so a
// subscriber never has to re-derive it.
func InvalidatePayload(parentID uint64, name string) string {
	return fmt.Sprintf("%d/%s", parentID, EscapeName(name))
}

// ParseInvalidate decodes InvalidatePayload.
func ParseInvalidate(payload string) (uint64, string, error) {
	i := strings.IndexByte(payload, '/')
	if i < 0 {
		return 0, "", fmt.Errorf("bad invalidate payload %q", payload)
	}
	id, err := strconv.ParseUint(payload[:i], 10, 64)
	if err != nil {
		return 0, "", fmt.Errorf("bad invalidate payload %q: %w", payload, err)
	}
	name, err := UnescapeName(payload[i+1:])
	if err != nil {
		return 0, "", fmt.Errorf("bad invalidate payload %q: %w", payload, err)
	}
	return id, name, nil
}

// EscapeName percent-encodes the bytes that would break the key grammar or a
// Redis glob. Everything else is stored verbatim, byte for byte.
func EscapeName(name string) string {
	needs := false
	for i := 0; i < len(name); i++ {
		if escapes(name[i]) {
			needs = true
			break
		}
	}
	if !needs {
		return name
	}
	var b strings.Builder
	b.Grow(len(name) + 8)
	for i := 0; i < len(name); i++ {
		c := name[i]
		if !escapes(c) {
			b.WriteByte(c)
			continue
		}
		const hex = "0123456789ABCDEF"
		b.WriteByte('%')
		b.WriteByte(hex[c>>4])
		b.WriteByte(hex[c&0x0F])
	}
	return b.String()
}

func escapes(c byte) bool {
	switch c {
	case '%', '/', '"', '*', '?', '[', ']', '{', '}', '\\':
		return true
	}
	return c < 0x20
}

// UnescapeName decodes EscapeName. It is url.PathUnescape, which leaves "+"
// alone: only %XX sequences are escapes.
func UnescapeName(s string) (string, error) {
	if !strings.Contains(s, "%") {
		return s, nil
	}
	name, err := url.PathUnescape(s)
	if err != nil {
		return "", err
	}
	return name, nil
}
