package store

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	TypeFile = "file"
	TypeDir  = "dir"
	TypeLink = "link"

	RootParentID uint64 = 0
	RootID       uint64 = 1
)

// Type codes stored in a directory hash. They are one byte because the value is
// a cache of immutable facts (type and inode), never a second authority.
const (
	CodeFile    = 'f'
	CodeDir     = 'd'
	CodeLink    = 'l'
	CodeUnknown = 0
)

// Child is one field of a directory hash: the listing's copy of the immutable
// facts about one name.
type Child struct {
	Name string
	Type string // TypeFile, TypeDir, TypeLink, or "" when the code is unknown
	ID   uint64
	// Pending marks a field a link operation has claimed but not settled.
	Pending bool
	// Raw is the field value as stored, for compare-and-set repairs.
	Raw string
	// Src is the address (<parent id>/<escaped name>) a pending rename copied
	// from; empty for a pending create.
	Src string
}

// Value is the hash value for this child, e.g. "d:42".
func (c Child) Value() string {
	return fmt.Sprintf("%c:%d", TypeCode(c.Type), c.ID)
}

// PendingValue marks a field whose entry is still being written at file level.
// The marker is the third segment of "d:42:p": the inode is already decided, the
// type is not announced until the field is settled.
func (c Child) PendingValue() string {
	return c.Value() + ":p"
}

// MovePendingValue marks the destination field of a rename. It carries the
// source address, so a repair that finds the destination half-moved can tell a
// rename that never took the source from one that did.
func (c Child) MovePendingValue(src string) string {
	return c.PendingValue() + ":" + src
}

// ParseChild decodes a hash field. Unknown codes and malformed ids keep the
// name and degrade the type to unknown, so a corrupt record is still listed.
// A pending field (file level) keeps its inode but reports unknown type: the
// kernel stats it instead of trusting a half-written entry.
func ParseChild(name, value string) Child {
	c := Child{Name: name, Raw: value}
	i := strings.IndexByte(value, ':')
	if i < 0 {
		return c
	}
	c.Type = TypeFromCode(value[0])
	rest := value[i+1:]
	if j := strings.IndexByte(rest, ':'); j >= 0 {
		marker := rest[j+1:]
		rest = rest[:j]
		c.Type = ""
		c.Pending = true
		c.Src = strings.TrimPrefix(marker, "p:")
		if c.Src == marker {
			c.Src = ""
		}
	}
	if id, err := strconv.ParseUint(rest, 10, 64); err == nil {
		c.ID = id
	}
	return c
}

// TypeCode maps a metadata type to its one-byte listing code.
func TypeCode(typ string) byte {
	switch typ {
	case TypeFile:
		return CodeFile
	case TypeDir:
		return CodeDir
	case TypeLink:
		return CodeLink
	default:
		return CodeUnknown
	}
}

// TypeFromCode maps a listing code back to a metadata type, "" when unknown.
func TypeFromCode(code byte) string {
	switch code {
	case CodeFile:
		return TypeFile
	case CodeDir:
		return TypeDir
	case CodeLink:
		return TypeLink
	default:
		return ""
	}
}

// FuseModeType returns the S_IF* bits for a metadata type, 0 when unknown.
func FuseModeType(typ string) uint32 {
	switch typ {
	case TypeDir:
		return 0040000
	case TypeLink:
		return 0120000
	case TypeFile:
		return 0100000
	default:
		return 0
	}
}

// Meta is the JSON blob stored at the entry's meta suffix
// (kvfs/<ns>/<parent id>/<name>/meta).
// The id is the immutable inode; the create script fills it in, which is why a
// fresh template carries 0.
type Meta struct {
	ID   uint64 `json:"id"`
	Type string `json:"type"` // file | dir | link
	Mode uint32 `json:"mode"` // permission bits only
	UID  uint32 `json:"uid"`
	GID  uint32 `json:"gid"`
	// Size is not stored: the length of the content key is the authority, and
	// Stat fills this field from it.
	Size  uint64 `json:"-"`
	Nlink uint32 `json:"nlink"`
	Mtime int64  `json:"mtime"`
	Ctime int64  `json:"ctime"`
	Atime int64  `json:"atime"`
}

func (m *Meta) Marshal() (string, error) {
	b, err := json.Marshal(m)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func UnmarshalMeta(s string) (*Meta, error) {
	var m Meta
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		return nil, err
	}
	return &m, nil
}

func NewMeta(id uint64, typ string, mode, uid, gid uint32) *Meta {
	now := time.Now().Unix()
	return &Meta{
		ID:    id,
		Type:  typ,
		Mode:  mode,
		UID:   uid,
		GID:   gid,
		Nlink: 1,
		Mtime: now,
		Ctime: now,
		Atime: now,
	}
}

func FuseMode(m *Meta) uint32 {
	return m.Mode&0777 | FuseModeType(m.Type)
}
