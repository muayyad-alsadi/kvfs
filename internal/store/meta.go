package store

import (
	"encoding/json"
	"time"
)

const (
	TypeFile = "file"
	TypeDir  = "dir"
	TypeLink = "link"

	RootParentID uint64 = 0
	RootName            = ""
	RootID       uint64 = 1
)

// Meta is the JSON blob stored at kvfs:{ns}:meta:{parentId}:{basename}.
type Meta struct {
	ID    uint64 `json:"id"`
	Type  string `json:"type"` // file | dir | link
	Mode  uint32 `json:"mode"` // permission bits only
	UID   uint32 `json:"uid"`
	GID   uint32 `json:"gid"`
	Size  uint64 `json:"size"`
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

func NewMeta(id uint64, typ string, mode, uid, gid uint32, size uint64) *Meta {
	now := time.Now().Unix()
	return &Meta{
		ID:    id,
		Type:  typ,
		Mode:  mode,
		UID:   uid,
		GID:   gid,
		Size:  size,
		Nlink: 1,
		Mtime: now,
		Ctime: now,
		Atime: now,
	}
}

func FuseMode(m *Meta) uint32 {
	perm := m.Mode & 0777
	switch m.Type {
	case TypeDir:
		return perm | 0040000 // S_IFDIR
	case TypeLink:
		return perm | 0120000 // S_IFLNK
	default:
		return perm | 0100000 // S_IFREG
	}
}
