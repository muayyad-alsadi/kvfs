package store

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/rueidis"
)

const DefaultOpTimeout = time.Second

var (
	ErrNotFound = errors.New("not found")
	ErrExist    = errors.New("already exists")
	ErrNotEmpty = errors.New("directory not empty")
	ErrNotDir   = errors.New("not a directory")
	ErrIsDir    = errors.New("is a directory")
	ErrTimeout  = errors.New("redis operation timed out")
)

// Ref locates a node in Redis: meta/dir/data keys use (ParentID, Name);
// ID is the immutable inode number stored inside meta JSON.
type Ref struct {
	ParentID uint64
	Name     string
	ID       uint64
}

func RootRef() Ref {
	return Ref{ParentID: RootParentID, Name: RootName, ID: RootID}
}

// Store talks to Redis/Valkey using the kvfs key schema.
type Store struct {
	client    rueidis.Client
	ns        string
	opTimeout time.Duration
}

func New(client rueidis.Client, ns string) *Store {
	if ns == "" {
		ns = "default"
	}
	return &Store{client: client, ns: ns, opTimeout: DefaultOpTimeout}
}

func (s *Store) SetOpTimeout(d time.Duration) {
	if d > 0 {
		s.opTimeout = d
	}
}

func (s *Store) NS() string            { return s.ns }
func (s *Store) Client() rueidis.Client { return s.client }

func (s *Store) ctx(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	if _, ok := parent.Deadline(); ok {
		return parent, func() {}
	}
	return context.WithTimeout(parent, s.opTimeout)
}

func mapTimeout(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return fmt.Errorf("%w: %v", ErrTimeout, err)
	}
	return err
}

func (s *Store) Publish(ctx context.Context, parentID uint64, name string) error {
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	cmd := s.client.B().Publish().Channel(s.Channel()).Message(InvalidatePayload(parentID, name)).Build()
	return mapTimeout(s.client.Do(ctx, cmd).Error())
}

// BootstrapRoot creates the namespace root exactly once using SET NX.
func (s *Store) BootstrapRoot(ctx context.Context, uid, gid uint32) error {
	ctx, cancel := s.ctx(ctx)
	defer cancel()

	metaKey := s.metaKey(RootParentID, RootName)
	exists, err := s.client.Do(ctx, s.client.B().Exists().Key(metaKey).Build()).AsInt64()
	if err != nil {
		return mapTimeout(err)
	}
	if exists > 0 {
		return nil
	}

	// Ensure counter is at least 1 (SETNX so concurrent mounts do not clobber).
	_, err = s.client.Do(ctx, s.client.B().Set().Key(s.counterKey()).Value("1").Nx().Build()).ToString()
	if err != nil && !rueidis.IsRedisNil(err) {
		return mapTimeout(err)
	}

	meta := NewMeta(RootID, TypeDir, 0755, uid, gid, 0)
	raw, err := meta.Marshal()
	if err != nil {
		return err
	}

	// SET NX meta — only one mount wins.
	ok, err := s.client.Do(ctx, s.client.B().Set().Key(metaKey).Value(raw).Nx().Build()).ToString()
	if err != nil && !rueidis.IsRedisNil(err) {
		return mapTimeout(err)
	}
	if ok == "" || rueidis.IsRedisNil(err) {
		// Another mount created root; verify it exists.
		exists, err = s.client.Do(ctx, s.client.B().Exists().Key(metaKey).Build()).AsInt64()
		if err != nil {
			return mapTimeout(err)
		}
		if exists == 0 {
			return fmt.Errorf("bootstrap race: root meta missing")
		}
		return nil
	}

	dirKey := s.dirKey(RootParentID, RootName)
	// Ensure empty dir set exists (SADD+SREM placeholder is race-safe enough).
	_ = s.client.Do(ctx, s.client.B().Sadd().Key(dirKey).Member("__placeholder__").Build()).Error()
	_ = s.client.Do(ctx, s.client.B().Srem().Key(dirKey).Member("__placeholder__").Build()).Error()
	return nil
}

func (s *Store) GetMeta(ctx context.Context, parentID uint64, name string) (*Meta, error) {
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	raw, err := s.client.Do(ctx, s.client.B().Get().Key(s.metaKey(parentID, name)).Build()).ToString()
	if err != nil {
		if rueidis.IsRedisNil(err) {
			return nil, ErrNotFound
		}
		return nil, mapTimeout(err)
	}
	return UnmarshalMeta(raw)
}

func (s *Store) GetMetaRef(ctx context.Context, ref Ref) (*Meta, error) {
	return s.GetMeta(ctx, ref.ParentID, ref.Name)
}

func (s *Store) GetData(ctx context.Context, parentID uint64, name string) ([]byte, error) {
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	raw, err := s.client.Do(ctx, s.client.B().Get().Key(s.dataKey(parentID, name)).Build()).AsBytes()
	if err != nil {
		if rueidis.IsRedisNil(err) {
			return []byte{}, nil
		}
		return nil, mapTimeout(err)
	}
	return raw, nil
}

func (s *Store) ListChildren(ctx context.Context, dir Ref) ([]string, error) {
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	members, err := s.client.Do(ctx, s.client.B().Smembers().Key(s.dirKey(dir.ParentID, dir.Name)).Build()).AsStrSlice()
	if err != nil {
		if rueidis.IsRedisNil(err) {
			return nil, nil
		}
		return nil, mapTimeout(err)
	}
	return members, nil
}

func (s *Store) nextID(ctx context.Context, c rueidis.DedicatedClient) (uint64, error) {
	n, err := c.Do(ctx, c.B().Incr().Key(s.counterKey()).Build()).AsInt64()
	if err != nil {
		return 0, mapTimeout(err)
	}
	return uint64(n), nil
}

func (s *Store) CreateFile(ctx context.Context, parent Ref, name string, mode, uid, gid uint32) (*Meta, Ref, error) {
	ctx, cancel := s.ctx(ctx)
	defer cancel()

	if _, err := s.GetMeta(ctx, parent.ID, name); err == nil {
		return nil, Ref{}, ErrExist
	} else if !errors.Is(err, ErrNotFound) {
		return nil, Ref{}, err
	}

	var meta *Meta
	err := s.dedicated(ctx, func(c rueidis.DedicatedClient) error {
		id, err := s.nextID(ctx, c)
		if err != nil {
			return err
		}
		meta = NewMeta(id, TypeFile, mode&0777, uid, gid, 0)
		raw, err := meta.Marshal()
		if err != nil {
			return err
		}
		return s.exec(ctx, c,
			c.B().Set().Key(s.metaKey(parent.ID, name)).Value(raw).Build(),
			c.B().Set().Key(s.dataKey(parent.ID, name)).Value("").Build(),
			c.B().Sadd().Key(s.dirKey(parent.ParentID, parent.Name)).Member(name).Build(),
		)
	})
	if err != nil {
		return nil, Ref{}, err
	}
	_ = s.Publish(ctx, parent.ID, name)
	return meta, Ref{ParentID: parent.ID, Name: name, ID: meta.ID}, nil
}

func (s *Store) Mkdir(ctx context.Context, parent Ref, name string, mode, uid, gid uint32) (*Meta, Ref, error) {
	ctx, cancel := s.ctx(ctx)
	defer cancel()

	if _, err := s.GetMeta(ctx, parent.ID, name); err == nil {
		return nil, Ref{}, ErrExist
	} else if !errors.Is(err, ErrNotFound) {
		return nil, Ref{}, err
	}

	var meta *Meta
	err := s.dedicated(ctx, func(c rueidis.DedicatedClient) error {
		id, err := s.nextID(ctx, c)
		if err != nil {
			return err
		}
		meta = NewMeta(id, TypeDir, mode&0777, uid, gid, 0)
		raw, err := meta.Marshal()
		if err != nil {
			return err
		}
		childDir := s.dirKey(parent.ID, name)
		return s.exec(ctx, c,
			c.B().Set().Key(s.metaKey(parent.ID, name)).Value(raw).Build(),
			c.B().Sadd().Key(childDir).Member("__placeholder__").Build(),
			c.B().Srem().Key(childDir).Member("__placeholder__").Build(),
			c.B().Sadd().Key(s.dirKey(parent.ParentID, parent.Name)).Member(name).Build(),
		)
	})
	if err != nil {
		return nil, Ref{}, err
	}
	_ = s.Publish(ctx, parent.ID, name)
	return meta, Ref{ParentID: parent.ID, Name: name, ID: meta.ID}, nil
}

func (s *Store) Symlink(ctx context.Context, parent Ref, name, target string, uid, gid uint32) (*Meta, Ref, error) {
	ctx, cancel := s.ctx(ctx)
	defer cancel()

	if _, err := s.GetMeta(ctx, parent.ID, name); err == nil {
		return nil, Ref{}, ErrExist
	} else if !errors.Is(err, ErrNotFound) {
		return nil, Ref{}, err
	}

	var meta *Meta
	err := s.dedicated(ctx, func(c rueidis.DedicatedClient) error {
		id, err := s.nextID(ctx, c)
		if err != nil {
			return err
		}
		meta = NewMeta(id, TypeLink, 0777, uid, gid, uint64(len(target)))
		raw, err := meta.Marshal()
		if err != nil {
			return err
		}
		return s.exec(ctx, c,
			c.B().Set().Key(s.metaKey(parent.ID, name)).Value(raw).Build(),
			c.B().Set().Key(s.dataKey(parent.ID, name)).Value(target).Build(),
			c.B().Sadd().Key(s.dirKey(parent.ParentID, parent.Name)).Member(name).Build(),
		)
	})
	if err != nil {
		return nil, Ref{}, err
	}
	_ = s.Publish(ctx, parent.ID, name)
	return meta, Ref{ParentID: parent.ID, Name: name, ID: meta.ID}, nil
}

func (s *Store) Unlink(ctx context.Context, parent Ref, name string) error {
	ctx, cancel := s.ctx(ctx)
	defer cancel()

	meta, err := s.GetMeta(ctx, parent.ID, name)
	if err != nil {
		return err
	}
	if meta.Type == TypeDir {
		return ErrIsDir
	}
	err = s.dedicated(ctx, func(c rueidis.DedicatedClient) error {
		return s.exec(ctx, c,
			c.B().Srem().Key(s.dirKey(parent.ParentID, parent.Name)).Member(name).Build(),
			c.B().Del().Key(s.metaKey(parent.ID, name)).Key(s.dataKey(parent.ID, name)).Build(),
		)
	})
	if err != nil {
		return err
	}
	_ = s.Publish(ctx, parent.ID, name)
	return nil
}

func (s *Store) Rmdir(ctx context.Context, parent Ref, name string) error {
	ctx, cancel := s.ctx(ctx)
	defer cancel()

	meta, err := s.GetMeta(ctx, parent.ID, name)
	if err != nil {
		return err
	}
	if meta.Type != TypeDir {
		return ErrNotDir
	}
	childRef := Ref{ParentID: parent.ID, Name: name, ID: meta.ID}
	children, err := s.ListChildren(ctx, childRef)
	if err != nil {
		return err
	}
	if len(children) > 0 {
		return ErrNotEmpty
	}
	err = s.dedicated(ctx, func(c rueidis.DedicatedClient) error {
		return s.exec(ctx, c,
			c.B().Srem().Key(s.dirKey(parent.ParentID, parent.Name)).Member(name).Build(),
			c.B().Del().Key(s.metaKey(parent.ID, name)).Key(s.dirKey(parent.ID, name)).Build(),
		)
	})
	if err != nil {
		return err
	}
	_ = s.Publish(ctx, parent.ID, name)
	return nil
}

func (s *Store) Rename(ctx context.Context, oldParent Ref, oldName string, newParent Ref, newName string) error {
	ctx, cancel := s.ctx(ctx)
	defer cancel()

	if oldParent.ID == newParent.ID && oldName == newName {
		return nil
	}
	oldMeta, err := s.GetMeta(ctx, oldParent.ID, oldName)
	if err != nil {
		return err
	}
	if _, err := s.GetMeta(ctx, newParent.ID, newName); err == nil {
		return ErrExist
	} else if !errors.Is(err, ErrNotFound) {
		return err
	}

	raw, err := oldMeta.Marshal()
	if err != nil {
		return err
	}

	err = s.dedicated(ctx, func(c rueidis.DedicatedClient) error {
		cmds := []rueidis.Completed{
			c.B().Set().Key(s.metaKey(newParent.ID, newName)).Value(raw).Build(),
			c.B().Del().Key(s.metaKey(oldParent.ID, oldName)).Build(),
			c.B().Srem().Key(s.dirKey(oldParent.ParentID, oldParent.Name)).Member(oldName).Build(),
			c.B().Sadd().Key(s.dirKey(newParent.ParentID, newParent.Name)).Member(newName).Build(),
		}
		switch oldMeta.Type {
		case TypeFile, TypeLink:
			data, err := s.GetData(ctx, oldParent.ID, oldName)
			if err != nil {
				return err
			}
			cmds = append(cmds,
				c.B().Set().Key(s.dataKey(newParent.ID, newName)).Value(rueidis.BinaryString(data)).Build(),
				c.B().Del().Key(s.dataKey(oldParent.ID, oldName)).Build(),
			)
		case TypeDir:
			children, err := s.ListChildren(ctx, Ref{ParentID: oldParent.ID, Name: oldName, ID: oldMeta.ID})
			if err != nil {
				return err
			}
			newDirKey := s.dirKey(newParent.ID, newName)
			oldDirKey := s.dirKey(oldParent.ID, oldName)
			if len(children) > 0 {
				cmds = append(cmds, c.B().Sadd().Key(newDirKey).Member(children...).Build())
			} else {
				cmds = append(cmds,
					c.B().Sadd().Key(newDirKey).Member("__placeholder__").Build(),
					c.B().Srem().Key(newDirKey).Member("__placeholder__").Build(),
				)
			}
			cmds = append(cmds, c.B().Del().Key(oldDirKey).Build())
		}
		return s.exec(ctx, c, cmds...)
	})
	if err != nil {
		return err
	}
	_ = s.Publish(ctx, oldParent.ID, oldName)
	_ = s.Publish(ctx, newParent.ID, newName)
	return nil
}

func (s *Store) WriteData(ctx context.Context, parentID uint64, name string, data []byte, publish bool) (*Meta, error) {
	ctx, cancel := s.ctx(ctx)
	defer cancel()

	meta, err := s.GetMeta(ctx, parentID, name)
	if err != nil {
		return nil, err
	}
	if meta.Type == TypeDir {
		return nil, ErrIsDir
	}
	now := time.Now().Unix()
	meta.Size = uint64(len(data))
	meta.Mtime = now
	meta.Ctime = now
	raw, err := meta.Marshal()
	if err != nil {
		return nil, err
	}
	err = s.dedicated(ctx, func(c rueidis.DedicatedClient) error {
		return s.exec(ctx, c,
			c.B().Set().Key(s.dataKey(parentID, name)).Value(rueidis.BinaryString(data)).Build(),
			c.B().Set().Key(s.metaKey(parentID, name)).Value(raw).Build(),
		)
	})
	if err != nil {
		return nil, err
	}
	if publish {
		_ = s.Publish(ctx, parentID, name)
	}
	return meta, nil
}

func (s *Store) UpdateMeta(ctx context.Context, parentID uint64, name string, mut func(*Meta)) (*Meta, error) {
	ctx, cancel := s.ctx(ctx)
	defer cancel()

	meta, err := s.GetMeta(ctx, parentID, name)
	if err != nil {
		return nil, err
	}
	mut(meta)
	meta.Ctime = time.Now().Unix()
	raw, err := meta.Marshal()
	if err != nil {
		return nil, err
	}
	err = s.dedicated(ctx, func(c rueidis.DedicatedClient) error {
		return s.exec(ctx, c,
			c.B().Set().Key(s.metaKey(parentID, name)).Value(raw).Build(),
		)
	})
	if err != nil {
		return nil, err
	}
	_ = s.Publish(ctx, parentID, name)
	return meta, nil
}

func (s *Store) Truncate(ctx context.Context, parentID uint64, name string, size uint64) (*Meta, error) {
	data, err := s.GetData(ctx, parentID, name)
	if err != nil {
		return nil, err
	}
	if uint64(len(data)) > size {
		data = data[:size]
	} else if uint64(len(data)) < size {
		padded := make([]byte, size)
		copy(padded, data)
		data = padded
	}
	return s.WriteData(ctx, parentID, name, data, true)
}

func ParseInvalidate(payload string) (parentID uint64, name string, err error) {
	i := strings.IndexByte(payload, ':')
	if i < 0 {
		return 0, "", fmt.Errorf("bad invalidate payload %q", payload)
	}
	parentID, err = strconv.ParseUint(payload[:i], 10, 64)
	if err != nil {
		return 0, "", err
	}
	return parentID, payload[i+1:], nil
}

func (s *Store) dedicated(ctx context.Context, fn func(rueidis.DedicatedClient) error) error {
	// Dedicated acquisition is not cancelable; keep critical sections short and
	// rely on ctx for Do/DoMulti. Never abandon an in-flight Dedicated call —
	// that permanently leaks the connection under ForceSingleClient.
	return mapTimeout(s.client.Dedicated(func(c rueidis.DedicatedClient) error {
		return fn(c)
	}))
}

func (s *Store) exec(ctx context.Context, c rueidis.DedicatedClient, cmds ...rueidis.Completed) error {
	all := make([]rueidis.Completed, 0, len(cmds)+2)
	all = append(all, c.B().Multi().Build())
	all = append(all, cmds...)
	all = append(all, c.B().Exec().Build())
	resps := c.DoMulti(ctx, all...)
	for _, r := range resps {
		if err := r.Error(); err != nil && !rueidis.IsRedisNil(err) {
			return mapTimeout(err)
		}
	}
	return nil
}
