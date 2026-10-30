package store

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/redis/rueidis"
)

// DefaultOpTimeout bounds one Redis operation when the caller sets no deadline.
const DefaultOpTimeout = time.Second

// Errors returned by the store. internal/fs maps them to errno in one place.
var (
	ErrNotFound = errors.New("not found")
	ErrExist    = errors.New("already exists")
	ErrNotEmpty = errors.New("directory not empty")
	ErrNotDir   = errors.New("not a directory")
	ErrIsDir    = errors.New("is a directory")
	ErrTimeout  = errors.New("redis operation timed out")
	// ErrBusy means another client is mid-commit on the name: a link operation
	// at dir or file level claimed it and has not settled it yet.
	ErrBusy = errors.New("name is mid-commit")
)

// Ref locates a node in Redis: every record of the entry uses (ParentID, Name);
// ID is the immutable inode number stored inside the metadata document.
type Ref struct {
	ParentID uint64
	Name     string
	ID       uint64
}

// RootRef is the root's reference: an ordinary entry named "." whose parent is
// the namespace pseudo-inode 0.
func RootRef() Ref {
	return Ref{ParentID: RootParentID, Name: RootName, ID: RootID}
}

// Store talks to Redis/Valkey using the kvfs key schema.
type Store struct {
	client    rueidis.Client
	ns        string
	level     Level
	opTimeout time.Duration
}

// New validates the namespace and the sharding level, then builds the store.
// A level this build cannot honour is refused here, before any key is read.
func New(client rueidis.Client, ns string, level Level) (*Store, error) {
	if ns == "" {
		ns = "default"
	}
	if !ValidNamespace(ns) {
		return nil, fmt.Errorf("invalid namespace %q: want [A-Za-z0-9-]+, at most %d bytes", ns, MaxNamespaceLen)
	}
	if level == "" {
		level = LevelNamespace
	}
	if _, err := ParseLevel(string(level)); err != nil {
		return nil, err
	}
	return &Store{client: client, ns: ns, level: level, opTimeout: DefaultOpTimeout}, nil
}

// SetOpTimeout bounds each operation. Non-positive values are ignored.
func (s *Store) SetOpTimeout(d time.Duration) {
	if d > 0 {
		s.opTimeout = d
	}
}

// Linked reports whether a link operation spans slots, so it must run as
// claim, prepare, settle instead of as one script. Only a linked store can
// leave the half-finished names that repair resolves.
func (s *Store) Linked() bool { return s.level != LevelNamespace }

// OpTimeout is the bound on one Redis operation.
func (s *Store) OpTimeout() time.Duration { return s.opTimeout }

// NS is the namespace this store addresses.
func (s *Store) NS() string { return s.ns }

// Client exposes the underlying client for pub/sub and health checks.
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

// Publish announces one address on the namespace channel. Mutations publish
// inside their own script; this is for callers that must announce out of band.
func (s *Store) Publish(ctx context.Context, parentID uint64, name string) error {
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	cmd := s.client.B().Publish().Channel(s.Channel()).Message(InvalidatePayload(parentID, name)).Build()
	return mapTimeout(s.client.Do(ctx, cmd).Error())
}

// bootstrapScript initializes a fresh namespace in one atomic step: the first
// mount sets the counter to the root's id and writes the root metadata, so the
// root id is always 1 and two mounts cannot both win.
const bootstrapScript = `
if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
redis.call('SET', KEYS[1], 1)
redis.call('SET', KEYS[2], ARGV[1])
return 1
`

// probePlacements reports which sharding levels already hold this namespace's
// counter, in one pipelined round trip. The counters live in different
// slots, so a script cannot read both: client-side fan-out is what keeps the
// probe to one round trip.
func (s *Store) probePlacements(ctx context.Context) ([]Level, error) {
	placements := Levels()
	probes := make([]rueidis.Completed, 0, len(placements))
	for _, l := range placements {
		probes = append(probes, s.client.B().Exists().Key(Key(s.ns, l, NamespaceID, RootName, SuffixCounter)).Build())
	}
	resps := s.client.DoMulti(ctx, probes...)
	var found []Level
	for i, r := range resps {
		n, err := r.AsInt64()
		if err != nil {
			return nil, mapTimeout(err)
		}
		if n > 0 {
			found = append(found, placements[i])
		}
	}
	return found, nil
}

// checkPlacement refuses a namespace that belongs to another level, or to more
// than one: two counters mean inode numbers can collide.
func (s *Store) checkPlacement(found []Level) error {
	switch {
	case len(found) > 1:
		return fmt.Errorf("namespace %q has counters at more than one sharding level: refusing to guess", s.ns)
	case len(found) == 1 && found[0] != s.level:
		return fmt.Errorf("namespace %q was initialized at %q level, this mount is %q", s.ns, found[0], s.level)
	}
	return nil
}

// BootstrapRoot creates the namespace root exactly once and verifies that an
// existing namespace was initialized at the level this mount is configured for.
func (s *Store) BootstrapRoot(ctx context.Context, uid, gid uint32) error {
	ctx, cancel := s.ctx(ctx)
	defer cancel()

	found, err := s.probePlacements(ctx)
	if err != nil {
		return err
	}
	if err := s.checkPlacement(found); err != nil {
		return err
	}
	if len(found) == 1 {
		if _, err := s.GetMeta(ctx, NamespaceID, RootName); err != nil {
			return fmt.Errorf("namespace %q has a counter but no root: %w", s.ns, err)
		}
		return nil
	}
	return s.initRoot(ctx, uid, gid)
}

func (s *Store) initRoot(ctx context.Context, uid, gid uint32) error {
	meta := NewMeta(RootID, TypeDir, 0755, uid, gid)
	raw, err := meta.Marshal()
	if err != nil {
		return err
	}
	n, err := s.eval(ctx, bootstrapScript,
		[]string{s.counterKey(), s.metaKey(NamespaceID, RootName)},
		[]string{raw},
	)
	if err != nil {
		return err
	}
	if n == 1 {
		return nil
	}
	// Another mount won the race between the probe and this script.
	if _, err := s.GetMeta(ctx, NamespaceID, RootName); err != nil {
		return fmt.Errorf("namespace %q lost initialization and has no root: %w", s.ns, err)
	}
	return nil
}

// createScript claims a name and writes the entry in one step. The metadata
// template carries the id as 0; the script substitutes the id it allocates, so
// create stays one round trip without a second write to an unclaimed name. The
// substitution is safe because no user-controlled text reaches the metadata
// document: every field is a type, a number, or a permission bit.
//
// KEYS[6] is the parent directory's metadata, which shares a slot with its
// listing, and ARGV[7] the inode the caller believes it has. A parent that was
// removed or replaced is refused (-1), so a stale creator writes nothing.
const createScript = `
local pm = redis.call('GET', KEYS[6])
if not pm or string.match(pm, '^{"id":(%d+)[,}]') ~= ARGV[7] then return -1 end
if redis.call('HEXISTS', KEYS[1], ARGV[1]) == 1 then return 0 end
local id = redis.call('INCR', KEYS[2])
local meta = string.gsub(ARGV[2], '"id":0', '"id":' .. id, 1)
redis.call('SET', KEYS[3], meta)
if ARGV[6] == '1' then redis.call('SET', KEYS[4], ARGV[3]) end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[4] .. ':' .. id)
redis.call('PUBLISH', KEYS[5], ARGV[5])
return id
`

// GetMeta reads one entry's metadata document by (parent, name). The inode in
// the document is the authority; the parent's listing is not consulted.
func (s *Store) GetMeta(ctx context.Context, parentID uint64, name string) (*Meta, error) {
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	raw, err := s.readString(ctx, s.client.B().Get().Key(s.metaKey(parentID, name)).Build())
	if err != nil {
		return nil, err
	}
	return UnmarshalMeta(raw)
}

// readString runs one string-valued read and gives a nil reply a single meaning:
// the record is missing. Every metadata-shaped read shares the error path.
func (s *Store) readString(ctx context.Context, cmd rueidis.Completed) (string, error) {
	raw, err := s.client.Do(ctx, cmd).ToString()
	if err != nil {
		if rueidis.IsRedisNil(err) {
			return "", ErrNotFound
		}
		return "", mapTimeout(err)
	}
	return raw, nil
}

// GetMetaRef reads the metadata of the entry a Ref points at.
func (s *Store) GetMetaRef(ctx context.Context, ref Ref) (*Meta, error) {
	return s.GetMeta(ctx, ref.ParentID, ref.Name)
}

// GetData returns an entry's content, or empty bytes when it has none.
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

// FieldValue returns the stored value of one listing field, so a caller can
// remember exactly what it saw before it asks for a compare-and-set repair.
func (s *Store) FieldValue(ctx context.Context, parent Ref, name string) (string, error) {
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	raw, _, err := s.field(ctx, parent, name)
	return raw, err
}

// ListChildren returns a directory's entries from one HGETALL: the type and
// inode come from the hash value, so readdir never reads metadata per child.
func (s *Store) ListChildren(ctx context.Context, dir Ref) ([]Child, error) {
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	raw, err := s.client.Do(ctx, s.client.B().Hgetall().Key(s.dirKey(dir.ParentID, dir.Name)).Build()).AsStrMap()
	if err != nil {
		if rueidis.IsRedisNil(err) {
			return nil, nil
		}
		return nil, mapTimeout(err)
	}
	children := make([]Child, 0, len(raw))
	for name, value := range raw {
		children = append(children, ParseChild(name, value))
	}
	sort.Slice(children, func(i, j int) bool { return children[i].Name < children[j].Name })
	return children, nil
}

func (s *Store) create(ctx context.Context, parent Ref, name string, meta *Meta, data []byte, writeData bool) (*Meta, Ref, error) {
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	if s.Linked() {
		return s.createLinked(ctx, parent, name, meta, data, writeData)
	}

	tmpl, err := meta.Marshal()
	if err != nil {
		return nil, Ref{}, err
	}
	flag := "0"
	if writeData {
		flag = "1"
	}
	n, err := s.eval(ctx, createScript,
		[]string{
			s.dirKey(parent.ParentID, parent.Name),
			s.counterKey(),
			s.metaKey(parent.ID, name),
			s.dataKey(parent.ID, name),
			s.Channel(),
			s.metaKey(parent.ParentID, parent.Name),
		},
		[]string{name, tmpl, string(data), string(TypeCode(meta.Type)), InvalidatePayload(parent.ID, name), flag, strconv.FormatUint(parent.ID, 10)},
	)
	if err != nil {
		return nil, Ref{}, err
	}
	if n < 0 {
		return nil, Ref{}, fmt.Errorf("create %s: the parent directory was removed: %w", name, ErrNotFound)
	}
	if n == 0 {
		return nil, Ref{}, ErrExist
	}
	meta.ID = uint64(n)
	return meta, Ref{ParentID: parent.ID, Name: name, ID: meta.ID}, nil
}

// CreateFile creates a regular file whose content key exists and is empty, so a
// later move never needs an existence branch and presence never means "empty".
func (s *Store) CreateFile(ctx context.Context, parent Ref, name string, mode, uid, gid uint32) (*Meta, Ref, error) {
	return s.create(ctx, parent, name, NewMeta(0, TypeFile, mode&0777, uid, gid), nil, true)
}

// Mkdir creates a directory. It writes no listing key: an empty directory is
// the absence of a hash, not a placeholder member.
func (s *Store) Mkdir(ctx context.Context, parent Ref, name string, mode, uid, gid uint32) (*Meta, Ref, error) {
	return s.create(ctx, parent, name, NewMeta(0, TypeDir, mode&0777, uid, gid), nil, false)
}

// Symlink stores the target path as content, like a file.
func (s *Store) Symlink(ctx context.Context, parent Ref, name, target string, uid, gid uint32) (*Meta, Ref, error) {
	meta := NewMeta(0, TypeLink, 0777, uid, gid)
	meta.Size = uint64(len(target))
	return s.create(ctx, parent, name, meta, []byte(target), true)
}

const unlinkScript = `
local v = redis.call('HGET', KEYS[3], ARGV[1])
if not v then return -1 end
if string.sub(v, 1, 1) == 'd' then return -2 end
if redis.call('EXISTS', KEYS[1]) == 0 then return -1 end
redis.call('DEL', KEYS[1])
redis.call('DEL', KEYS[2])
redis.call('HDEL', KEYS[3], ARGV[1])
redis.call('PUBLISH', KEYS[4], ARGV[2])
return 1
`

// Unlink removes a file or symlink. The type comes from the listing's immutable
// code inside the script, so there is no read-then-write window and one round
// trip suffices.
func (s *Store) Unlink(ctx context.Context, parent Ref, name string) error {
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	if s.Linked() {
		return s.unlinkLinked(ctx, parent, name)
	}
	n, err := s.eval(ctx, unlinkScript,
		[]string{
			s.metaKey(parent.ID, name),
			s.dataKey(parent.ID, name),
			s.dirKey(parent.ParentID, parent.Name),
			s.Channel(),
		},
		[]string{name, InvalidatePayload(parent.ID, name)},
	)
	if err != nil {
		return err
	}
	switch n {
	case 1:
		return nil
	case -2:
		return ErrIsDir
	default:
		return ErrNotFound
	}
}

const rmdirScript = `
local v = redis.call('HGET', KEYS[3], ARGV[1])
if not v then return -1 end
if string.sub(v, 1, 1) ~= 'd' then return -2 end
if redis.call('EXISTS', KEYS[1]) == 0 then return -1 end
if redis.call('HLEN', KEYS[2]) > 0 then return 0 end
redis.call('DEL', KEYS[1])
redis.call('DEL', KEYS[2])
redis.call('HDEL', KEYS[3], ARGV[1])
redis.call('PUBLISH', KEYS[4], ARGV[2])
return 1
`

// Rmdir removes an empty directory. Emptiness is a count inside the script, so
// no member is ever fetched, and the type check cannot race a concurrent
// replacement of the name.
func (s *Store) Rmdir(ctx context.Context, parent Ref, name string) error {
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	if s.Linked() {
		return s.rmdirLinked(ctx, parent, name)
	}
	n, err := s.eval(ctx, rmdirScript,
		[]string{
			s.metaKey(parent.ID, name),
			s.dirKey(parent.ID, name),
			s.dirKey(parent.ParentID, parent.Name),
			s.Channel(),
		},
		[]string{name, InvalidatePayload(parent.ID, name)},
	)
	if err != nil {
		return err
	}
	switch n {
	case 1:
		return nil
	case 0:
		return ErrNotEmpty
	case -2:
		return ErrNotDir
	default:
		return ErrNotFound
	}
}

// renameScript moves an entry within one slot without reading it first: the
// source field carries the immutable facts the listing needs, so the script is
// the only round trip. Key renames replace byte copies, the destination check
// closes the check-then-write race, and each name that changed is published.
// ARGV[5] is "1" when an existing destination may be replaced, with POSIX
// rules: a non-directory replaces a non-directory, a directory replaces only an
// empty directory, and any other pairing is refused.
const renameScript = `
local pm = redis.call('GET', KEYS[10])
if not pm or string.match(pm, '^{"id":(%d+)[,}]') ~= ARGV[6] then return {'missing', ''} end
local v = redis.call('HGET', KEYS[7], ARGV[1])
if not v then return {'missing', ''} end
if redis.call('EXISTS', KEYS[1]) == 0 then return {'missing', ''} end
local dv = redis.call('HGET', KEYS[8], ARGV[2])
if dv then
  if ARGV[5] ~= '1' then return {'exists', ''} end
  local src = string.sub(v, 1, 1) == 'd'
  local dst = string.sub(dv, 1, 1) == 'd'
  if dst and not src then return {'isdir', ''} end
  if src and not dst then return {'notdir', ''} end
  if dst and redis.call('HLEN', KEYS[6]) > 0 then return {'notempty', ''} end
  redis.call('DEL', KEYS[4], KEYS[5], KEYS[6])
end
redis.call('RENAME', KEYS[1], KEYS[4])
if redis.call('EXISTS', KEYS[2]) == 1 then redis.call('RENAME', KEYS[2], KEYS[5]) end
if redis.call('EXISTS', KEYS[3]) == 1 then redis.call('RENAME', KEYS[3], KEYS[6]) end
redis.call('HDEL', KEYS[7], ARGV[1])
redis.call('HSET', KEYS[8], ARGV[2], v)
redis.call('PUBLISH', KEYS[9], ARGV[3])
redis.call('PUBLISH', KEYS[9], ARGV[4])
return {'ok', v}
`

// renameMode says what a rename does when the destination name is taken.
type renameMode string

const (
	replaceExisting renameMode = "1"
	refuseExisting  renameMode = "0"
)

// Rename moves one entry and returns its unchanged inode. A taken destination
// is replaced as POSIX rename(2) says: a non-directory replaces a
// non-directory (ErrIsDir if the destination is a directory), a directory
// replaces only an empty directory (ErrNotDir, ErrNotEmpty otherwise). At the
// default level it is one script, so the caller does not read the entry first.
func (s *Store) Rename(ctx context.Context, oldParent Ref, oldName string, newParent Ref, newName string) (uint64, error) {
	return s.rename(ctx, oldParent, oldName, newParent, newName, replaceExisting)
}

// RenameNoReplace is Rename that fails with ErrExist when the destination name
// is taken, which is what RENAME_NOREPLACE asks for.
func (s *Store) RenameNoReplace(ctx context.Context, oldParent Ref, oldName string, newParent Ref, newName string) (uint64, error) {
	return s.rename(ctx, oldParent, oldName, newParent, newName, refuseExisting)
}

func (s *Store) rename(ctx context.Context, oldParent Ref, oldName string, newParent Ref, newName string, mode renameMode) (uint64, error) {
	if oldParent.ID == newParent.ID && oldName == newName {
		return 0, nil
	}
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	if s.Linked() {
		return s.renameLinked(ctx, oldParent, oldName, newParent, newName, mode)
	}
	reply, err := s.evalStrs(ctx, renameScript,
		[]string{
			s.metaKey(oldParent.ID, oldName),
			s.dataKey(oldParent.ID, oldName),
			s.dirKey(oldParent.ID, oldName),
			s.metaKey(newParent.ID, newName),
			s.dataKey(newParent.ID, newName),
			s.dirKey(newParent.ID, newName),
			s.dirKey(oldParent.ParentID, oldParent.Name),
			s.dirKey(newParent.ParentID, newParent.Name),
			s.Channel(),
			s.metaKey(newParent.ParentID, newParent.Name),
		},
		[]string{
			oldName,
			newName,
			InvalidatePayload(oldParent.ID, oldName),
			InvalidatePayload(newParent.ID, newName),
			string(mode),
			strconv.FormatUint(newParent.ID, 10),
		},
	)
	if err != nil {
		return 0, err
	}
	if len(reply) != 2 {
		return 0, fmt.Errorf("rename: unexpected reply %v", reply)
	}
	if reply[0] != "ok" {
		return 0, renameRefusal(reply[0])
	}
	return ParseChild(newName, reply[1]).ID, nil
}

// renameRefusals maps the script's verdicts to the store's sentinels.
var renameRefusals = map[string]error{
	"exists":   ErrExist,
	"isdir":    ErrIsDir,
	"notdir":   ErrNotDir,
	"notempty": ErrNotEmpty,
}

func renameRefusal(verdict string) error {
	if err, ok := renameRefusals[verdict]; ok {
		return err
	}
	return ErrNotFound
}

// WriteData replaces a file's whole content and its timestamps in one
// transaction. Partial writes use WriteAt.
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
	if err := s.writeEntry(ctx, parentID, name, meta, data, true, publish); err != nil {
		return nil, err
	}
	return meta, nil
}

// maxUpdateAttempts bounds how often UpdateMeta re-reads a document that keeps
// changing under it before it reports the entry busy.
const maxUpdateAttempts = 8

// errConflict means a watched key changed before EXEC, so the transaction
// wrote nothing.
var errConflict = errors.New("a watched key changed")

// UpdateMeta applies mut to the metadata document, writes it back with its
// invalidation, and returns it with the content size. The read is watched: a
// change that lands between the read and the write makes the attempt start
// over on the new document instead of overwriting it. An attempt is two round
// trips.
func (s *Store) UpdateMeta(ctx context.Context, parentID uint64, name string, mut func(*Meta)) (*Meta, error) {
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	for range maxUpdateAttempts {
		meta, err := s.updateMetaOnce(ctx, parentID, name, mut)
		if !errors.Is(err, errConflict) {
			return meta, err
		}
	}
	return nil, fmt.Errorf("update %s: metadata kept changing: %w", name, ErrBusy)
}

func (s *Store) updateMetaOnce(ctx context.Context, parentID uint64, name string, mut func(*Meta)) (*Meta, error) {
	metaKey := s.metaKey(parentID, name)
	var meta *Meta
	err := s.dedicated(ctx, func(c rueidis.DedicatedClient) error {
		got := c.DoMulti(ctx,
			c.B().Watch().Key(metaKey).Build(),
			c.B().Get().Key(metaKey).Build(),
			c.B().Strlen().Key(s.dataKey(parentID, name)).Build(),
		)
		m, cmds, err := s.prepareUpdate(parentID, name, got, mut)
		if err != nil {
			// No EXEC follows to clear the watch, and the read's error is the
			// one worth reporting.
			c.Do(ctx, c.B().Unwatch().Build())
			return err
		}
		if err := s.exec(ctx, c, cmds...); err != nil {
			return err
		}
		meta = m
		return nil
	})
	return meta, err
}

// prepareUpdate decodes a watched read, applies mut, and builds the writes
// that commit it.
func (s *Store) prepareUpdate(parentID uint64, name string, got []rueidis.RedisResult, mut func(*Meta)) (*Meta, []rueidis.Completed, error) {
	if err := got[0].Error(); err != nil {
		return nil, nil, mapTimeout(err)
	}
	m, err := statReply(got[1], got[2])
	if err != nil {
		return nil, nil, err
	}
	mut(m)
	m.Ctime = time.Now().Unix()
	cmds, err := s.entryWrites(parentID, name, m, nil, false, true)
	if err != nil {
		return nil, nil, err
	}
	return m, cmds, nil
}

// writeEntry writes the metadata document, optionally its content, and the
// invalidation in one transaction.
func (s *Store) writeEntry(ctx context.Context, parentID uint64, name string, meta *Meta, data []byte, withData, publish bool) error {
	cmds, err := s.entryWrites(parentID, name, meta, data, withData, publish)
	if err != nil {
		return err
	}
	return s.dedicated(ctx, func(c rueidis.DedicatedClient) error {
		return s.exec(ctx, c, cmds...)
	})
}

func (s *Store) entryWrites(parentID uint64, name string, meta *Meta, data []byte, withData, publish bool) ([]rueidis.Completed, error) {
	raw, err := meta.Marshal()
	if err != nil {
		return nil, err
	}
	cmds := make([]rueidis.Completed, 0, 3)
	if withData {
		cmds = append(cmds, s.client.B().Set().Key(s.dataKey(parentID, name)).Value(rueidis.BinaryString(data)).Build())
	}
	cmds = append(cmds, s.client.B().Set().Key(s.metaKey(parentID, name)).Value(raw).Build())
	if publish {
		cmds = append(cmds, s.client.B().Publish().Channel(s.Channel()).Message(InvalidatePayload(parentID, name)).Build())
	}
	return cmds, nil
}

func (s *Store) evalCmd(script string, keys, args []string) (rueidis.Completed, error) {
	// rueidis types the script builder stage by stage: the key list is required
	// to reach the argument stage, which is optional.
	if len(keys) == 0 {
		return rueidis.Completed{}, fmt.Errorf("eval: script needs at least one key")
	}
	kb := s.client.B().Eval().Script(script).Numkeys(int64(len(keys))).Key(keys[0])
	for _, k := range keys[1:] {
		kb = kb.Key(k)
	}
	if len(args) == 0 {
		return kb.Build(), nil
	}
	ab := kb.Arg(args[0])
	for _, a := range args[1:] {
		ab = ab.Arg(a)
	}
	return ab.Build(), nil
}

func (s *Store) eval(ctx context.Context, script string, keys, args []string) (int64, error) {
	cmd, err := s.evalCmd(script, keys, args)
	if err != nil {
		return 0, err
	}
	n, err := s.client.Do(ctx, cmd).AsInt64()
	if err != nil {
		return 0, mapTimeout(err)
	}
	return n, nil
}

func (s *Store) evalStrs(ctx context.Context, script string, keys, args []string) ([]string, error) {
	cmd, err := s.evalCmd(script, keys, args)
	if err != nil {
		return nil, err
	}
	reply, err := s.client.Do(ctx, cmd).AsStrSlice()
	if err != nil {
		return nil, mapTimeout(err)
	}
	return reply, nil
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
	if rueidis.IsRedisNil(resps[len(resps)-1].Error()) {
		return errConflict
	}
	return nil
}
