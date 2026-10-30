package store

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/redis/rueidis"
)

// At `dir` and `file` level a link operation (create, unlink, rmdir, rename)
// writes the parent's listing in one shard and the entry's own keys in another,
// so none of them can be a single script. These helpers run the intent protocol
// of SPEC.md: the parent's listing is the commit point, its value carries a
// pending marker until the entry's shard is written, and a reader treats a
// marked field as an unknown type instead of trusting it.
//
// Every step that changes a field is a compare-and-set on the value it expects,
// so a client that loses a race cannot overwrite the winner's entry.

const (
	// claimScript inserts a pending field only if the name is free. The insert
	// is what decides an exclusive create: two creators cannot both win. KEYS[2]
	// is the parent directory's metadata, which shares a slot with its listing
	// at every level, and ARGV[3] the inode the caller believes the parent has.
	// A directory removed by rmdir has no metadata, and one replaced by a rename
	// has a different inode, so a late creator is refused instead of writing
	// keys under a directory that is gone; an early one makes rmdir see a
	// non-empty listing.
	claimScript = `
local m = redis.call('GET', KEYS[2])
if not m or string.match(m, '^{"id":(%d+)[,}]') ~= ARGV[3] then return -1 end
if redis.call('HEXISTS', KEYS[1], ARGV[1]) == 1 then return 0 end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
return 1
`
	// settleScript replaces a pending field with its final value and announces
	// it, but only while it still holds the value this writer inserted. The
	// channel travels as an argument, never as a key: it lives in the namespace
	// shard, and a script may only name keys from one slot.
	settleScript = `
if redis.call('HGET', KEYS[1], ARGV[1]) ~= ARGV[2] then return 0 end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[3])
redis.call('PUBLISH', ARGV[4], ARGV[5])
return 1
`
	// deleteFieldScript removes a field and announces the name, but only while
	// it still holds the value the caller decided on.
	deleteFieldScript = `
if redis.call('HGET', KEYS[1], ARGV[1]) ~= ARGV[2] then return 0 end
redis.call('HDEL', KEYS[1], ARGV[1])
redis.call('PUBLISH', ARGV[3], ARGV[4])
return 1
`
	// takeoverScript overwrites a settled field with this writer's pending
	// value, but only while it still holds the value that was inspected. It is
	// how a rename claims a destination name that is already taken.
	takeoverScript = `
if redis.call('HGET', KEYS[1], ARGV[1]) ~= ARGV[2] then return 0 end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[3])
return 1
`
	// renameWithinListingScript is the commit of a rename inside one parent,
	// where the source field and the destination field are in the same hash. It
	// removes the source and settles the destination as one step, so the state
	// between the two writes does not exist. It returns 0 when the source field
	// is no longer the value the caller decided on, and 2 when the destination
	// no longer holds the caller's pending value.
	renameWithinListingScript = `
if redis.call('HGET', KEYS[1], ARGV[1]) ~= ARGV[2] then return 0 end
if redis.call('HGET', KEYS[1], ARGV[3]) ~= ARGV[4] then return 2 end
redis.call('HDEL', KEYS[1], ARGV[1])
redis.call('HSET', KEYS[1], ARGV[3], ARGV[5])
redis.call('PUBLISH', ARGV[6], ARGV[7])
redis.call('PUBLISH', ARGV[6], ARGV[8])
return 1
`
	// releaseScript withdraws a claim that was never announced.
	releaseScript = `
if redis.call('HGET', KEYS[1], ARGV[1]) ~= ARGV[2] then return 0 end
redis.call('HDEL', KEYS[1], ARGV[1])
return 1
`
	// clearDirectoryScript deletes a directory's own keys atomically with the
	// emptiness test, so a child cannot be added between the count and the
	// delete on this shard.
	clearDirectoryScript = `
if redis.call('HLEN', KEYS[2]) > 0 then return 0 end
redis.call('DEL', KEYS[1])
redis.call('DEL', KEYS[2])
return 1
`
	// moveScript renames an entry's own keys inside one shard. It clears the
	// destination's keys first: the destination name is already ours, and a
	// replaced entry must not leave a stale data or listing key behind. It is the `dir`
	// level rename within one parent: the parent id is the tag, so the source
	// and destination keys share a slot and no byte is read or rewritten.
	moveScript = `
if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
redis.call('DEL', KEYS[4], KEYS[5], KEYS[6])
redis.call('RENAME', KEYS[1], KEYS[4])
if redis.call('EXISTS', KEYS[2]) == 1 then redis.call('RENAME', KEYS[2], KEYS[5]) end
if redis.call('EXISTS', KEYS[3]) == 1 then redis.call('RENAME', KEYS[3], KEYS[6]) end
return 1
`
)

// Repair names what RepairEntry did to one listed name.
type Repair string

// The outcomes of a repair.
const (
	// RepairNone means the field and the metadata already agreed.
	RepairNone Repair = "none"
	// RepairSettled means a pending field was rewritten from complete metadata.
	RepairSettled Repair = "settled"
	// RepairRemoved means a field whose metadata is missing was deleted.
	RepairRemoved Repair = "removed"
	// RepairDiscarded means a half-finished rename was undone: the source still
	// owned the inode, so the destination copy and its field were deleted.
	RepairDiscarded Repair = "discarded"
)

// listingKey is the parent's hash: a directory's children, addressed by the
// directory's own location.
func (s *Store) listingKey(parent Ref) string {
	return s.dirKey(parent.ParentID, parent.Name)
}

// nextID allocates the entry's inode. The counter lives in the namespace shard,
// which is a different shard from a non-root entry, so this cannot ride inside
// the commit script.
func (s *Store) nextID(ctx context.Context) (uint64, error) {
	n, err := s.client.Do(ctx, s.client.B().Incr().Key(s.counterKey()).Build()).AsInt64()
	if err != nil {
		return 0, mapTimeout(err)
	}
	return uint64(n), nil
}

// field reads one listing field and decodes it. A pending field decodes to an
// unknown type, which is how the rest of the store learns it is mid-commit.
func (s *Store) field(ctx context.Context, parent Ref, name string) (string, Child, error) {
	raw, err := s.readString(ctx, s.client.B().Hget().Key(s.listingKey(parent)).Field(name).Build())
	if err != nil {
		return "", Child{}, err
	}
	return raw, ParseChild(name, raw), nil
}

// settledType answers the type check from the listing's immutable code. A
// pending field is another client's unfinished commit, so it is refused; a
// field with an unreadable code falls back to the metadata.
func (s *Store) settledType(ctx context.Context, parent Ref, name string, child Child) (string, error) {
	if child.Pending {
		return "", fmt.Errorf("%s: %w", name, ErrBusy)
	}
	if child.Type != "" {
		return child.Type, nil
	}
	m, err := s.GetMeta(ctx, parent.ID, name)
	if err != nil {
		return "", err
	}
	return m.Type, nil
}

// claim inserts the pending field that reserves a name. It reports false when
// the name is taken and ErrNotFound when the parent directory is gone.
func (s *Store) claim(ctx context.Context, parent Ref, name, pending string) (bool, error) {
	n, err := s.eval(ctx, claimScript,
		[]string{s.listingKey(parent), s.metaKey(parent.ParentID, parent.Name)},
		[]string{name, pending, strconv.FormatUint(parent.ID, 10)},
	)
	if err != nil {
		return false, err
	}
	if n < 0 {
		return false, fmt.Errorf("claim %s: the parent directory was removed: %w", name, ErrNotFound)
	}
	return n == 1, nil
}

// settle announces a committed entry by replacing its pending field. It returns
// false when the field no longer holds the pending value, which means another
// client resolved the name first.
func (s *Store) settle(ctx context.Context, parent Ref, name, expected string, child Child) (bool, error) {
	n, err := s.eval(ctx, settleScript,
		[]string{s.listingKey(parent)},
		[]string{name, expected, child.Value(), s.Channel(), InvalidatePayload(parent.ID, name)},
	)
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// release withdraws a claim that was never settled, and never publishes. A
// failure leaves a pending field behind, which RepairEntry removes.
func (s *Store) release(ctx context.Context, parent Ref, name, expected string) {
	_, _ = s.eval(ctx, releaseScript,
		[]string{s.listingKey(parent)},
		[]string{name, expected},
	)
}

// deleteField removes a listing field and announces the name. It returns false
// when the field no longer holds the value the caller decided on.
func (s *Store) deleteField(ctx context.Context, parent Ref, name, expected string) (bool, error) {
	n, err := s.eval(ctx, deleteFieldScript,
		[]string{s.listingKey(parent)},
		[]string{name, expected, s.Channel(), InvalidatePayload(parent.ID, name)},
	)
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// commitDeletion deletes the field as the commit point of an unlink or rmdir.
func (s *Store) commitDeletion(ctx context.Context, parent Ref, name, expected string) error {
	ok, err := s.deleteField(ctx, parent, name, expected)
	if err != nil {
		return err
	}
	if !ok {
		// A concurrent rename moved the name while its keys were being removed.
		return ErrNotFound
	}
	return nil
}

// deleteEntryKeys removes the entry's own records in one command: they share a
// slot, and one DEL is applied whole or not at all, so a crash cannot leave the
// metadata gone and the content behind. The field is deleted after this, so a
// crash in between leaves a listed name with no metadata, which is the
// repairable state the protocol defines.
func (s *Store) deleteEntryKeys(ctx context.Context, owner uint64, name string, isDir bool) error {
	content := SuffixData
	if isDir {
		content = SuffixDir
	}
	cmd := s.client.B().Del().Key(s.key(owner, name, SuffixMeta), s.key(owner, name, content)).Build()
	if err := s.client.Do(ctx, cmd).Error(); err != nil {
		return mapTimeout(err)
	}
	return nil
}

// discard undoes a destination that was claimed or copied but never settled:
// its keys go first, then its field, so a crash in between leaves the
// repairable "field without metadata" state.
func (s *Store) discard(ctx context.Context, parent Ref, name, pending string, isDir bool) error {
	if err := s.deleteEntryKeys(ctx, parent.ID, name, isDir); err != nil {
		return err
	}
	s.release(ctx, parent, name, pending)
	return nil
}

// createLinked runs create as claim -> prepare -> settle. The claim goes first
// because it is the exclusive decision: a loser writes nothing, so it can never
// overwrite the winner's metadata. The cost is a spent inode number, since the
// allocator lives in the namespace shard and cannot be checked in the same
// script as the name.
func (s *Store) createLinked(ctx context.Context, parent Ref, name string, meta *Meta, data []byte, writeData bool) (*Meta, Ref, error) {
	id, err := s.nextID(ctx)
	if err != nil {
		return nil, Ref{}, err
	}
	meta.ID = id
	child := Child{Type: meta.Type, ID: id}
	pending := child.PendingValue()

	claimed, err := s.claim(ctx, parent, name, pending)
	if err != nil {
		return nil, Ref{}, err
	}
	if !claimed {
		return nil, Ref{}, ErrExist
	}
	if err := s.writeEntry(ctx, parent.ID, name, meta, data, writeData, false); err != nil {
		s.release(ctx, parent, name, pending)
		return nil, Ref{}, err
	}
	settled, err := s.settle(ctx, parent, name, pending, child)
	if err != nil {
		return nil, Ref{}, err
	}
	if !settled {
		return nil, Ref{}, fmt.Errorf("create %s: the claim was resolved by another client: %w", name, ErrNotFound)
	}
	return meta, Ref{ParentID: parent.ID, Name: name, ID: id}, nil
}

func (s *Store) unlinkLinked(ctx context.Context, parent Ref, name string) error {
	raw, child, err := s.field(ctx, parent, name)
	if err != nil {
		return err
	}
	typ, err := s.settledType(ctx, parent, name, child)
	if err != nil {
		return err
	}
	if typ == TypeDir {
		return ErrIsDir
	}
	if err := s.deleteEntryKeys(ctx, parent.ID, name, false); err != nil {
		return err
	}
	return s.commitDeletion(ctx, parent, name, raw)
}

func (s *Store) rmdirLinked(ctx context.Context, parent Ref, name string) error {
	raw, child, err := s.field(ctx, parent, name)
	if err != nil {
		return err
	}
	typ, err := s.settledType(ctx, parent, name, child)
	if err != nil {
		return err
	}
	if typ != TypeDir {
		return ErrNotDir
	}
	empty, err := s.clearDirectory(ctx, parent.ID, name)
	if err != nil {
		return err
	}
	if !empty {
		return ErrNotEmpty
	}
	return s.commitDeletion(ctx, parent, name, raw)
}

// clearDirectory removes a directory's own records only when its listing is
// empty, both in one script on the directory's shard.
func (s *Store) clearDirectory(ctx context.Context, owner uint64, name string) (bool, error) {
	n, err := s.eval(ctx, clearDirectoryScript,
		[]string{s.metaKey(owner, name), s.dirKey(owner, name)},
		nil,
	)
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// renameLinked claims the destination, carries the entry over, removes the
// source field and settles the destination. The source field is the commit
// point: until it is gone the rename can still be undone, and once it is gone
// the destination is the only owner of the inode even if the process stops
// before the settle. The destination's marker names the source, so repair can
// tell those two states apart.
func (s *Store) renameLinked(ctx context.Context, oldParent Ref, oldName string, newParent Ref, newName string, mode renameMode) (uint64, error) {
	raw, child, err := s.field(ctx, oldParent, oldName)
	if err != nil {
		return 0, err
	}
	if child.Type, err = s.settledType(ctx, oldParent, oldName, child); err != nil {
		return 0, err
	}
	pending := child.MovePendingValue(InvalidatePayload(oldParent.ID, oldName))
	if err := s.claimDestination(ctx, newParent, newName, pending, child.Type, mode); err != nil {
		return 0, err
	}
	if err := s.carry(ctx, oldParent, oldName, newParent, newName, child, pending); err != nil {
		return 0, err
	}
	if oldParent.ID == newParent.ID {
		return child.ID, s.commitMoveWithinParent(ctx, oldParent, oldName, raw, newName, pending, child)
	}
	return child.ID, s.commitMove(ctx, oldParent, oldName, raw, newParent, newName, pending, child)
}

// claimDestination reserves the destination name. A free name is claimed; a
// taken one is taken over when the mode and the types allow it. A crash after
// a takeover leaves the replaced entry's keys behind a pending field, and
// repair discards them, so the replaced entry is gone even though the rename
// did not finish: the same end state a finished rename would have produced for
// that name.
func (s *Store) claimDestination(ctx context.Context, parent Ref, name, pending, srcType string, mode renameMode) error {
	claimed, err := s.claim(ctx, parent, name, pending)
	if err != nil {
		return err
	}
	if claimed {
		return nil
	}
	if mode == refuseExisting {
		return ErrExist
	}
	return s.takeOver(ctx, parent, name, pending, srcType)
}

// takeOver replaces a settled destination field after the POSIX type and
// emptiness checks. A pending destination belongs to another client and is
// refused like any taken name.
func (s *Store) takeOver(ctx context.Context, parent Ref, name, pending, srcType string) error {
	raw, dest, err := s.field(ctx, parent, name)
	if errors.Is(err, ErrNotFound) {
		return ErrExist // freed while we looked: the caller may simply retry
	}
	if err != nil {
		return err
	}
	if dest.Pending {
		return ErrExist
	}
	destIsDir, err := s.checkReplaceable(ctx, parent, name, dest, srcType)
	if err != nil {
		return err
	}
	if destIsDir {
		if err := s.removeEmptyDirectory(ctx, parent, name); err != nil {
			return err
		}
	}
	n, err := s.eval(ctx, takeoverScript, []string{s.listingKey(parent)}, []string{name, raw, pending})
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrExist
	}
	return nil
}

// checkReplaceable applies the type pairing rules of rename(2) and reports
// whether the destination is a directory, which must then be emptied.
func (s *Store) checkReplaceable(ctx context.Context, parent Ref, name string, dest Child, srcType string) (bool, error) {
	destType, err := s.settledType(ctx, parent, name, dest)
	if err != nil {
		return false, err
	}
	switch {
	case destType == TypeDir && srcType != TypeDir:
		return false, ErrIsDir
	case destType != TypeDir && srcType == TypeDir:
		return false, ErrNotDir
	}
	return destType == TypeDir, nil
}

// removeEmptyDirectory deletes a destination directory's own keys atomically
// with the emptiness test. Once they are gone a creator into it is refused by
// the claim, so nothing can be written under the replaced directory. If the
// takeover then loses its compare-and-set, the field is left pointing at no
// metadata, which repair removes.
func (s *Store) removeEmptyDirectory(ctx context.Context, parent Ref, name string) error {
	empty, err := s.clearDirectory(ctx, parent.ID, name)
	if err != nil {
		return err
	}
	if !empty {
		return ErrNotEmpty
	}
	return nil
}

// carry moves the entry's own records to the destination and removes the
// source's. A failure before the source is touched undoes the destination; a
// failure after it leaves the pending field for repair, which settles it.
func (s *Store) carry(ctx context.Context, oldParent Ref, oldName string, newParent Ref, newName string, child Child, pending string) error {
	isDir := child.Type == TypeDir
	if s.level == LevelDir && oldParent.ID == newParent.ID {
		err := s.moveEntry(ctx, oldParent.ID, oldName, newName)
		if errors.Is(err, ErrNotFound) {
			s.release(ctx, newParent, newName, pending)
		}
		return err
	}
	if err := s.copyEntry(ctx, oldParent.ID, oldName, newParent.ID, newName, child); err != nil {
		_ = s.discard(ctx, newParent, newName, pending, isDir)
		return err
	}
	return s.deleteEntryKeys(ctx, oldParent.ID, oldName, isDir)
}

// commitMove removes the source field and settles the destination. A source
// field that changed means another client took the name first, so this rename
// loses and its destination is undone.
func (s *Store) commitMove(ctx context.Context, oldParent Ref, oldName, raw string, newParent Ref, newName, pending string, child Child) error {
	won, err := s.deleteField(ctx, oldParent, oldName, raw)
	if err != nil {
		return err
	}
	if !won {
		_ = s.discard(ctx, newParent, newName, pending, child.Type == TypeDir)
		return fmt.Errorf("rename %s: the source was taken by another client: %w", oldName, ErrNotFound)
	}
	settled, err := s.settle(ctx, newParent, newName, pending, child)
	if err != nil {
		return err
	}
	if !settled {
		return fmt.Errorf("rename to %s: the claim was resolved by another client: %w", newName, ErrNotFound)
	}
	return nil
}

// commitMoveWithinParent finishes a rename inside one parent with a single
// script, because both fields live in the parent's one listing. It reports the
// same two losses as commitMove: a source that changed under it undoes the
// destination, and a destination that was resolved by someone else is an error.
func (s *Store) commitMoveWithinParent(ctx context.Context, parent Ref, oldName, raw, newName, pending string, child Child) error {
	n, err := s.eval(ctx, renameWithinListingScript,
		[]string{s.listingKey(parent)},
		[]string{
			oldName, raw, newName, pending, child.Value(), s.Channel(),
			InvalidatePayload(parent.ID, oldName), InvalidatePayload(parent.ID, newName),
		},
	)
	if err != nil {
		return err
	}
	switch n {
	case 1:
		return nil
	case 0:
		_ = s.discard(ctx, parent, newName, pending, child.Type == TypeDir)
		return fmt.Errorf("rename %s: the source was taken by another client: %w", oldName, ErrNotFound)
	default:
		return fmt.Errorf("rename to %s: the claim was resolved by another client: %w", newName, ErrNotFound)
	}
}

// moveEntry renames an entry's own keys inside one shard.
func (s *Store) moveEntry(ctx context.Context, owner uint64, srcName, dstName string) error {
	n, err := s.eval(ctx, moveScript, []string{
		s.metaKey(owner, srcName), s.dataKey(owner, srcName), s.dirKey(owner, srcName),
		s.metaKey(owner, dstName), s.dataKey(owner, dstName), s.dirKey(owner, dstName),
	}, nil)
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// copyEntry duplicates an entry's own records into the destination shard. It
// reads everything before it writes anything, so a missing source is detected
// while the destination is still empty. A directory copies its listing, and its
// children stay where they are: they are addressed by the directory's inode.
func (s *Store) copyEntry(ctx context.Context, srcOwner uint64, srcName string, dstOwner uint64, dstName string, child Child) error {
	reads := []rueidis.Completed{s.client.B().Get().Key(s.metaKey(srcOwner, srcName)).Build()}
	if child.Type == TypeDir {
		reads = append(reads, s.client.B().Hgetall().Key(s.dirKey(srcOwner, srcName)).Build())
	} else {
		reads = append(reads, s.client.B().Get().Key(s.dataKey(srcOwner, srcName)).Build())
	}
	got := s.client.DoMulti(ctx, reads...)

	metaRaw, err := got[0].ToString()
	if err != nil {
		if rueidis.IsRedisNil(err) {
			return ErrNotFound
		}
		return mapTimeout(err)
	}
	if child.Type == TypeDir {
		return s.copyDirectory(ctx, dstOwner, dstName, metaRaw, got[1])
	}
	return s.copyFile(ctx, dstOwner, dstName, metaRaw, got[1])
}

func (s *Store) copyFile(ctx context.Context, dstOwner uint64, dstName, metaRaw string, data rueidis.RedisResult) error {
	cmds := []rueidis.Completed{s.client.B().Set().Key(s.metaKey(dstOwner, dstName)).Value(metaRaw).Build()}
	if raw, err := data.AsBytes(); err == nil {
		cmds = append(cmds, s.client.B().Set().Key(s.dataKey(dstOwner, dstName)).Value(rueidis.BinaryString(raw)).Build())
	} else if rueidis.IsRedisNil(err) {
		// An empty source has no data key; a replaced destination's must not survive.
		cmds = append(cmds, s.client.B().Del().Key(s.dataKey(dstOwner, dstName)).Build())
	} else {
		return mapTimeout(err)
	}
	return s.writeAtomic(ctx, cmds)
}

func (s *Store) copyDirectory(ctx context.Context, dstOwner uint64, dstName, metaRaw string, listing rueidis.RedisResult) error {
	fields, err := listing.AsStrMap()
	if err != nil {
		return mapTimeout(err)
	}
	cmds := []rueidis.Completed{
		s.client.B().Set().Key(s.metaKey(dstOwner, dstName)).Value(metaRaw).Build(),
		s.client.B().Del().Key(s.dataKey(dstOwner, dstName)).Build(), // a replaced file's content
	}
	if len(fields) > 0 {
		// One HSET carries the whole listing, so a reader never sees a partial
		// copy: the destination field is still pending until the copy is done.
		names := make([]string, 0, len(fields))
		for name := range fields {
			names = append(names, name)
		}
		sort.Strings(names)
		hs := s.client.B().Hset().Key(s.dirKey(dstOwner, dstName)).FieldValue()
		for _, name := range names {
			hs = hs.FieldValue(name, fields[name])
		}
		cmds = append(cmds, hs.Build())
	}
	return s.writeAtomic(ctx, cmds)
}

// writeAtomic writes a set of keys that share one shard in one transaction.
func (s *Store) writeAtomic(ctx context.Context, cmds []rueidis.Completed) error {
	return s.dedicated(ctx, func(c rueidis.DedicatedClient) error {
		return s.exec(ctx, c, cmds...)
	})
}

// RepairEntry is the repair pass of the link protocol, applied to one named
// entry. A field whose metadata is missing is removed. A pending field whose
// metadata is complete is settled, unless it is the destination of a rename
// whose source still owns the inode: that rename never took the source, so the
// destination copy is discarded instead and the name keeps one owner. Any other
// field that disagrees with the metadata is rewritten from the metadata. It
// reads only that entry and is idempotent.
func (s *Store) RepairEntry(ctx context.Context, parent Ref, name string) (Repair, error) {
	if !s.Linked() {
		// Namespace-level mutations are atomic; there is no half-written state.
		return RepairNone, nil
	}
	ctx, cancel := s.ctx(ctx)
	defer cancel()

	raw, child, err := s.field(ctx, parent, name)
	if errors.Is(err, ErrNotFound) {
		return RepairNone, nil
	}
	if err != nil {
		return RepairNone, err
	}
	meta, err := s.GetMeta(ctx, parent.ID, name)
	if errors.Is(err, ErrNotFound) {
		return s.removeField(ctx, parent, name, raw)
	}
	if err != nil {
		return RepairNone, err
	}
	want := Child{Type: meta.Type, ID: meta.ID}
	if raw == want.Value() {
		return RepairNone, nil
	}
	return s.settleOrDiscard(ctx, parent, name, raw, child, want)
}

// removeField drops a name that points at nothing.
func (s *Store) removeField(ctx context.Context, parent Ref, name, raw string) (Repair, error) {
	removed, err := s.deleteField(ctx, parent, name, raw)
	if err != nil || !removed {
		return RepairNone, err
	}
	return RepairRemoved, nil
}

// settleOrDiscard resolves a field that disagrees with complete metadata.
func (s *Store) settleOrDiscard(ctx context.Context, parent Ref, name, raw string, child, want Child) (Repair, error) {
	if child.Pending && s.sourceStillOwns(ctx, child.Src, want.ID) {
		if err := s.discard(ctx, parent, name, raw, want.Type == TypeDir); err != nil {
			return RepairNone, err
		}
		return RepairDiscarded, nil
	}
	settled, err := s.settle(ctx, parent, name, raw, want)
	if err != nil || !settled {
		return RepairNone, err
	}
	return RepairSettled, nil
}

// sourceStillOwns reports whether the rename source address still holds the
// metadata of inode id. An empty or unreadable address is no source.
func (s *Store) sourceStillOwns(ctx context.Context, src string, id uint64) bool {
	if src == "" {
		return false
	}
	parentID, name, err := ParseInvalidate(src)
	if err != nil {
		return false
	}
	m, err := s.GetMeta(ctx, parentID, name)
	return err == nil && m.ID == id
}

// RepairPending repairs a name only if its field still holds the pending value
// the caller saw. A caller that waits out a grace period before calling cannot
// undo a link operation that is still running.
func (s *Store) RepairPending(ctx context.Context, parent Ref, name, seen string) (Repair, error) {
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	raw, _, err := s.field(ctx, parent, name)
	if errors.Is(err, ErrNotFound) || (err == nil && raw != seen) {
		return RepairNone, nil
	}
	if err != nil {
		return RepairNone, err
	}
	return s.RepairEntry(ctx, parent, name)
}

// PathRepair is the outcome of repairing one component of a path.
type PathRepair struct {
	Path   string
	Result Repair
}

// RepairPath repairs every component of a path from the root down, so an
// operator can resolve a pending or dangling name without writing Go. Repairing
// a component can remove it; the walk then ends cleanly.
func (s *Store) RepairPath(ctx context.Context, path string) ([]PathRepair, error) {
	parent := RootRef()
	var out []PathRepair
	walked := ""
	for _, name := range strings.FieldsFunc(path, func(r rune) bool { return r == '/' }) {
		walked += "/" + name
		res, err := s.RepairEntry(ctx, parent, name)
		if err != nil {
			return out, fmt.Errorf("repair %s: %w", walked, err)
		}
		out = append(out, PathRepair{Path: walked, Result: res})
		meta, err := s.GetMeta(ctx, parent.ID, name)
		if errors.Is(err, ErrNotFound) && res != RepairNone {
			return out, nil
		}
		if err != nil {
			return out, fmt.Errorf("repair %s: %w", walked, err)
		}
		parent = Ref{ParentID: parent.ID, Name: name, ID: meta.ID}
	}
	return out, nil
}
