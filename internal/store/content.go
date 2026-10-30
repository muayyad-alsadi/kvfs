package store

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/rueidis"
)

// A file's content is one string at its data key, and its size is that string's
// length. The metadata document does not carry a size, so a write has no second
// record to keep in step: it is one SETRANGE, whose cost is the bytes written
// and not the size of the file.

const (
	// writeAtScript writes a range of the content, but only while the entry's
	// metadata exists. Without that check a write through a handle whose file was
	// unlinked would recreate the content key with nothing owning it. It returns
	// the new length, or -1 when the entry is gone.
	writeAtScript = `
if redis.call('EXISTS', KEYS[1]) == 0 then return -1 end
return redis.call('SETRANGE', KEYS[2], ARGV[1], ARGV[2])
`
	// truncateScript resizes the content. A string cannot shrink in place, so a
	// shorter size rewrites what is kept; a longer one pads with zero bytes.
	truncateScript = `
if redis.call('EXISTS', KEYS[1]) == 0 then return -1 end
local n = tonumber(ARGV[1])
local cur = redis.call('STRLEN', KEYS[2])
if n == 0 then
  redis.call('SET', KEYS[2], '')
elseif n < cur then
  redis.call('SET', KEYS[2], redis.call('GETRANGE', KEYS[2], 0, n - 1))
elseif n > cur then
  redis.call('SETRANGE', KEYS[2], n - 1, '\0')
end
return n
`
)

// Stat reads an entry's metadata and the length of its content in one round
// trip. The length is the file's size; a directory has no content and reports
// zero.
func (s *Store) Stat(ctx context.Context, parentID uint64, name string) (*Meta, error) {
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	got := s.client.DoMulti(ctx,
		s.client.B().Get().Key(s.metaKey(parentID, name)).Build(),
		s.client.B().Strlen().Key(s.dataKey(parentID, name)).Build(),
	)
	return statReply(got[0], got[1])
}

// statReply decodes the replies of a metadata GET and a content STRLEN into
// metadata carrying the size.
func statReply(metaReply, sizeReply rueidis.RedisResult) (*Meta, error) {
	raw, err := metaReply.ToString()
	if err != nil {
		if rueidis.IsRedisNil(err) {
			return nil, ErrNotFound
		}
		return nil, mapTimeout(err)
	}
	size, err := sizeReply.AsInt64()
	if err != nil {
		return nil, mapTimeout(err)
	}
	m, err := UnmarshalMeta(raw)
	if err != nil {
		return nil, err
	}
	m.Size = uint64(size)
	return m, nil
}

// StatRef is Stat for the entry a Ref points at.
func (s *Store) StatRef(ctx context.Context, ref Ref) (*Meta, error) {
	return s.Stat(ctx, ref.ParentID, ref.Name)
}

// ReadAt returns up to n bytes of a file's content from off. A read past the end
// returns nothing.
func (s *Store) ReadAt(ctx context.Context, parentID uint64, name string, off int64, n int) ([]byte, error) {
	if n <= 0 || off < 0 {
		return []byte{}, nil
	}
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	cmd := s.client.B().Getrange().Key(s.dataKey(parentID, name)).Start(off).End(off + int64(n) - 1).Build()
	raw, err := s.client.Do(ctx, cmd).AsBytes()
	if err != nil {
		return nil, mapTimeout(err)
	}
	return raw, nil
}

// WriteAt writes data into a file's content at off, growing it with zero bytes
// if off is past the end. It is one command and touches no metadata: call
// MarkModified when the writer is done.
func (s *Store) WriteAt(ctx context.Context, parentID uint64, name string, off int64, data []byte) error {
	if len(data) == 0 {
		return nil
	}
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	n, err := s.eval(ctx, writeAtScript,
		[]string{s.metaKey(parentID, name), s.dataKey(parentID, name)},
		[]string{strconv.FormatInt(off, 10), string(data)},
	)
	if err != nil {
		return err
	}
	if n < 0 {
		return fmt.Errorf("write %s: %w", name, ErrNotFound)
	}
	return nil
}

// MarkModified stamps a file's modification time and tells other mounts to
// re-read it. Writers call it once when they close or flush, not per write.
func (s *Store) MarkModified(ctx context.Context, parentID uint64, name string) error {
	_, err := s.UpdateMeta(ctx, parentID, name, stampModified(time.Now()))
	return err
}

// Truncate resizes a file, zero-filling when it grows, stamps it modified and
// returns its metadata with the new size.
func (s *Store) Truncate(ctx context.Context, parentID uint64, name string, size uint64) (*Meta, error) {
	if err := s.truncateContent(ctx, parentID, name, size); err != nil {
		return nil, err
	}
	return s.UpdateMeta(ctx, parentID, name, stampModified(time.Now()))
}

func stampModified(now time.Time) func(*Meta) {
	return func(m *Meta) { m.Mtime = now.Unix() }
}

func (s *Store) truncateContent(ctx context.Context, parentID uint64, name string, size uint64) error {
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	n, err := s.eval(ctx, truncateScript,
		[]string{s.metaKey(parentID, name), s.dataKey(parentID, name)},
		[]string{strconv.FormatUint(size, 10)},
	)
	if err != nil {
		return err
	}
	if n < 0 {
		return fmt.Errorf("truncate %s: %w", name, ErrNotFound)
	}
	return nil
}
