package store_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"

	"kvfs/internal/store"

	"github.com/redis/rueidis"
)

// sent is one command as it left the client; tx is true when it went over a
// dedicated connection: a watched read or a MULTI/EXEC batch.
type sent struct {
	args []string
	tx   bool
}

// recordingClient records every command the store sends, so a test can assert
// what went over the wire (which keys a script names, whether a body moved)
// instead of inferring it from the implementation.
type recordingClient struct {
	rueidis.Client
	mu   sync.Mutex
	cmds []sent
}

func (c *recordingClient) record(tx bool, cmds ...rueidis.Completed) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, cmd := range cmds {
		// rueidis recycles a command's slice once it is sent: copy it now.
		c.cmds = append(c.cmds, sent{args: append([]string(nil), cmd.Commands()...), tx: tx})
	}
}

func (c *recordingClient) Do(ctx context.Context, cmd rueidis.Completed) rueidis.RedisResult {
	c.record(false, cmd)
	return c.Client.Do(ctx, cmd)
}

func (c *recordingClient) DoMulti(ctx context.Context, cmds ...rueidis.Completed) []rueidis.RedisResult {
	c.record(false, cmds...)
	return c.Client.DoMulti(ctx, cmds...)
}

func (c *recordingClient) Dedicated(fn func(rueidis.DedicatedClient) error) error {
	return c.Client.Dedicated(func(d rueidis.DedicatedClient) error {
		return fn(&recordingDedicated{DedicatedClient: d, owner: c})
	})
}

type recordingDedicated struct {
	rueidis.DedicatedClient
	owner *recordingClient
}

func (d *recordingDedicated) Do(ctx context.Context, cmd rueidis.Completed) rueidis.RedisResult {
	d.owner.record(true, cmd)
	return d.DedicatedClient.Do(ctx, cmd)
}

func (d *recordingDedicated) DoMulti(ctx context.Context, cmds ...rueidis.Completed) []rueidis.RedisResult {
	d.owner.record(true, cmds...)
	return d.DedicatedClient.DoMulti(ctx, cmds...)
}

// take returns the commands recorded since the last call and clears the log.
func (c *recordingClient) take() []sent {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := c.cmds
	c.cmds = nil
	return out
}

func newRecordingStore(t testing.TB, level store.Level) (*store.Store, *recordingClient) {
	t.Helper()
	_, client := testClient(t)
	rec := &recordingClient{Client: client}
	return bootstrappedAt(t, rec, level), rec
}

// hashTag returns the text between the first '{' and the next '}' of a key.
func hashTag(key string) string {
	open := strings.IndexByte(key, '{')
	if open < 0 {
		return ""
	}
	end := strings.IndexByte(key[open:], '}')
	if end < 0 {
		return ""
	}
	return key[open+1 : open+end]
}

// keysOf returns the keys one command names, or nil for commands that name none.
func keysOf(args []string) []string {
	if len(args) < 2 {
		return nil
	}
	switch strings.ToUpper(args[0]) {
	case "EVAL":
		if len(args) < 3 {
			panic(fmt.Sprintf("malformed EVAL: %q", args))
		}
		n, err := strconv.Atoi(args[2])
		if err != nil || len(args) < 3+n {
			panic(fmt.Sprintf("malformed EVAL: %q", args))
		}
		return args[3 : 3+n]
	case "MULTI", "EXEC", "PUBLISH", "SUBSCRIBE":
		return nil
	default:
		return args[1:2]
	}
}
