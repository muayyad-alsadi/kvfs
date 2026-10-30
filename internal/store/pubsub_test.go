package store_test

import (
	"context"
	"reflect"
	"runtime"
	"testing"
	"time"

	"kvfs/internal/store"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/rueidis"
)

const sentinelName = "sentinel"

// subscriber receives what the store publishes on the namespace channel, through
// a second client, the way a mount on another host would.
type subscriber struct {
	t  *testing.T
	st *store.Store
	ch chan string
}

func newSubscriber(t *testing.T, mr *miniredis.Miniredis, st *store.Store) *subscriber {
	t.Helper()
	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress:       []string{mr.Addr()},
		DisableCache:      true,
		ForceSingleClient: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	sub := &subscriber{t: t, st: st, ch: make(chan string, 64)}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = client.Receive(ctx, client.B().Subscribe().Channel(st.Channel()).Build(), func(m rueidis.PubSubMessage) {
			sub.ch <- m.Message
		})
	}()
	t.Cleanup(func() {
		cancel()
		<-done
		client.Close()
	})
	deadline := time.Now().Add(5 * time.Second)
	for mr.PubSubNumSub(st.Channel())[st.Channel()] < 1 {
		if time.Now().After(deadline) {
			t.Fatal("the subscriber never attached")
		}
		runtime.Gosched()
	}
	return sub
}

// payloads publishes a sentinel after the mutation and collects everything the
// subscriber saw before it. Messages arrive in order, so anything the mutation
// published beyond what the caller expects shows up in the result.
func (s *subscriber) payloads() []string {
	s.t.Helper()
	if err := s.st.Publish(context.Background(), 0, sentinelName); err != nil {
		s.t.Fatal(err)
	}
	want := store.InvalidatePayload(0, sentinelName)
	var got []string
	for {
		select {
		case m := <-s.ch:
			if m == want {
				return got
			}
			got = append(got, m)
		case <-time.After(5 * time.Second):
			s.t.Fatalf("no sentinel; saw %v", got)
		}
	}
}

func TestEveryMutationPublishesItsAddresses(t *testing.T) {
	for _, level := range store.Levels() {
		t.Run(string(level), func(t *testing.T) {
			st, mr := newTestStoreAt(t, level)
			sub := newSubscriber(t, mr, st)
			ctx := context.Background()
			root := store.RootRef()

			steps := []struct {
				name string
				op   func() error
				want []string
			}{
				{"create", func() error { _, _, err := st.CreateFile(ctx, root, "a b", 0644, 1, 1); return err }, []string{"1/a b"}},
				{"mkdir", func() error { _, _, err := st.Mkdir(ctx, root, "d", 0755, 1, 1); return err }, []string{"1/d"}},
				{"symlink", func() error { _, _, err := st.Symlink(ctx, root, "l", "a b", 1, 1); return err }, []string{"1/l"}},
				{"write", func() error { _, err := st.WriteData(ctx, store.RootID, "a b", []byte("x"), true); return err }, []string{"1/a b"}},
				{"setattr", func() error {
					_, err := st.UpdateMeta(ctx, store.RootID, "a b", func(m *store.Meta) { m.Mode = 0600 })
					return err
				}, []string{"1/a b"}},
				{"rename publishes old then new", func() error { _, err := st.Rename(ctx, root, "a b", root, "c/d"); return err }, []string{"1/a b", "1/c%2Fd"}},
				{"unlink", func() error { return st.Unlink(ctx, root, "c/d") }, []string{"1/c%2Fd"}},
				{"unlink of the link", func() error { return st.Unlink(ctx, root, "l") }, []string{"1/l"}},
				{"rmdir", func() error { return st.Rmdir(ctx, root, "d") }, []string{"1/d"}},
				{"a refused create publishes nothing", func() error {
					if _, _, err := st.CreateFile(ctx, root, "x", 0644, 1, 1); err != nil {
						return err
					}
					sub.payloads()
					if _, _, err := st.CreateFile(ctx, root, "x", 0644, 1, 1); err == nil {
						t.Fatal("duplicate create succeeded")
					}
					return nil
				}, nil},
			}
			for _, tc := range steps {
				if err := tc.op(); err != nil {
					t.Fatalf("%s: %v", tc.name, err)
				}
				if got := sub.payloads(); !reflect.DeepEqual(got, tc.want) {
					t.Fatalf("%s published %q, want %q", tc.name, got, tc.want)
				}
			}
		})
	}
}
