package kvfs

import (
	"context"
	"errors"
	"log"
	"sync/atomic"
	"time"

	"kvfs/internal/store"
)

// graceFactor is how many operation timeouts a pending field must stay
// unchanged before the mount repairs it. A link operation is a handful of
// round trips, each bounded by the timeout, so a field still pending after this
// long belongs to a client that stopped, not to one that is working.
const graceFactor = 10

// Health counts the disagreements between listings and metadata this mount has
// observed. Each counter counts observations, not distinct names.
type Health struct {
	// Unknown is listed fields whose type code was unreadable or pending.
	Unknown uint64
	// Pending is listed fields that a link operation has claimed but not settled.
	Pending uint64
	// Dangling is listed names whose metadata was missing when they were looked up.
	Dangling uint64
	// Repaired is names a repair settled, removed or discarded.
	Repaired uint64
}

type healthCounters struct {
	unknown, pending, dangling, repaired atomic.Uint64
}

// Health returns a snapshot of the diagnostics counters.
func (r *Root) Health() Health {
	return Health{
		Unknown:  r.health.unknown.Load(),
		Pending:  r.health.pending.Load(),
		Dangling: r.health.dangling.Load(),
		Repaired: r.health.repaired.Load(),
	}
}

// repairReq names a pending field exactly as it was seen.
type repairReq struct {
	parent store.Ref
	name   string
	raw    string
}

type repairKey struct {
	parent store.Ref
	name   string
}

type repairSeen struct {
	req repairReq
	at  time.Time
}

// noteListing counts what a listing showed and hands its pending fields to the
// repair loop. It never blocks the request path: a full queue drops the hint,
// and the next listing offers the name again.
func (r *Root) noteListing(dir store.Ref, children []store.Child) {
	for _, c := range children {
		if c.Type == "" {
			r.health.unknown.Add(1)
		}
		if !c.Pending {
			continue
		}
		r.health.pending.Add(1)
		r.enqueueRepair(repairReq{parent: dir, name: c.Name, raw: c.Raw})
	}
}

// noteBusy remembers a name an operation refused as mid-commit, so the retry
// after the grace period finds it settled.
func (r *Root) noteBusy(ctx context.Context, err error, parent store.Ref, name string) {
	if !errors.Is(err, store.ErrBusy) {
		return
	}
	raw, ferr := r.store.FieldValue(ctx, parent, name)
	if ferr != nil {
		return
	}
	r.enqueueRepair(repairReq{parent: parent, name: name, raw: raw})
}

func (r *Root) enqueueRepair(req repairReq) {
	select {
	case r.repairCh <- req:
	default:
	}
}

// repairLoop repairs pending fields that stayed unchanged for the grace period.
// It is the only place the mount repairs a name that still resolves.
func (r *Root) repairLoop(ctx context.Context) {
	grace := r.grace
	tick := time.NewTicker(grace / 2)
	defer tick.Stop()
	seen := map[repairKey]repairSeen{}
	for {
		select {
		case <-ctx.Done():
			return
		case req := <-r.repairCh:
			k := repairKey{req.parent, req.name}
			if cur, ok := seen[k]; !ok || cur.req.raw != req.raw {
				seen[k] = repairSeen{req: req, at: time.Now()}
			}
		case now := <-tick.C:
			for k, e := range seen {
				if now.Sub(e.at) < grace {
					continue
				}
				delete(seen, k)
				r.repairOne(ctx, e.req)
			}
		}
	}
}

func (r *Root) repairOne(ctx context.Context, req repairReq) {
	res, err := r.store.RepairPending(ctx, req.parent, req.name, req.raw)
	if err != nil {
		log.Printf("kvfs: repair %d/%s: %v", req.parent.ID, req.name, err)
		return
	}
	if res != store.RepairNone {
		r.health.repaired.Add(1)
	}
}

// repairMiss resolves a name whose lookup missed. A name that is not listed
// costs one field read. A settled field with no metadata is a ghost and is
// removed before the miss is answered. A pending field may belong to a create
// that has claimed the name and not yet written it, so it waits for the repair
// loop instead. A namespace-level store writes every entry in one script, so
// a miss there is just a miss and costs nothing more.
func (r *Root) repairMiss(ctx context.Context, parent store.Ref, name string) {
	if !r.store.Linked() {
		return
	}
	raw, err := r.store.FieldValue(ctx, parent, name)
	if err != nil {
		return
	}
	if store.ParseChild(name, raw).Pending {
		r.enqueueRepair(repairReq{parent: parent, name: name, raw: raw})
		return
	}
	res, err := r.store.RepairEntry(ctx, parent, name)
	if err != nil || res == store.RepairNone {
		return
	}
	r.health.dangling.Add(1)
	r.health.repaired.Add(1)
}
