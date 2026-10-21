package vr

import (
	"fmt"
	"io"
	"log/slog"
	"testing"
)

// The throughput benchmarks measure only the CPU cost of the protocol. A
// router delivers each message at once, in memory, with no network, no
// serialization and no ticks. A real node is slower than this.

// router delivers messages between replicas in FIFO order.
type router struct {
	replicas []*Replica
	queue    []Outgoing
	replies  []Outgoing
}

func (rt *router) drain() {
	for len(rt.queue) > 0 {
		o := rt.queue[0]
		rt.queue = rt.queue[1:]
		if o.To.Type == CorrespondentClient {
			rt.replies = append(rt.replies, o)
			continue
		}
		rt.queue = append(rt.queue, rt.replicas[o.To.Peer].Step(o.Msg)...)
	}
}

func (rt *router) primary() *Replica {
	for _, r := range rt.replicas {
		if r.state.Role == RolePrimary && r.state.Status == StatusNormal {
			return r
		}
	}
	return nil
}

// newBenchCluster ticks a new cluster until it has a primary and all
// replicas are normal in the same view.
func newBenchCluster(b *testing.B, n int) *router {
	members := []ReplicaNumber{}
	for i := range n {
		members = append(members, ReplicaNumber(i))
	}
	rt := &router{}
	for _, id := range members {
		rt.replicas = append(rt.replicas,
			NewReplica(id, members, func(p Payload) Payload { return p }, 1, false))
	}
	for range 1000 {
		for _, r := range rt.replicas {
			rt.queue = append(rt.queue, r.Tick()...)
		}
		rt.drain()
		if p := rt.primary(); p != nil {
			view := p.state.ViewNumber
			ready := true
			for _, r := range rt.replicas {
				ready = ready && r.state.Status == StatusNormal && r.state.ViewNumber == view
			}
			if ready {
				return rt
			}
		}
	}
	b.Fatal("no primary after 1000 ticks")
	return nil
}

// benchThroughput commits b.N requests. Each client has one request in
// flight, so clients is also the number of ops in flight.
func benchThroughput(b *testing.B, replicas, clients, payloadSize int) {
	rt := newBenchCluster(b, replicas)
	primary := rt.primary()
	payload := make(Payload, payloadSize)
	next := make([]RequestNumber, clients)

	send := func(c int) {
		next[c]++
		rt.queue = append(rt.queue, primary.Step(&ClientRequest{
			From: ClientId(c), RequestNumber: next[c], Operation: payload,
		})...)
	}

	b.ReportAllocs()
	b.ResetTimer()
	committed, sent := 0, 0
	for c := range min(clients, b.N) {
		send(c)
		sent++
	}
	for committed < b.N {
		rt.drain()
		if len(rt.replies) == 0 {
			b.Fatalf("no progress after %d commits", committed)
		}
		replies := rt.replies
		rt.replies = nil
		for _, o := range replies {
			committed++
			if sent < b.N {
				send(int(o.To.Client))
				sent++
			}
		}
	}
	b.StopTimer()

	if primary.state.CommitNumber != CommitNumber(b.N) {
		b.Fatalf("commit %d, want %d", primary.state.CommitNumber, b.N)
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/s")
}

func BenchmarkThroughput(b *testing.B) {
	for _, replicas := range []int{3, 5} {
		for _, clients := range []int{1, 10, 100, 1000} {
			b.Run(fmt.Sprintf("replicas=%d/clients=%d", replicas, clients), func(b *testing.B) {
				benchThroughput(b, replicas, clients, 64)
			})
		}
	}
}

// BenchmarkThroughputWithLogging uses a real slog handler, as a node with
// the default logger does. TestMain discards the logs for the other tests.
func BenchmarkThroughputWithLogging(b *testing.B) {
	old := slog.Default()
	defer slog.SetDefault(old)
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))

	benchThroughput(b, 3, 10, 64)
}

