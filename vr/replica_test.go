package vr

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"testing"
)

type inFlight struct {
	deliverAt int
	from      Correspondent
	out       Outgoing
}

func (i *inFlight) toIncoming() Incoming {
	return Incoming{
		From: i.from,
		Req:  i.out.Req,
	}
}

type Sim struct {
	t        *testing.T
	seed     uint64
	rand     *rand.Rand
	now      int
	replicas []*Replica
	down     map[ReplicaNumber]bool
	network  []inFlight

	executed map[ReplicaNumber][]Payload
	replies  []*ProcessReply
}

func NewSim(t *testing.T, memberCount int, seed uint64) *Sim {
	t.Helper()

	s := &Sim{
		t:        t,
		seed:     seed,
		rand:     rand.New(rand.NewPCG(seed, 0)),
		down:     map[ReplicaNumber]bool{},
		executed: map[ReplicaNumber][]Payload{},
	}

	config := make([]ReplicaConfiguration, memberCount)
	for i := range memberCount {
		config[i] = ReplicaConfiguration{
			ID:       ReplicaNumber(i),
			Hostname: fmt.Sprintf("%d", i),
		}
	}

	for i := range memberCount {
		id := ReplicaNumber(i)
		upCall := func(op Payload) Payload {
			s.executed[id] = append(s.executed[id], op)
			return op
		}
		s.replicas = append(s.replicas, NewReplica(id, config, upCall, seed+1))
	}

	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("sim seed %d, failed at tick %d", s.seed, s.now)
		}
	})

	return s
}

func (s *Sim) getPeer(id ReplicaNumber) *Replica {
	for _, peer := range s.replicas {
		if peer.state.ReplicaNumber == id {
			return peer
		}
	}

	return nil
}

func (s *Sim) getPrimary() *Replica {
	for _, peer := range s.replicas {
		if peer.state.Role == RolePrimary {
			return peer
		}
	}

	return nil
}

func (s *Sim) stop(id ReplicaNumber) {
	s.down[id] = true
}

func (s *Sim) proceed(id ReplicaNumber) {
	delete(s.down, id)
}

type Condition func() bool

func (s *Sim) sendNextTick(from Correspondent, outgoing []Outgoing) {
	for _, msg := range outgoing {
		s.network = append(s.network, inFlight{
			deliverAt: s.now + 1,
			from:      from,
			out:       msg,
		})
	}
}

func (s *Sim) Send(from, to Correspondent, req Request, deliveryAt int) {
	s.network = append(s.network, inFlight{
		deliverAt: deliveryAt,
		from:      from,
		out: Outgoing{
			To:  to,
			Req: req,
		},
	})
}

func (s *Sim) deliver() {
	s.t.Helper()

	newMsgs := []inFlight{}

	i := 0
	for i = 0; i < len(s.network); i++ {
		msg := s.network[i]
		if msg.deliverAt > s.now {
			break
		}

		if msg.out.To.Type == CorrespondentPeer {
			peer := s.getPeer(msg.out.To.Peer.ID)
			if s.down[peer.state.ReplicaNumber] {
				continue
			}

			msgs := peer.Step(msg.toIncoming())

			newInflights := make([]inFlight, len(msgs))
			for i, msg := range msgs {
				newInflights[i] = inFlight{
					deliverAt: s.now + 1,
					from:      PeerCorrespondent(peer.state.GetConfiguration()),
					out:       msg,
				}
			}

			newMsgs = append(newMsgs, newInflights...)
		} else {
			s.replies = append(s.replies, msg.out.Req.(*ProcessReply))
		}
	}

	s.network = s.network[i:]
	s.network = append(s.network, newMsgs...)
}

func (s *Sim) tick() {
	s.t.Helper()

	s.now++

	s.deliver()
	for _, peer := range s.replicas {
		if s.down[peer.state.ReplicaNumber] {
			continue
		}

		out := peer.Tick()
		s.sendNextTick(PeerCorrespondent(peer.state.GetConfiguration()), out)
	}
}

func (s *Sim) RunUntil(condition Condition, maxTicks int) {
	s.t.Helper()

	stopTicks := s.now + maxTicks
	for s.now < stopTicks {
		if condition() {
			break
		}
		s.tick()
	}

	if !condition() {
		s.t.Fatal("condition not met")
	}
}

func (s *Sim) HasPrimary() bool {
	for _, peer := range s.replicas {
		if peer.state.Role == RolePrimary {
			return true
		}
	}

	return false
}

func (s *Sim) IsOperational() bool {
	primaries := 0
	backups := 0
	highestView := ViewNumber(0)
	for _, peer := range s.replicas {
		if s.down[peer.state.ReplicaNumber] {
			continue
		}

		if peer.state.Role == RolePrimary {
			primaries++
		} else {
			backups++
		}
		if peer.state.ViewNumber > highestView {
			highestView = peer.state.ViewNumber
		}
	}

	if primaries != 1 {
		return false
	}

	for _, peer := range s.replicas {
		if s.down[peer.state.ReplicaNumber] {
			continue
		}

		if peer.state.ViewNumber != highestView {
			return false
		}

		if peer.state.Status != StatusNormal {
			return false
		}
	}

	return true
}

func (s *Sim) IsInViewChange() bool {
	for _, peer := range s.replicas {
		if s.down[peer.state.ReplicaNumber] {
			continue
		}

		if peer.state.Status != StatusViewChange {
			return false
		}
	}

	return true
}

func (s *Sim) IsCommitted(clientId ClientId, requestNumber RequestNumber) bool {
	primary := s.getPrimary()
	for logIndex, logEntry := range primary.state.Log {
		if logEntry.ClientId == clientId && logEntry.RequestNumber == requestNumber {
			opNumber := logIndex + 1
			if primary.state.CommitNumber >= CommitNumber(opNumber) {
				return true
			}

			return false
		}
	}
	return false
}

func (s *Sim) IsResponded(requestNumber RequestNumber) bool {
	for _, reply := range s.replies {
		if reply.RequestNumber == requestNumber {
			return true
		}
	}

	return false
}

func TestInitialViewChangeSim(t *testing.T) {
	s := NewSim(t, 3, 42)
	s.RunUntil(s.IsOperational, 100)
}

func TestViewChangeWithOneDown(t *testing.T) {
	s := NewSim(t, 3, 42)
	s.RunUntil(s.IsOperational, 100)

	// stop primary
	primary := s.getPrimary()
	s.stop(primary.state.ReplicaNumber)

	// remaining peers enter view change
	s.RunUntil(s.IsInViewChange, 100)

	// remaining peers can complete view change
	s.RunUntil(s.IsOperational, 100)
}

func TestProcessSim(t *testing.T) {
	s := NewSim(t, 3, 42)
	s.RunUntil(s.IsOperational, 100)

	clientId := ClientId(555)
	reqId := RequestNumber(123)
	operation := []byte("lol")
	primary := s.getPrimary()
	s.Send(ClientCorrespondent(clientId), PeerCorrespondent(primary.state.GetConfiguration()),
		&ProcessRequest{
			RequestNumber: reqId,
			ClientId:      clientId,
			Operation:     operation,
		}, s.now+1)

	s.RunUntil(func() bool { return s.IsCommitted(clientId, reqId) }, 100)
	if !bytes.Equal(s.executed[primary.state.ReplicaNumber][0], operation) {
		t.Error("wrong payload")
	}

	s.RunUntil(func() bool { return s.IsResponded(reqId) }, 100)
}
