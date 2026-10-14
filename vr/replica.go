package vr

import (
	"log/slog"
	"time"
)

const (
	TickInterval   = 50 * time.Millisecond
	PrimaryTimeout = 500 * time.Millisecond
)

type ReplicaConfiguration struct {
	ID       ReplicaNumber
	Hostname string
}

type ReplicaStatus int

const (
	StatusNormal ReplicaStatus = iota
	StatusViewChange
	StatusRecovering
)

type ReplicaRole int

const (
	RolePrimary ReplicaRole = iota
	RoleBackup
)

type Client struct {
	LatestRequestNumber RequestNumber
	LatestResult        Payload
	ReplyChans          map[RequestNumber]chan<- ProcessReply
}

type ReplicaState struct {
	ClusterMembers []ReplicaConfiguration
	ReplicaNumber  ReplicaNumber
	ViewNumber     ViewNumber
	Status         ReplicaStatus
	OpNumber       OpNumber
	Log            []*ProcessRequest
	CommitNumber   CommitNumber
	ClientTable    map[ClientId]*Client
	Role           ReplicaRole
}

func DefaultReplicaState() ReplicaState {
	return ReplicaState{
		Role:        RoleBackup,
		ClientTable: map[ClientId]*Client{},
	}
}

type Incoming struct {
	// either From or Return
	From   ReplicaConfiguration
	Return chan ProcessReply

	Req Request
}

type Outgoing struct {
	Target ReplicaConfiguration
	Req    Request
}

type ReplyState struct {
	oks map[ReplicaNumber]bool
}

type UpCall func(Payload) Payload

type Replica struct {
	state    ReplicaState
	incoming chan Incoming
	outgoing chan Outgoing
	stop     chan bool
	stopped  chan bool

	earlyPrepares map[OpNumber]*Prepare
	inflight      map[OpNumber]ReplyState
	upCall        UpCall

	primaryAlive time.Time

	lastNormalView ViewNumber
	viewChanges    map[ViewNumber]*ViewChange
}

func NewReplica(id ReplicaNumber, clusterMembers []ReplicaConfiguration, upCall UpCall) *Replica {
	state := DefaultReplicaState()
	state.ReplicaNumber = id
	state.ClusterMembers = clusterMembers

	return &Replica{
		state:         state,
		incoming:      make(chan Incoming, 100),
		outgoing:      make(chan Outgoing, 100),
		stop:          make(chan bool, 1),
		stopped:       make(chan bool, 1),
		earlyPrepares: map[OpNumber]*Prepare{},
		inflight:      map[OpNumber]ReplyState{},
		upCall:        upCall,
		viewChanges:   map[ViewNumber]*ViewChange{},
	}
}

func (r *Replica) Request(req ProcessRequest) <-chan ProcessReply {
	replyChan := make(chan ProcessReply, 1)
	r.incoming <- Incoming{
		Req:    &req,
		Return: replyChan,
	}
	return replyChan
}

func (r *Replica) Run() {
	ticker := time.NewTicker(250 * time.Millisecond)

loop:
	for {
		select {
		case <-r.stop:
			break loop
		case req := <-r.incoming:
			r.handleReq(req)
		case <-ticker.C:
			r.handleTick()
		}
	}

	r.stopped <- true
}

func (r *Replica) Stop() {
	r.stop <- true
	<-r.stopped
}

func (r *Replica) Queue(incoming Incoming) {
	r.incoming <- incoming
}

func (r *Replica) peers() []ReplicaConfiguration {
	peers := []ReplicaConfiguration{}

	for _, peer := range r.state.ClusterMembers {
		if peer.ID == r.state.ReplicaNumber {
			continue
		}
		peers = append(peers, peer)
	}

	return peers
}

func (r *Replica) quorum() int {
	return r.resilience() + 1
}

func (r *Replica) resilience() int {
	return (len(r.state.ClusterMembers) - 1) / 2
}

func (r *Replica) handleReq(incoming Incoming) {
	switch req := incoming.Req.(type) {
	case *ProcessRequest:
		slog.Info("handleReq ProcessRequest", "replica", r.state.ReplicaNumber, "from", incoming.From, "req", incoming.Req)
		r.handleProcessRequest(req, incoming.Return)
	case *Prepare:
		slog.Info("handleReq Prepare", "replica", r.state.ReplicaNumber, "from", incoming.From, "req", incoming.Req)
		r.handlePrepare(incoming.From, req)
	case *PrepareOK:
		slog.Info("handleReq PrepareOK", "replica", r.state.ReplicaNumber, "from", incoming.From, "req", incoming.Req)
		r.handlePrepareOK(incoming.From, req)
	case *Commit:
		slog.Info("handleReq Commit", "replica", r.state.ReplicaNumber, "from", incoming.From, "req", incoming.Req)
		r.handleCommit(incoming.From, req)
	case *StartViewChange:
		slog.Info("handleReq StartViewChange", "replica", r.state.ReplicaNumber, "from", incoming.From, "req", incoming.Req)
		r.handleStartViewChange(incoming.From, req)
	case *DoViewChange:
		slog.Info("handleReq DoViewChange", "replica", r.state.ReplicaNumber, "from", incoming.From, "req", incoming.Req)
		r.handleDoViewChange(incoming.From, req)
	case *StartView:
		slog.Info("handleReq StartView", "replica", r.state.ReplicaNumber, "from", incoming.From, "req", incoming.Req)
		r.handleStartView(incoming.From, req)
	default:
		panic("lol")
	}
}

func (r *Replica) sendPeer(peer ReplicaConfiguration, req Request) {
	r.outgoing <- Outgoing{
		Target: peer,
		Req:    req,
	}
}

func (r *Replica) handleTick() {
	if r.state.Role == RolePrimary {
		for _, peer := range r.peers() {
			r.sendPeer(peer, &Commit{
				CommitNumber: r.state.CommitNumber,
				OpNumber:     r.state.OpNumber,
			})
		}
	} else {
		if time.Since(r.primaryAlive) > PrimaryTimeout {
			r.initiateViewChange()
		}
	}
}
