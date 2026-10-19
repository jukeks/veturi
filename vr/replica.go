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
}

func NewClient() *Client {
	return &Client{}

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

func (rs *ReplicaState) GetConfiguration() ReplicaConfiguration {
	for _, member := range rs.ClusterMembers {
		if member.ID == rs.ReplicaNumber {
			return member
		}
	}

	return ReplicaConfiguration{}
}

func DefaultReplicaState() ReplicaState {
	return ReplicaState{
		Role:        RoleBackup,
		ClientTable: map[ClientId]*Client{},
	}
}

func (s *ReplicaState) getClient(id ClientId) *Client {
	client, ok := s.ClientTable[id]
	if !ok {
		client = NewClient()
		s.ClientTable[id] = client
	}
	return client
}

type CorrespondendType int

const (
	CorrespondentClient CorrespondendType = iota
	CorrespondentPeer
)

type Correspondend struct {
	Type   CorrespondendType
	Peer   ReplicaConfiguration
	Client ClientId
}

func ClientCorrespondent(id ClientId) Correspondend {
	return Correspondend{
		Type:   CorrespondentClient,
		Client: id,
	}
}

func PeerCorrespondent(peer ReplicaConfiguration) Correspondend {
	return Correspondend{
		Type: CorrespondentPeer,
		Peer: peer,
	}
}

type Incoming struct {
	From Correspondend
	Req  Request
}

type Outgoing struct {
	To  Correspondend
	Req Request
}

type ReplyState struct {
	oks map[ReplicaNumber]bool
}

type UpCall func(Payload) Payload

type Replica struct {
	state          ReplicaState
	incoming       chan Incoming
	outgoing       chan Outgoing
	outgoingBuffer []Outgoing
	stop           chan bool
	stopped        chan bool

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

func (r *Replica) Request(req ProcessRequest) {
	r.incoming <- Incoming{
		Req:  &req,
		From: ClientCorrespondent(req.ClientId),
	}
}

func (r *Replica) Step(req Incoming) []Outgoing {
	r.handleReq(req)

	out := r.outgoingBuffer
	r.outgoingBuffer = nil
	return out
}

func (r *Replica) Run() {
	ticker := time.NewTicker(TickInterval)

loop:
	for {
		select {
		case <-r.stop:
			break loop
		case req := <-r.incoming:
			out := r.Step(req)
			for _, req := range out {
				r.outgoing <- req
			}
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
		r.handleProcessRequest(req, incoming.From.Client)
	case *Prepare:
		slog.Info("handleReq Prepare", "replica", r.state.ReplicaNumber, "from", incoming.From, "req", incoming.Req)
		r.handlePrepare(incoming.From.Peer, req)
	case *PrepareOK:
		slog.Info("handleReq PrepareOK", "replica", r.state.ReplicaNumber, "from", incoming.From, "req", incoming.Req)
		r.handlePrepareOK(incoming.From.Peer, req)
	case *Commit:
		slog.Info("handleReq Commit", "replica", r.state.ReplicaNumber, "from", incoming.From, "req", incoming.Req)
		r.handleCommit(incoming.From.Peer, req)
	case *StartViewChange:
		slog.Info("handleReq StartViewChange", "replica", r.state.ReplicaNumber, "from", incoming.From, "req", incoming.Req)
		r.handleStartViewChange(incoming.From.Peer, req)
	case *DoViewChange:
		slog.Info("handleReq DoViewChange", "replica", r.state.ReplicaNumber, "from", incoming.From, "req", incoming.Req)
		r.handleDoViewChange(incoming.From.Peer, req)
	case *StartView:
		slog.Info("handleReq StartView", "replica", r.state.ReplicaNumber, "from", incoming.From, "req", incoming.Req)
		r.handleStartView(incoming.From.Peer, req)
	default:
		panic("lol")
	}
}

func (r *Replica) sendPeer(peer ReplicaConfiguration, req Request) {
	r.outgoingBuffer = append(r.outgoingBuffer, Outgoing{
		To:  PeerCorrespondent(peer),
		Req: req,
	})
}

func (r *Replica) sendClient(client ClientId, req Request) {
	r.outgoingBuffer = append(r.outgoingBuffer, Outgoing{
		To:  ClientCorrespondent(client),
		Req: req,
	})
}

func (r *Replica) handleTick() {
	if r.state.Role == RolePrimary {
		for _, peer := range r.peers() {
			r.sendPeer(peer, &Commit{
				ViewNumber:   r.state.ViewNumber,
				CommitNumber: r.state.CommitNumber,
			})
		}
	} else {
		if time.Since(r.primaryAlive) > PrimaryTimeout {
			r.initiateViewChange()
		}
	}
}

func (r *Replica) commitUpTo(commit CommitNumber) {
	for r.state.CommitNumber < commit {
		r.state.CommitNumber++
		req := r.state.Log[r.state.CommitNumber-1]
		response := r.upCall(req.Operation)

		client := r.state.getClient(req.ClientId)
		client.LatestRequestNumber = req.RequestNumber
		client.LatestResult = response

		if r.state.Role == RolePrimary {
			r.sendClient(req.ClientId, &ProcessReply{
				ViewNumber:    r.state.ViewNumber,
				RequestNumber: req.RequestNumber,
				Reply:         response,
			})
		}
	}
}
