package vr

import (
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"
)

const (
	TickInterval        = 50 * time.Millisecond
	PrimaryTimeoutTicks = 10
)

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
	ClusterMembers []ReplicaNumber
	ReplicaNumber  ReplicaNumber
	ViewNumber     ViewNumber
	Status         ReplicaStatus
	OpNumber       OpNumber
	Log            []*ClientRequest
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

func (s *ReplicaState) getClient(id ClientId) *Client {
	client, ok := s.ClientTable[id]
	if !ok {
		client = NewClient()
		s.ClientTable[id] = client
	}
	return client
}

type CorrespondentType int

const (
	CorrespondentClient CorrespondentType = iota
	CorrespondentPeer
)

type Correspondent struct {
	Type   CorrespondentType
	Peer   ReplicaNumber
	Client ClientId
}

func ClientCorrespondent(id ClientId) Correspondent {
	return Correspondent{
		Type:   CorrespondentClient,
		Client: id,
	}
}

func PeerCorrespondent(peer ReplicaNumber) Correspondent {
	return Correspondent{
		Type: CorrespondentPeer,
		Peer: peer,
	}
}

type Outgoing struct {
	To  Correspondent
	Msg Message
}

type UpCall func(Payload) Payload

type Replica struct {
	state          ReplicaState
	incoming       chan Message
	outgoing       chan Outgoing
	outgoingBuffer []Outgoing
	stop           chan bool
	stopped        chan bool

	upCall UpCall

	earlyPrepares map[OpNumber]*Prepare

	// on the primary: the highest op that each peer acknowledged in this view
	acked map[ReplicaNumber]OpNumber
	// the tick of the last progress of each peer, for the resends
	lastAck map[ReplicaNumber]int

	ticksSincePrimaryComms int
	primaryTimeoutTicks    int

	lastNormalView ViewNumber
	viewChanges    map[ViewNumber]*ViewChange

	rand *rand.Rand

	stateRequested         bool
	ticksSinceStateRequest int

	ticksWithPrepareOKGap int

	recoveryResponses           map[ReplicaNumber]*RecoveryResponse
	startingInRecover           bool
	ticksSinceRecoveryRequested int
	recoveryNonce               Nonce
}

func NewReplica(id ReplicaNumber, clusterMembers []ReplicaNumber,
	upCall UpCall, seed uint64, recover bool) *Replica {
	state := DefaultReplicaState()
	state.ReplicaNumber = id
	state.ClusterMembers = clusterMembers
	if recover {
		state.Status = StatusRecovering
	}

	r := Replica{
		state:             state,
		incoming:          make(chan Message, 100),
		outgoing:          make(chan Outgoing, 100),
		stop:              make(chan bool, 1),
		stopped:           make(chan bool, 1),
		earlyPrepares:     map[OpNumber]*Prepare{},
		upCall:            upCall,
		acked:             map[ReplicaNumber]OpNumber{},
		lastAck:           map[ReplicaNumber]int{},
		viewChanges:       map[ViewNumber]*ViewChange{},
		rand:              rand.New(rand.NewPCG(seed, uint64(id))),
		recoveryResponses: map[ReplicaNumber]*RecoveryResponse{},
		startingInRecover: recover,
	}

	r.resetTimer()
	return &r
}

func (r *Replica) Request(req ClientRequest) {
	r.incoming <- &req
}

func (r *Replica) Step(msg Message) []Outgoing {
	r.handleMessage(msg)
	return r.flush()
}

func (r *Replica) Tick() []Outgoing {
	r.handleTick()
	return r.flush()
}

func (r *Replica) Run() {
	ticker := time.NewTicker(TickInterval)

loop:
	for {
		select {
		case <-r.stop:
			break loop
		case msg := <-r.incoming:
			r.send(r.Step(msg))
		case <-ticker.C:
			r.send(r.Tick())
		}
	}

	r.stopped <- true
}

func (r *Replica) Stop() {
	r.stop <- true
	<-r.stopped
}

func (r *Replica) Queue(msg Message) {
	r.incoming <- msg
}

func (r *Replica) peers() []ReplicaNumber {
	peers := []ReplicaNumber{}

	for _, peer := range r.state.ClusterMembers {
		if peer == r.state.ReplicaNumber {
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

func (r *Replica) handleMessage(msg Message) {
	slog.Info("handleMessage", "replica", r.state.ReplicaNumber,
		"type", fmt.Sprintf("%T", msg), "msg", msg)

	if r.state.Status == StatusRecovering {
		switch msg.(type) {
		case *RecoveryResponse:
			break
		default:
			slog.Info("recovering, dropping requests")
			return
		}
	}

	switch msg := msg.(type) {
	case *ClientRequest:
		r.handleProcessRequest(msg)
	case *Prepare:
		r.handlePrepare(msg)
	case *PrepareOK:
		r.handlePrepareOK(msg)
	case *Commit:
		r.handleCommit(msg)
	case *StartViewChange:
		r.handleStartViewChange(msg)
	case *DoViewChange:
		r.handleDoViewChange(msg)
	case *StartView:
		r.handleStartView(msg)
	case *GetState:
		r.handleGetState(msg)
	case *NewState:
		r.handleNewState(msg)
	case *Recovery:
		r.handleRecovery(msg)
	case *RecoveryResponse:
		r.handleRecoveryResponse(msg)
	default:
		slog.Error("received message that is not recognized", "msg", msg)
	}
}

func (r *Replica) sendPeer(peer ReplicaNumber, msg Message) {
	r.outgoingBuffer = append(r.outgoingBuffer, Outgoing{
		To:  PeerCorrespondent(peer),
		Msg: msg,
	})
}

func (r *Replica) sendClient(client ClientId, msg Message) {
	r.outgoingBuffer = append(r.outgoingBuffer, Outgoing{
		To:  ClientCorrespondent(client),
		Msg: msg,
	})
}

func (r *Replica) flush() []Outgoing {
	out := r.outgoingBuffer
	r.outgoingBuffer = nil
	return out
}

func (r *Replica) send(msgs []Outgoing) {
	for _, msg := range msgs {
		r.outgoing <- msg
	}
}

const RecoveryTimeout = 10
const EarlyPrepareTimeout = 5
const PrepareOkTimeout = 3

func (r *Replica) handleTick() {
	if r.startingInRecover {
		r.startingInRecover = false
		r.startRecovery()
		return
	}
	if r.state.Status == StatusRecovering {
		r.ticksSinceRecoveryRequested++
		if r.ticksSinceRecoveryRequested > RecoveryTimeout {
			r.requestRecovery()
		}
		return
	}

	if r.state.Role == RolePrimary {
		for _, peer := range r.peers() {
			r.sendPeer(peer, &Commit{
				ViewNumber:   r.state.ViewNumber,
				CommitNumber: r.state.CommitNumber,
			})

			r.lastAck[peer]++
			if r.lastAck[peer] < PrepareOkTimeout {
				continue
			}
			r.lastAck[peer] = 0

			if r.acked[peer] < r.state.OpNumber {
				for op := r.acked[peer] + 1; op <= r.state.OpNumber; op++ {
					logEntry := r.state.Log[op-1]
					r.sendPeer(peer, &Prepare{
						ViewNumber:   r.state.ViewNumber,
						Operation:    *logEntry,
						OpNumber:     op,
						CommitNumber: r.state.CommitNumber,
					})
				}
			}
		}
	} else {
		r.ticksSincePrimaryComms++
		if r.ticksSincePrimaryComms >= r.primaryTimeoutTicks {
			r.initiateViewChange()
		}

		if len(r.earlyPrepares) == 0 {
			r.ticksWithPrepareOKGap = 0
		} else {
			r.ticksWithPrepareOKGap++
			if r.ticksWithPrepareOKGap >= EarlyPrepareTimeout {
				r.requestState()
			}
		}
	}

	if r.stateRequested {
		r.ticksSinceStateRequest++
		if r.ticksSinceStateRequest > StateRequestTimeout {
			r.requestState()
		}
	}
}

func (r *Replica) commitUpTo(commit CommitNumber) {
	for r.state.CommitNumber < commit {
		r.state.CommitNumber++
		req := r.state.Log[r.state.CommitNumber-1]
		response := r.upCall(req.Operation)

		client := r.state.getClient(req.From)
		client.LatestRequestNumber = req.RequestNumber
		client.LatestResult = response

		if r.state.Role == RolePrimary {
			r.sendClient(req.From, &ClientReply{
				ViewNumber:    r.state.ViewNumber,
				RequestNumber: req.RequestNumber,
				Reply:         response,
			})
		}
	}
}

func (r *Replica) resetTimer() {
	r.ticksSincePrimaryComms = 0
	r.primaryTimeoutTicks = PrimaryTimeoutTicks + r.rand.IntN(PrimaryTimeoutTicks) // jitter
}
