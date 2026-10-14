package vr

import (
	"log/slog"
	"slices"
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
loop:
	for {
		select {
		case <-r.stop:
			break loop
		case req := <-r.incoming:
			r.handleReq(req)
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
	default:
		panic("lol")
	}
}

func (r *Replica) handleProcessRequest(req *ProcessRequest, replyChan chan ProcessReply) {
	if r.state.Role != RolePrimary {
		slog.Info("ProcessRequest reached backup, dropping")
		return
	}

	lastClientState, ok := r.state.ClientTable[req.ClientId]
	if ok {
		if lastClientState.LatestRequestNumber == req.RequestNumber {
			replyChan <- ProcessReply{
				ViewNumber:    r.state.ViewNumber,
				RequestNumber: lastClientState.LatestRequestNumber,
				Reply:         lastClientState.LatestResult,
			}
			return
		}
		if lastClientState.LatestRequestNumber > req.RequestNumber {
			// drop request
			slog.Info("Stale request received, dropping")
			return
		}
	}

	r.state.Log = append(r.state.Log, req)
	r.state.OpNumber++

	client, ok := r.state.ClientTable[req.ClientId]
	if !ok {
		client = &Client{ReplyChans: make(map[RequestNumber]chan<- ProcessReply)}
		r.state.ClientTable[req.ClientId] = client
	}
	client.LatestRequestNumber = req.RequestNumber
	client.LatestResult = nil
	client.ReplyChans[req.RequestNumber] = replyChan

	r.inflight[r.state.OpNumber] = ReplyState{oks: map[ReplicaNumber]bool{
		r.state.ReplicaNumber: true,
	}}

	for _, peer := range r.peers() {
		r.outgoing <- Outgoing{
			Target: peer,
			Req: &Prepare{
				ViewNumber:   r.state.ViewNumber,
				Operation:    *req,
				OpNumber:     r.state.OpNumber,
				CommitNumber: r.state.CommitNumber,
			},
		}
	}
}

func (r *Replica) handlePrepare(from ReplicaConfiguration, req *Prepare) {
	if r.state.Role != RoleBackup {
		slog.Info("prepare: reached primary, dropping")
		return
	}

	if req.OpNumber < r.state.OpNumber {
		slog.Info("prepare: reprocessing", "req", req.OpNumber, "me", r.state.OpNumber)
		r.outgoing <- Outgoing{
			Target: from,
			Req: &PrepareOK{
				ViewNumber: r.state.ViewNumber,
				OpNumber:   req.OpNumber,
				BackupId:   r.state.ReplicaNumber,
			},
		}
		return
	}

	if req.OpNumber != r.state.OpNumber+1 {
		slog.Info("prepare: missing ops", "req", req.OpNumber, "me", r.state.OpNumber)
		r.earlyPrepares[req.OpNumber] = req
		return
	}

	r.state.Log = append(r.state.Log, &req.Operation)
	r.state.OpNumber++
	client, ok := r.state.ClientTable[req.Operation.ClientId]
	if !ok {
		client = &Client{ReplyChans: map[RequestNumber]chan<- ProcessReply{}}
	}
	client.LatestRequestNumber = req.Operation.RequestNumber
	client.LatestResult = nil

	r.handleEarlyPrepares()
	r.backupCommit(req.CommitNumber)

	r.outgoing <- Outgoing{
		Target: from,
		Req: &PrepareOK{
			ViewNumber: r.state.ViewNumber,
			OpNumber:   req.OpNumber,
			BackupId:   r.state.ReplicaNumber,
		},
	}
}

func (r *Replica) handleEarlyPrepares() {
	for len(r.earlyPrepares) > 0 {
		keys := make([]OpNumber, 0, len(r.earlyPrepares))
		for on := range r.earlyPrepares {
			keys = append(keys, on)
		}
		slices.Sort(keys)

		earliest := keys[0]

		if earliest != r.state.OpNumber+1 {
			// can't process yet
			break
		}

		req := r.earlyPrepares[earliest]
		delete(r.earlyPrepares, earliest)

		r.state.Log = append(r.state.Log, &req.Operation)
		r.state.OpNumber++
		client, ok := r.state.ClientTable[req.Operation.ClientId]
		if !ok {
			client = &Client{} // backup doesn't handle replies
		}
		client.LatestRequestNumber = req.Operation.RequestNumber
		client.LatestResult = nil
	}
}

func (r *Replica) quorum() int {
	return (len(r.state.ClusterMembers)-1)/2 + 1
}

func (r *Replica) earliestUncommittedOpWithQuorum() OpNumber {
	keys := make([]OpNumber, 0, len(r.inflight))
	for on := range r.inflight {
		keys = append(keys, on)
	}
	slices.Sort(keys)

	earliest := keys[0]

	replyState := r.inflight[earliest]
	if len(replyState.oks) < r.quorum() {
		// can't commit yet
		return 0
	}

	return earliest
}

func (r *Replica) handlePrepareOK(_ ReplicaConfiguration, req *PrepareOK) {
	replyState, ok := r.inflight[req.OpNumber]
	if !ok {
		slog.Info("got PrepareOK for unknown op", "PrepareOK", req)
		return
	}
	replyState.oks[req.BackupId] = true

	// check inflight
	for len(r.inflight) > 0 {
		earliestOp := r.earliestUncommittedOpWithQuorum()
		if earliestOp == 0 {
			// no quorum yet
			break
		}

		if earliestOp != OpNumber(r.state.CommitNumber)+1 {
			// no quorum yet for next to be committed
			break
		}

		r.state.CommitNumber++
		slog.Info("Committing", "commitNumber", r.state.CommitNumber)

		delete(r.inflight, earliestOp)

		originalReq := r.state.Log[r.state.CommitNumber-1]
		response := r.upCall(originalReq.Operation)

		client := r.state.ClientTable[originalReq.ClientId]
		client.LatestRequestNumber = originalReq.RequestNumber
		client.LatestResult = response
		client.ReplyChans[originalReq.RequestNumber] <- ProcessReply{
			ViewNumber:    r.state.ViewNumber,
			RequestNumber: originalReq.RequestNumber,
			Reply:         response,
		}
	}
}

func (r *Replica) handleCommit(_ ReplicaConfiguration, req *Commit) {
	r.backupCommit(req.CommitNumber)
}

func (r *Replica) backupCommit(primaryCommitNumber CommitNumber) {
	lastCommitable := primaryCommitNumber
	if r.state.CommitNumber >= lastCommitable {
		return
	}

	if r.state.OpNumber < OpNumber(primaryCommitNumber) {
		slog.Info("can't commit up to primaryCommitNumber, clamping", "my op",
			r.state.OpNumber, "primary commit", primaryCommitNumber)
		lastCommitable = CommitNumber(r.state.OpNumber)
	}

	for r.state.CommitNumber <= lastCommitable {
		r.state.CommitNumber++

		originalReq := r.state.Log[r.state.CommitNumber-1]

		response := r.upCall(originalReq.Operation)

		client := r.state.ClientTable[originalReq.ClientId]
		client.LatestRequestNumber = originalReq.RequestNumber
		client.LatestResult = response
	}
}
