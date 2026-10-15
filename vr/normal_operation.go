package vr

import (
	"log/slog"
	"slices"
	"time"
)

func (r *Replica) handleProcessRequest(req *ProcessRequest, replyChan chan ProcessReply) {
	if r.state.Status != StatusNormal {
		slog.Info("Can't process requests")
		return
	}

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
	if r.state.Status != StatusNormal {
		slog.Info("Can't process requests")
		return
	}
	if r.checkView(req.ViewNumber) {
		return
	}
	if r.state.Role != RoleBackup {
		slog.Info("prepare: reached primary, dropping")
		return
	}

	r.primaryAlive = time.Now()

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
		r.state.ClientTable[req.Operation.ClientId] = client
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
			r.state.ClientTable[req.Operation.ClientId] = client
		}
		client.LatestRequestNumber = req.Operation.RequestNumber
		client.LatestResult = nil
	}
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
	if r.checkView(req.ViewNumber) {
		return
	}

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
		if replyChan := client.ReplyChans[originalReq.RequestNumber]; replyChan != nil {
			replyChan <- ProcessReply{
				ViewNumber:    r.state.ViewNumber,
				RequestNumber: originalReq.RequestNumber,
				Reply:         response,
			}
		}
	}
}

func (r *Replica) handleCommit(_ ReplicaConfiguration, req *Commit) {
	if r.checkView(req.ViewNumber) {
		return
	}
	if r.state.Status != StatusNormal {
		return
	}
	if r.state.Role != RoleBackup {
		slog.Info("got commit as a primary")
		return
	}

	r.primaryAlive = time.Now()
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

	for r.state.CommitNumber < lastCommitable {
		r.state.CommitNumber++

		originalReq := r.state.Log[r.state.CommitNumber-1]

		response := r.upCall(originalReq.Operation)

		client := r.state.ClientTable[originalReq.ClientId]
		client.LatestRequestNumber = originalReq.RequestNumber
		client.LatestResult = response
	}
}
