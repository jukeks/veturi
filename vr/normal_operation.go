package vr

import (
	"log/slog"
	"slices"
)

func (r *Replica) handleProcessRequest(req *ProcessRequest, from ClientId) {
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
			r.sendClient(from, &ProcessReply{
				ViewNumber:    r.state.ViewNumber,
				RequestNumber: lastClientState.LatestRequestNumber,
				Reply:         lastClientState.LatestResult,
			})
			return
		}
		if lastClientState.LatestRequestNumber > req.RequestNumber {
			// drop request
			slog.Info("Stale request received, dropping")
			return
		}
	}

	for op := OpNumber(r.state.CommitNumber + 1); op <= r.state.OpNumber; op++ {
		logEntry := r.state.Log[op-1]
		if logEntry.ClientId == req.ClientId && logEntry.RequestNumber == req.RequestNumber {
			slog.Info("dropping already processing request")
			return
		}
	}

	r.state.Log = append(r.state.Log, req)
	r.state.OpNumber++

	r.inflight[r.state.OpNumber] = ReplyState{oks: map[ReplicaNumber]bool{
		r.state.ReplicaNumber: true,
	}}

	for _, peer := range r.peers() {
		r.sendPeer(peer, &Prepare{
			ViewNumber:   r.state.ViewNumber,
			Operation:    *req,
			OpNumber:     r.state.OpNumber,
			CommitNumber: r.state.CommitNumber,
		})
	}
}

func (r *Replica) handlePrepare(from ReplicaConfiguration, req *Prepare) {
	if r.checkViewForNormalOperations(req.ViewNumber) {
		return
	}
	if r.state.Role != RoleBackup {
		slog.Info("prepare: reached primary, dropping")
		return
	}
	if r.state.Status != StatusNormal {
		slog.Info("Can't process requests")
		return
	}

	r.resetTimer()

	if req.OpNumber <= r.state.OpNumber {
		slog.Info("prepare: reprocessing", "req", req.OpNumber, "me", r.state.OpNumber)
		r.sendPeer(from, &PrepareOK{
			ViewNumber: r.state.ViewNumber,
			OpNumber:   req.OpNumber,
			BackupId:   r.state.ReplicaNumber,
		})
		return
	}

	if req.OpNumber != r.state.OpNumber+1 {
		slog.Info("prepare: missing ops", "req", req.OpNumber, "me", r.state.OpNumber)
		r.earlyPrepares[req.OpNumber] = req
		return
	}

	r.state.Log = append(r.state.Log, &req.Operation)
	r.state.OpNumber++

	r.handleEarlyPrepares()
	r.backupCommit(req.CommitNumber)

	r.sendPeer(from, &PrepareOK{
		ViewNumber: r.state.ViewNumber,
		OpNumber:   r.state.OpNumber,
		BackupId:   r.state.ReplicaNumber,
	})
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

func (r *Replica) updateOKUpTo(lastOp OpNumber, peer ReplicaNumber) {
	// mark all prepares ok up to lastOp
	for key := range r.inflight {
		replyState := r.inflight[key]
		if key <= lastOp {
			replyState.oks[peer] = true
		}
	}
}

func (r *Replica) handlePrepareOK(_ ReplicaConfiguration, req *PrepareOK) {
	if r.checkViewForNormalOperations(req.ViewNumber) {
		return
	}

	_, ok := r.inflight[req.OpNumber]
	if !ok {
		slog.Info("got PrepareOK for unknown op", "PrepareOK", req)
		return
	}
	r.updateOKUpTo(req.OpNumber, req.BackupId)

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

		delete(r.inflight, earliestOp)

		r.commitUpTo(CommitNumber(earliestOp))
	}
}

func (r *Replica) handleCommit(_ ReplicaConfiguration, req *Commit) {
	if r.checkViewForNormalOperations(req.ViewNumber) {
		return
	}
	if r.state.Status != StatusNormal {
		return
	}
	if r.state.Role != RoleBackup {
		slog.Info("got commit as a primary")
		return
	}

	r.resetTimer()
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
		r.requestState()
	}

	r.commitUpTo(lastCommitable)
}
