package vr

import (
	"log/slog"
	"slices"
)

func (r *Replica) handleProcessRequest(req *ClientRequest) {
	if r.state.Status != StatusNormal {
		slog.Info("Can't process requests")
		return
	}

	if r.state.Role != RolePrimary {
		slog.Info("ProcessRequest reached backup, dropping")
		return
	}

	lastClientState, ok := r.state.ClientTable[req.From]
	if ok {
		if lastClientState.LatestRequestNumber == req.RequestNumber {
			r.sendClient(req.From, &ClientReply{
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
		if logEntry.From == req.From && logEntry.RequestNumber == req.RequestNumber {
			slog.Info("dropping already processing request")
			return
		}
	}

	r.state.Log = append(r.state.Log, req)
	r.state.OpNumber++

	for _, peer := range r.peers() {
		r.sendPeer(peer, &Prepare{
			ViewNumber:   r.state.ViewNumber,
			Operation:    *req,
			OpNumber:     r.state.OpNumber,
			CommitNumber: r.state.CommitNumber,
		})
	}
}

func (r *Replica) handlePrepare(req *Prepare) {
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
		r.sendPeer(r.getPrimaryForView(r.state.ViewNumber), &PrepareOK{
			ViewNumber: r.state.ViewNumber,
			OpNumber:   req.OpNumber,
			From:       r.state.ReplicaNumber,
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

	r.sendPeer(r.getPrimaryForView(r.state.ViewNumber), &PrepareOK{
		ViewNumber: r.state.ViewNumber,
		OpNumber:   r.state.OpNumber,
		From:       r.state.ReplicaNumber,
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

func (r *Replica) quorumOp() OpNumber {
	ops := []OpNumber{r.state.OpNumber}
	for _, peer := range r.peers() {
		ops = append(ops, r.acked[peer])
	}
	slices.Sort(ops)                // n is 3 or 5
	return ops[len(ops)-r.quorum()] // the quorum()-th highest
}

func (r *Replica) handlePrepareOK(req *PrepareOK) {
	if r.checkViewForNormalOperations(req.ViewNumber) {
		return
	}
	if r.state.Role != RolePrimary || r.state.Status != StatusNormal {
		return
	}
	if req.OpNumber > r.state.OpNumber || req.OpNumber <= r.acked[req.From] {
		return
	}

	r.acked[req.From] = req.OpNumber
	r.lastAck[req.From] = 0
	if op := r.quorumOp(); CommitNumber(op) > r.state.CommitNumber {
		r.commitUpTo(CommitNumber(op))
	}
}

func (r *Replica) handleCommit(req *Commit) {
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
