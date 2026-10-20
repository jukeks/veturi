package vr

import (
	"log/slog"
	"slices"
)

func (r *Replica) handleGetState(from ReplicaConfiguration, req *GetState) {
	if r.checkView(req.ViewNumber) {
		return
	}
	if r.state.Status != StatusNormal {
		return
	}
	if req.OpNumber > r.state.OpNumber {
		slog.Error("peer requested newer state than i have")
		return
	}

	r.sendPeer(from, &NewState{
		ViewNumber:   r.state.ViewNumber,
		Log:          slices.Clone(r.state.Log[req.OpNumber:]),
		OpNumber:     r.state.OpNumber,
		CommitNumber: r.state.CommitNumber,
	})
}

func (r *Replica) handleNewState(from ReplicaConfiguration, req *NewState) {
	if r.checkView(req.ViewNumber) {
		return
	}

	r.stateRequested = false
	r.ticksSinceRequest = 0

	if req.OpNumber > r.state.OpNumber {
		logSuffix := req.Log
		if req.OpNumber-OpNumber(len(logSuffix)) != r.state.OpNumber {
			opDiff := req.OpNumber - r.state.OpNumber
			logSuffix = logSuffix[OpNumber(len(logSuffix))-opDiff:]
		}
		r.state.Log = append(r.state.Log, logSuffix...)
		r.state.OpNumber = req.OpNumber
	}

	r.commitUpTo(req.CommitNumber)
	r.lastNormalView = r.state.ViewNumber

	toDelete := []OpNumber{}
	for op := range r.earlyPrepares {
		if op <= req.OpNumber {
			toDelete = append(toDelete, op)
		}
	}
	for _, op := range toDelete {
		delete(r.earlyPrepares, op)
	}
	r.handleEarlyPrepares()

	r.sendPeer(r.getPrimaryForView(r.state.ViewNumber), &PrepareOK{
		ViewNumber: r.state.ViewNumber,
		OpNumber:   r.state.OpNumber,
		BackupId:   r.state.ReplicaNumber,
	})
}

const StateRequestTimeout = 10

func (r *Replica) requestState() {
	if r.stateRequested && r.ticksSinceRequest > StateRequestTimeout {
		r.stateRequested = false
		r.ticksSinceRequest = 0
	}
	if r.stateRequested {
		return
	}

	r.stateRequested = true

	r.sendPeer(r.getPrimaryForView(r.state.ViewNumber), &GetState{
		ViewNumber: r.state.ViewNumber,
		OpNumber:   r.state.OpNumber,
		BackupId:   r.state.ReplicaNumber,
	})
}
