package vr

import (
	"log/slog"
	"slices"
)

func (r *Replica) handleGetState(req *GetState) {
	if r.checkViewForNormalOperations(req.ViewNumber) {
		return
	}
	if r.state.Status != StatusNormal {
		return
	}
	if req.OpNumber > r.state.OpNumber {
		slog.Info("peer requested newer state than i have")
		return
	}
	if r.lastNormalView != r.state.ViewNumber {
		slog.Info("can't answer to GetState as not in normal view yet")
		return
	}

	r.sendPeer(req.From, &NewState{
		ViewNumber:   r.state.ViewNumber,
		Log:          slices.Clone(r.state.Log[req.OpNumber:]),
		OpNumber:     r.state.OpNumber,
		CommitNumber: r.state.CommitNumber,
	})
}

func (r *Replica) handleNewState(req *NewState) {
	if r.checkViewForNormalOperations(req.ViewNumber) {
		return
	}

	if req.OpNumber > r.state.OpNumber {
		logSuffix := req.Log
		newStateLogStartOp := req.OpNumber - OpNumber(len(req.Log))
		if newStateLogStartOp > r.state.OpNumber {
			// gap!
			slog.Info("NewState does not follow my log, dropping",
				"start", newStateLogStartOp, "my op", r.state.OpNumber)
			return
		}
		if newStateLogStartOp < r.state.OpNumber {
			// already had a new op added to log, dropping start of newStateLog
			logSuffix = logSuffix[r.state.OpNumber-newStateLogStartOp:]
		}
		r.state.Log = append(r.state.Log, logSuffix...)
		r.state.OpNumber = req.OpNumber
	}

	r.stateRequested = false
	r.ticksSinceRequest = 0
	r.ticksWithGap = 0

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
		From:       r.state.ReplicaNumber,
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
		From:       r.state.ReplicaNumber,
	})
}
