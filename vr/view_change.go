package vr

import (
	"log/slog"
)

type ViewChange struct {
	StartViewChanges map[ReplicaNumber]*StartViewChange
	DoViewChanges    map[ReplicaNumber]*DoViewChange
}

func NewViewChange() *ViewChange {
	return &ViewChange{
		StartViewChanges: map[ReplicaNumber]*StartViewChange{},
		DoViewChanges:    map[ReplicaNumber]*DoViewChange{},
	}
}

func (vc *ViewChange) hasQuorum(quorum int) bool {
	return len(vc.DoViewChanges) >= quorum
}

func (vc *ViewChange) longestLogAndHighestCommit() (*DoViewChange, CommitNumber) {
	dos := vc.DoViewChanges
	keys := make([]ReplicaNumber, 0, len(vc.DoViewChanges))
	for rn := range vc.DoViewChanges {
		keys = append(keys, rn)
	}

	highestLastView := dos[keys[0]]
	for _, do := range dos {
		if do.LastView > highestLastView.LastView {
			highestLastView = do
		}
	}
	dosFromLastView := []*DoViewChange{}
	for _, do := range dos {
		if do.LastView == highestLastView.LastView {
			dosFromLastView = append(dosFromLastView, do)
		}
	}

	longestLog := dosFromLastView[0]
	for _, do := range dosFromLastView {
		if do.OpNumber > longestLog.OpNumber {
			longestLog = do
		}
	}
	highestCommit := dosFromLastView[0]
	for _, do := range dosFromLastView {
		if do.CommitNumber > highestCommit.CommitNumber {
			highestCommit = do
		}
	}

	return longestLog, highestCommit.CommitNumber
}

func (r *Replica) checkView(reqView ViewNumber) bool {
	if r.state.ViewNumber > reqView {
		slog.Info("req from older view, dropping")
		return true
	}
	if r.state.ViewNumber < reqView {
		r.moveToNewerView(reqView)
	}

	return false
}

func (r *Replica) resetViewState(v ViewNumber) {
	r.state.ViewNumber = v
	r.state.Role = RoleBackup
	r.resetTimer()

	clear(r.inflight)
	clear(r.earlyPrepares)

	for view := range r.viewChanges {
		if view < v {
			delete(r.viewChanges, view)
		}
	}
}

func (r *Replica) startViewChange(v ViewNumber) {
	r.resetViewState(v)
	r.state.Status = StatusViewChange
	for _, peer := range r.peers() {
		r.sendPeer(peer, &StartViewChange{ViewNumber: v, ReplicaNumber: r.state.ReplicaNumber})
	}
}

func (r *Replica) startNormal(v ViewNumber, role ReplicaRole,
	log []*ProcessRequest, op OpNumber, commit CommitNumber) {
	r.resetViewState(v)
	r.state.Status = StatusNormal
	r.state.Role = role
	r.lastNormalView = v
	r.state.Log = nil
	r.state.Log = append(r.state.Log, log...)
	r.state.OpNumber = op
	r.commitUpTo(commit)

	for n := OpNumber(r.state.CommitNumber) + 1; n <= op; n++ {
		if role == RolePrimary {
			r.inflight[n] = ReplyState{
				oks: map[ReplicaNumber]bool{r.state.ReplicaNumber: true},
			}
		} else {
			r.sendPeer(r.getPrimaryForView(v),
				&PrepareOK{ViewNumber: v, OpNumber: n, BackupId: r.state.ReplicaNumber})
		}
	}
}

func (r *Replica) moveToNewerView(v ViewNumber) {
	r.resetViewState(v)
	r.state.OpNumber = OpNumber(r.state.CommitNumber)
	r.state.Log = r.state.Log[:r.state.OpNumber]
	r.state.Status = StatusNormal
	// do not set lastNormalView: this replica has only the committed prefix of view v
	// TODO: send GETSTATE; after NEWSTATE, set lastNormalView = v
}

func (r *Replica) initiateViewChange() {
	r.startViewChange(r.state.ViewNumber + 1)
}

func (r *Replica) getPrimaryForView(view ViewNumber) ReplicaConfiguration {
	return r.state.ClusterMembers[int(view)%len(r.state.ClusterMembers)]
}

func (r *Replica) handleStartViewChange(from ReplicaConfiguration, req *StartViewChange) {
	if req.ViewNumber < r.state.ViewNumber {
		// older view change
		return
	}

	viewChange, ok := r.viewChanges[req.ViewNumber]
	if !ok {
		viewChange = NewViewChange()
		r.viewChanges[req.ViewNumber] = viewChange
	}
	viewChange.StartViewChanges[from.ID] = req

	if req.ViewNumber > r.state.ViewNumber {
		r.startViewChange(req.ViewNumber)
	}

	if len(viewChange.StartViewChanges) < r.resilience() {
		return
	}

	viewPrimary := r.getPrimaryForView(req.ViewNumber)
	r.sendPeer(viewPrimary, &DoViewChange{
		ViewNumber:    r.state.ViewNumber,
		Log:           r.state.Log,
		LastView:      r.lastNormalView,
		OpNumber:      r.state.OpNumber,
		CommitNumber:  r.state.CommitNumber,
		ReplicaNumber: r.state.ReplicaNumber,
	})
}

func (r *Replica) handleDoViewChange(from ReplicaConfiguration, req *DoViewChange) {
	if req.ViewNumber > r.state.ViewNumber {
		r.startViewChange(req.ViewNumber)
	}
	if r.state.Status == StatusNormal {
		// view change already concluded
		return
	}
	if req.ViewNumber < r.state.ViewNumber {
		// older view change
		return
	}

	viewChange, ok := r.viewChanges[req.ViewNumber]
	if !ok {
		viewChange = NewViewChange()
		r.viewChanges[req.ViewNumber] = viewChange
	}

	viewChange.DoViewChanges[req.ReplicaNumber] = req
	if !viewChange.hasQuorum(r.quorum()) {
		return
	}

	longestLog, highestCommit := viewChange.longestLogAndHighestCommit()

	r.startNormal(req.ViewNumber, RolePrimary, longestLog.Log, longestLog.OpNumber,
		highestCommit)

	for _, peer := range r.peers() {
		r.sendPeer(peer, &StartView{
			ViewNumber:   r.state.ViewNumber,
			Log:          r.state.Log,
			OpNumber:     r.state.OpNumber,
			CommitNumber: r.state.CommitNumber,
		})
	}
}

func (r *Replica) handleStartView(_ ReplicaConfiguration, req *StartView) {
	if req.ViewNumber < r.state.ViewNumber ||
		(req.ViewNumber == r.state.ViewNumber && r.state.Status == StatusNormal) {
		return
	}

	r.startNormal(req.ViewNumber, RoleBackup, req.Log, req.OpNumber,
		req.CommitNumber)
}
