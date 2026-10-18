package vr

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
