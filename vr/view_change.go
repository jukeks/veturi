package vr

import "time"

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

func (r *Replica) initiateViewChange() {
	r.state.Status = StatusViewChange
	r.state.ViewNumber++

	for _, peer := range r.peers() {
		r.sendPeer(peer, &StartViewChange{
			ViewNumber:    r.state.ViewNumber,
			ReplicaNumber: r.state.ReplicaNumber,
		})
	}
}

func (r *Replica) getPrimaryForView(view ViewNumber) ReplicaConfiguration {
	return r.state.ClusterMembers[int(view)%len(r.state.ClusterMembers)]
}

func (r *Replica) handleStartViewChange(from ReplicaConfiguration, req *StartViewChange) {
	viewChange, ok := r.viewChanges[req.ViewNumber]
	if !ok {
		viewChange = NewViewChange()
		r.viewChanges[req.ViewNumber] = viewChange
	}
	viewChange.StartViewChanges[from.ID] = req

	if req.ViewNumber > r.state.ViewNumber {
		r.state.Status = StatusViewChange
		r.state.ViewNumber = req.ViewNumber

		for _, peer := range r.peers() {
			r.sendPeer(peer, &StartViewChange{
				ViewNumber:    r.state.ViewNumber,
				ReplicaNumber: r.state.ReplicaNumber,
			})
		}
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
		r.state.Status = StatusViewChange
		r.state.ViewNumber = req.ViewNumber

		for _, peer := range r.peers() {
			r.sendPeer(peer, &StartViewChange{
				ViewNumber:    r.state.ViewNumber,
				ReplicaNumber: r.state.ReplicaNumber,
			})
		}
	}

	viewChange, ok := r.viewChanges[req.ViewNumber]
	if !ok {
		viewChange = NewViewChange()
		r.viewChanges[req.ViewNumber] = viewChange
	}

	viewChange.DoViewChanges[req.ReplicaNumber] = req
	if len(viewChange.DoViewChanges) < r.quorum() {
		return
	}

	r.state.ViewNumber = req.ViewNumber
	r.state.Status = StatusNormal
	r.state.Role = RolePrimary
	r.primaryAlive = time.Now()

	dos := viewChange.DoViewChanges
	keys := make([]ReplicaNumber, 0, len(viewChange.DoViewChanges))
	for rn := range viewChange.DoViewChanges {
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

	highest := dosFromLastView[0]
	for _, do := range dosFromLastView {
		if do.OpNumber > highest.OpNumber {
			highest = do
		}
	}

	lastCommit := OpNumber(r.state.CommitNumber)
	r.state.OpNumber = highest.OpNumber
	r.state.CommitNumber = highest.CommitNumber
	r.state.Log = highest.Log

	if len(r.state.Log) > 0 {
		for cur := lastCommit; cur <= OpNumber(r.state.CommitNumber); cur++ {
			originalReq := r.state.Log[cur-1]
			response := r.upCall(originalReq.Operation)

			client := r.state.ClientTable[originalReq.ClientId]
			client.LatestRequestNumber = originalReq.RequestNumber
			client.LatestResult = response
		}
	}

	for _, peer := range r.peers() {
		r.sendPeer(peer, &StartView{
			ViewNumber:   r.state.ViewNumber,
			Log:          r.state.Log,
			OpNumber:     r.state.OpNumber,
			CommitNumber: r.state.CommitNumber,
		})
	}
}

func (r *Replica) handleStartView(from ReplicaConfiguration, req *StartView) {
	r.state.ViewNumber = req.ViewNumber
	r.state.Status = StatusNormal
	r.state.Role = RoleBackup
	r.primaryAlive = time.Now()
	r.state.Log = req.Log
	r.state.OpNumber = req.OpNumber

	if len(r.state.Log) > 0 {
		for cur := OpNumber(r.state.CommitNumber); cur <= OpNumber(req.CommitNumber); cur++ {
			originalReq := r.state.Log[cur-1]
			response := r.upCall(originalReq.Operation)

			client := r.state.ClientTable[originalReq.ClientId]
			client.LatestRequestNumber = originalReq.RequestNumber
			client.LatestResult = response
			r.sendPeer(from, &PrepareOK{
				ViewNumber: r.state.ViewNumber,
				OpNumber:   cur,
				BackupId:   r.state.ReplicaNumber,
			})
		}
	}

	r.state.CommitNumber = req.CommitNumber
}
