package vr

import (
	"log/slog"
	"slices"
)

func (r *Replica) handleRecovery(msg *Recovery) {
	if r.state.Status != StatusNormal {
		slog.Info("can't process recovery yet, not in normal state")
		return
	}
	if r.state.Role != RolePrimary {
		r.sendPeer(msg.From, &RecoveryResponse{
			ViewNumber:   r.state.ViewNumber,
			Nonce:        msg.Nonce,
			Log:          nil,
			OpNumber:     0,
			CommitNumber: 0,
			IsPrimary:    false,
			From:         r.state.ReplicaNumber,
		})
		return
	}

	r.sendPeer(msg.From, &RecoveryResponse{
		ViewNumber:   r.state.ViewNumber,
		Nonce:        msg.Nonce,
		Log:          slices.Clone(r.state.Log),
		OpNumber:     r.state.OpNumber,
		CommitNumber: r.state.CommitNumber,
		IsPrimary:    true,
		From:         r.state.ReplicaNumber,
	})
}

func (r *Replica) handleRecoveryResponse(msg *RecoveryResponse) {
	if r.state.Status != StatusRecovering {
		slog.Info("no longer recovering")
		return
	}
	if msg.Nonce != r.recoveryNonce {
		slog.Info("got recovery response for other nonce")
		return
	}

	r.recoveryResponses[msg.From] = msg

	if len(r.recoveryResponses) < r.quorum() {
		return
	}

	highestView := ViewNumber(0)
	for _, resp := range r.recoveryResponses {
		if resp.ViewNumber > highestView {
			highestView = resp.ViewNumber
		}
	}

	var primaryResponse *RecoveryResponse = nil
	for _, resp := range r.recoveryResponses {
		if resp.ViewNumber != highestView {
			continue
		}
		if resp.IsPrimary {
			primaryResponse = resp
		}
	}

	if primaryResponse == nil {
		slog.Info("no primary recovery-response in latest view despite quorum")
		return
	}
	clear(r.recoveryResponses)

	r.startNormal(primaryResponse.ViewNumber, RoleBackup, primaryResponse.Log,
		primaryResponse.OpNumber, primaryResponse.CommitNumber)
}

func (r *Replica) startRecovery() {
	r.recoveryNonce = Nonce(r.rand.Uint64())
	r.state.Status = StatusRecovering
	r.requestRecovery()
}

func (r *Replica) requestRecovery() {
	r.ticksSinceRecoveryRequested = 0
	for _, peer := range r.peers() {
		r.sendPeer(peer, &Recovery{
			From:  r.state.ReplicaNumber,
			Nonce: r.recoveryNonce,
		})
	}
}
