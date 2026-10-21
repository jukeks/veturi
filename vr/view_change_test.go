package vr

import "testing"

// A replica can have a higher commit-number than the replicas with the
// newest LastView. For example, it joined a view through state transfer and
// committed ops before NewState set its lastNormalView. The new primary must
// use the highest commit-number of all DoViewChange messages. Otherwise it
// waits for votes for ops that the backups already committed, and those ops
// never commit on the primary.
func TestViewChangeUsesHighestCommit(t *testing.T) {
	configs := testConfigs()
	op1 := &getTestPrepare(0, 1, 0).Operation

	// replica 0 is the primary of view 3
	primary := NewTestReplica(0, RoleBackup, configs)
	primary.state.ViewNumber = 3
	primary.state.Status = StatusViewChange

	from := func(id int) Correspondent { return PeerCorrespondent(configs[id]) }

	// newest LastView, but op 1 is not committed
	primary.Step(Incoming{From: from(0), Req: &DoViewChange{
		ViewNumber: 3, ReplicaNumber: 0, LastView: 2,
		Log: []*ProcessRequest{op1}, OpNumber: 1, CommitNumber: 0,
	}})
	// older LastView, but op 1 is committed
	out := primary.Step(Incoming{From: from(1), Req: &DoViewChange{
		ViewNumber: 3, ReplicaNumber: 1, LastView: 1,
		Log: []*ProcessRequest{op1}, OpNumber: 1, CommitNumber: 1,
	}})

	if primary.state.Role != RolePrimary || primary.state.Status != StatusNormal {
		t.Fatalf("replica 0 role %d status %d, want normal primary",
			primary.state.Role, primary.state.Status)
	}
	if primary.state.CommitNumber != 1 {
		t.Errorf("primary commit %d, want 1", primary.state.CommitNumber)
	}
	if len(primary.inflight) != 0 {
		t.Errorf("primary waits for votes for %d committed ops", len(primary.inflight))
	}
	for _, o := range out {
		if sv, ok := o.Req.(*StartView); ok && sv.CommitNumber != 1 {
			t.Errorf("StartView to %d has commit %d, want 1", o.To.Peer.ID, sv.CommitNumber)
		}
	}
}
