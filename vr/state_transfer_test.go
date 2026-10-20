package vr

import (
	"bytes"
	"fmt"
	"testing"
)

func testConfigs() []ReplicaConfiguration {
	return []ReplicaConfiguration{
		{ID: 0, Hostname: "0"},
		{ID: 1, Hostname: "1"},
		{ID: 2, Hostname: "2"},
	}
}

func getTestPrepare(view ViewNumber, op OpNumber, commit CommitNumber) *Prepare {
	return &Prepare{
		ViewNumber:   view,
		OpNumber:     op,
		CommitNumber: commit,
		Operation: ProcessRequest{
			ClientId:      555,
			RequestNumber: RequestNumber(op),
			Operation:     []byte{byte(op)},
		},
	}
}

func getStates(out []Outgoing) []*GetState {
	found := []*GetState{}
	for _, o := range out {
		if gs, ok := o.Req.(*GetState); ok {
			found = append(found, gs)
		}
	}
	return found
}

func TestLaterPrepareDuringStateTransfer(t *testing.T) {
	configs := testConfigs()
	backup := NewTestReplica(1, RoleBackup, configs)
	primary := PeerCorrespondent(configs[2])

	// A Commit from view 2 moves the backup to view 2 and starts state transfer
	out := backup.Step(Incoming{From: primary, Req: &Commit{ViewNumber: 2}})
	if len(getStates(out)) != 1 {
		t.Fatalf("expected one GetState, got %v", out)
	}

	// Prepare for op 5 arrives before NewState, so it is buffered
	backup.Step(Incoming{From: primary, Req: getTestPrepare(2, 5, 0)})

	// NewState brings ops 1..5, including op 5
	log := []*ProcessRequest{}
	for op := OpNumber(1); op <= 5; op++ {
		log = append(log, &getTestPrepare(2, op, 0).Operation)
	}
	backup.Step(Incoming{From: primary, Req: &NewState{
		ViewNumber: 2, Log: log, OpNumber: 5, CommitNumber: 0,
	}})
	if backup.state.OpNumber != 5 {
		t.Fatalf("after NewState: op %d, want 5", backup.state.OpNumber)
	}
	if len(backup.earlyPrepares) != 0 {
		t.Fatalf("after NewState: %d buffered Prepares, want 0", len(backup.earlyPrepares))
	}

	// Op 7 arrives before op 6. Both must end up in the log.
	backup.Step(Incoming{From: primary, Req: getTestPrepare(2, 7, 0)})
	backup.Step(Incoming{From: primary, Req: getTestPrepare(2, 6, 0)})
	if backup.state.OpNumber != 7 {
		t.Fatalf("after ops 7 and 6: op %d, want 7", backup.state.OpNumber)
	}
}

func TestCommitBeyondLogRequestsState(t *testing.T) {
	configs := testConfigs()
	backup := NewTestReplica(1, RoleBackup, configs)
	primary := PeerCorrespondent(configs[0]) // primary of view 0

	backup.Step(Incoming{From: primary, Req: getTestPrepare(0, 1, 0)})
	if backup.state.OpNumber != 1 {
		t.Fatalf("op %d, want 1", backup.state.OpNumber)
	}

	// The primary has committed ops up to 3, but the backup has only op 1.
	out := backup.Step(Incoming{From: primary, Req: &Commit{ViewNumber: 0, CommitNumber: 3}})
	gs := getStates(out)
	if len(gs) != 1 {
		t.Fatalf("expected one GetState, got %v", out)
	}
	if gs[0].OpNumber != 1 || gs[0].ViewNumber != 0 {
		t.Errorf("GetState = %+v, want view 0, op 1", gs[0])
	}
	if backup.state.CommitNumber != 1 {
		t.Errorf("commit %d, want 1 (clamped to own op)", backup.state.CommitNumber)
	}

	// The next tick must not send a second GetState.
	out = backup.Step(Incoming{From: primary, Req: &Commit{ViewNumber: 0, CommitNumber: 3}})
	if n := len(getStates(out)); n != 0 {
		t.Errorf("second Commit sent %d GetState, want 0", n)
	}
}

func TestStateTransferSim(t *testing.T) {
	s := NewSim(t, 3, 42)
	s.RunUntil(s.IsOperational, 100)

	oldPrimary := s.getPrimary()
	s.pause(oldPrimary.state.ReplicaNumber)
	s.RunUntil(s.IsOperational, 100)
	newPrimary := s.getPrimary()

	// Commit some requests in the new view, while the old primary is down.
	clientId := ClientId(555)
	for req := RequestNumber(1); req <= 3; req++ {
		s.Send(ClientCorrespondent(clientId),
			PeerCorrespondent(newPrimary.state.GetConfiguration()),
			&ProcessRequest{
				ClientId:      clientId,
				RequestNumber: req,
				Operation:     []byte(fmt.Sprintf("op %d", req)),
			}, s.now+1)
		s.RunUntil(func() bool { return s.IsCommitted(clientId, req) }, 100)
	}
	if oldPrimary.state.OpNumber != 0 {
		t.Fatalf("old primary op %d before it comes back, want 0", oldPrimary.state.OpNumber)
	}

	// The old primary comes back and must catch up.
	s.resume(oldPrimary.state.ReplicaNumber)
	s.RunUntil(func() bool {
		return oldPrimary.state.ViewNumber == newPrimary.state.ViewNumber &&
			oldPrimary.lastNormalView == newPrimary.state.ViewNumber &&
			oldPrimary.state.OpNumber == newPrimary.state.OpNumber &&
			oldPrimary.state.CommitNumber == newPrimary.state.CommitNumber
	}, 100)

	if oldPrimary.state.Role != RoleBackup {
		t.Errorf("old primary role %d, want backup", oldPrimary.state.Role)
	}

	got := s.executed[oldPrimary.state.ReplicaNumber]
	want := s.executed[newPrimary.state.ReplicaNumber]
	if len(got) != len(want) {
		t.Fatalf("old primary executed %d ops, new primary %d", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Errorf("op %d: old primary executed %q, new primary %q", i+1, got[i], want[i])
		}
	}
}
