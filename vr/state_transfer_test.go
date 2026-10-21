package vr

import (
	"fmt"
	"testing"
)

func testMembers() []ReplicaNumber {
	return []ReplicaNumber{0, 1, 2}
}

func getTestPrepare(view ViewNumber, op OpNumber, commit CommitNumber) *Prepare {
	return &Prepare{
		ViewNumber:   view,
		OpNumber:     op,
		CommitNumber: commit,
		Operation: ClientRequest{
			From:          555,
			RequestNumber: RequestNumber(op),
			Operation:     []byte{byte(op)},
		},
	}
}

func getStates(out []Outgoing) []*GetState {
	found := []*GetState{}
	for _, o := range out {
		if gs, ok := o.Msg.(*GetState); ok {
			found = append(found, gs)
		}
	}
	return found
}

func TestLaterPrepareDuringStateTransfer(t *testing.T) {
	members := testMembers()
	backup := NewTestReplica(1, RoleBackup, members)

	// A Commit from view 2 moves the backup to view 2 and starts state transfer
	out := backup.Step(&Commit{ViewNumber: 2})
	if len(getStates(out)) != 1 {
		t.Fatalf("expected one GetState, got %v", out)
	}

	// Prepare for op 5 arrives before NewState, so it is buffered
	backup.Step(getTestPrepare(2, 5, 0))

	// NewState brings ops 1..5, including op 5
	log := []*ClientRequest{}
	for op := OpNumber(1); op <= 5; op++ {
		log = append(log, &getTestPrepare(2, op, 0).Operation)
	}
	backup.Step(&NewState{
		ViewNumber: 2, Log: log, OpNumber: 5, CommitNumber: 0,
	})
	if backup.state.OpNumber != 5 {
		t.Fatalf("after NewState: op %d, want 5", backup.state.OpNumber)
	}
	if len(backup.earlyPrepares) != 0 {
		t.Fatalf("after NewState: %d buffered Prepares, want 0", len(backup.earlyPrepares))
	}

	// Op 7 arrives before op 6. Both must end up in the log.
	backup.Step(getTestPrepare(2, 7, 0))
	backup.Step(getTestPrepare(2, 6, 0))
	if backup.state.OpNumber != 7 {
		t.Fatalf("after ops 7 and 6: op %d, want 7", backup.state.OpNumber)
	}
}

func TestCommitBeyondLogRequestsState(t *testing.T) {
	members := testMembers()
	backup := NewTestReplica(1, RoleBackup, members)

	backup.Step(getTestPrepare(0, 1, 0))
	if backup.state.OpNumber != 1 {
		t.Fatalf("op %d, want 1", backup.state.OpNumber)
	}

	// The primary has committed ops up to 3, but the backup has only op 1.
	out := backup.Step(&Commit{ViewNumber: 0, CommitNumber: 3})
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
	out = backup.Step(&Commit{ViewNumber: 0, CommitNumber: 3})
	if n := len(getStates(out)); n != 0 {
		t.Errorf("second Commit sent %d GetState, want 0", n)
	}
}

// The old primary misses a view change and the requests that are committed
// in the new view. When it comes back, it must catch up.
func stateTransferScenario(t *testing.T, s *Sim, strict bool) {
	s.RunUntil(s.IsOperational, 200)

	oldPrimary := s.getPrimary()
	s.pause(oldPrimary.state.ReplicaNumber)
	s.RunUntil(func() bool {
		p := s.getPrimary()
		return s.IsOperational() && p != nil && p != oldPrimary
	}, 200)

	// Commit some requests in the new view, while the old primary is down.
	clientId := ClientId(555)
	for req := RequestNumber(1); req <= 3; req++ {
		s.RequestUntilCommitted(clientId, req, fmt.Appendf(nil, "op %d", req), 300)
	}
	if oldPrimary.state.OpNumber != 0 {
		t.Fatalf("old primary op %d before it comes back, want 0", oldPrimary.state.OpNumber)
	}

	// The old primary comes back and must catch up.
	s.resume(oldPrimary.state.ReplicaNumber)
	s.RunUntil(s.AllCaughtUp, 300)

	if strict && oldPrimary.state.Role != RoleBackup {
		t.Errorf("old primary role %d, want backup", oldPrimary.state.Role)
	}
}

func TestStateTransfer(t *testing.T) {
	stateTransferScenario(t, NewSim(t, 3, 42), true)
}

func FuzzStateTransfer(f *testing.F) {
	fuzzSim(f, 3, defaultFaults, func(t *testing.T, s *Sim) {
		stateTransferScenario(t, s, false)
	})
}

func TestEarlyPrepareTimeout(t *testing.T) {
	members := testMembers()
	backup := NewTestReplica(1, RoleBackup, members)

	// op = 2, can't process yet
	backup.Step(getTestPrepare(0, 2, 0))

	stateRequested := false
	for range PrimaryTimeoutTicks - 1 {
		gs := getStates(backup.Tick())
		if len(gs) > 0 {
			stateRequested = true
			break
		}
	}

	if !stateRequested {
		t.Error("backup didn't request state for missing prepares")
	}
}

func TestMissedStartView(t *testing.T) {
	members := testMembers()
	backup := NewTestReplica(1, RoleBackup, members)

	backup.Step(&StartViewChange{
		ViewNumber: 2,
		From:       0,
	})
	out := backup.Step(getTestPrepare(2, 1, 0))
	gs := getStates(out)
	if len(gs) == 0 {
		t.Fatal("getstate was not requested")
	}
}
