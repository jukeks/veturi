package vr

import "testing"

// A PrepareOK is a vote in one view. After a view change, the new log can
// have a different op at the same op-number, so the votes from an older
// view must not count.
//
// Replica 1 votes for op "a" in view 0. In view 5, replica 0 is primary
// again, with op "b" from replica 3. Only replicas 0 and 3 have "b". If
// the old vote of replica 1 counts, "b" commits with 2 of 5 replicas, and
// a later view change with replicas 1, 2 and 4 can lose it.
func TestVotesFromOlderViewDoNotCount(t *testing.T) {
	members := []ReplicaNumber{0, 1, 2, 3, 4}
	p := NewTestReplica(0, RolePrimary, members)

	p.Step(&ClientRequest{RequestNumber: 1, From: 9, Operation: Payload("a")})
	p.Step(&PrepareOK{ViewNumber: 0, OpNumber: 1, From: 1})
	if p.state.CommitNumber != 0 {
		t.Fatalf("commit %d in view 0 with 2 of 5 votes, want 0", p.state.CommitNumber)
	}

	// View change to view 5. Replica 0 is its primary (5 % 5).
	p.Step(&StartViewChange{ViewNumber: 5, From: 2})
	b := &ClientRequest{RequestNumber: 1, From: 8, Operation: Payload("b")}
	p.Step(&DoViewChange{ViewNumber: 5, From: 3, LastView: 4,
		Log: []*ClientRequest{b}, OpNumber: 1, CommitNumber: 0})
	p.Step(&DoViewChange{ViewNumber: 5, From: 2, LastView: 0})
	p.Step(&DoViewChange{ViewNumber: 5, From: 4, LastView: 0})
	if p.state.Role != RolePrimary || p.state.Status != StatusNormal || p.state.ViewNumber != 5 {
		t.Fatalf("replica 0: view %d role %d status %d, want normal primary of view 5",
			p.state.ViewNumber, p.state.Role, p.state.Status)
	}
	if string(p.state.Log[0].Operation) != "b" {
		t.Fatalf("op 1 is %q, want b", p.state.Log[0].Operation)
	}

	p.Step(&PrepareOK{ViewNumber: 5, OpNumber: 1, From: 3})
	if p.state.CommitNumber != 0 {
		t.Fatalf("commit %d with votes from 0 and 3 in view 5: the vote of 1 from view 0 counted",
			p.state.CommitNumber)
	}
	p.Step(&PrepareOK{ViewNumber: 5, OpNumber: 1, From: 2})
	if p.state.CommitNumber != 1 {
		t.Fatalf("commit %d with 3 votes in view 5, want 1", p.state.CommitNumber)
	}
}

// The primary sends a Prepare again only after PrepareOkTimeout ticks with
// no progress, and then waits PrepareOkTimeout ticks again.
func TestPrepareResendOncePerTimeout(t *testing.T) {
	p := NewTestReplica(0, RolePrimary, testMembers())
	p.Step(&ClientRequest{RequestNumber: 1, From: 9, Operation: Payload("a")})

	resends := 0
	for range 2 * PrepareOkTimeout {
		for _, o := range p.Tick() {
			if _, ok := o.Msg.(*Prepare); ok && o.To.Peer == 1 {
				resends++
			}
		}
	}
	if resends != 2 {
		t.Errorf("%d resends to replica 1 in %d ticks, want 2", resends, 2*PrepareOkTimeout)
	}
}

// Only the primary counts votes. A backup that gets a PrepareOK must not
// commit, because it does not know which replicas have its ops.
func TestBackupIgnoresPrepareOK(t *testing.T) {
	b := NewTestReplica(1, RoleBackup, testMembers())
	b.Step(getTestPrepare(0, 1, 0))
	b.Step(&PrepareOK{ViewNumber: 0, OpNumber: 1, From: 2})
	if b.state.CommitNumber != 0 {
		t.Errorf("backup commit %d after a PrepareOK, want 0", b.state.CommitNumber)
	}
}

// An old PrepareOK that arrives late must not move the vote of a replica
// back. Otherwise the primary sends ops again that the replica has.
func TestLatePrepareOKDoesNotMoveVoteBack(t *testing.T) {
	members := []ReplicaNumber{0, 1, 2, 3, 4}
	p := NewTestReplica(0, RolePrimary, members)
	for req := range RequestNumber(2) {
		p.Step(&ClientRequest{RequestNumber: req + 1, From: 9, Operation: Payload("x")})
	}
	p.Step(&PrepareOK{ViewNumber: 0, OpNumber: 2, From: 1})
	p.Step(&PrepareOK{ViewNumber: 0, OpNumber: 1, From: 1})

	for range PrepareOkTimeout {
		for _, o := range p.Tick() {
			if _, ok := o.Msg.(*Prepare); ok && o.To.Peer == 1 {
				t.Errorf("primary sends op %d again to replica 1, which has ops 1 and 2",
					o.Msg.(*Prepare).OpNumber)
			}
		}
	}
}
