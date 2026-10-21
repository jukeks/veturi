package vr

import (
	"testing"
)

func NewTestReplica(id ReplicaNumber, role ReplicaRole, members []ReplicaNumber) *Replica {
	r := NewReplica(id, members, func(_ Payload) Payload { return nil }, 42, false)
	r.state.Role = role
	r.state.Status = StatusNormal

	return r
}

func TestGapsInPrepareOK(t *testing.T) {
	backup := NewTestReplica(1, RoleBackup, testMembers())

	req1 := &Prepare{
		ViewNumber: 0,
		Operation: ClientRequest{
			RequestNumber: 1,
			From:          555,
			Operation:     []byte("req1"),
		},
		OpNumber:     1,
		CommitNumber: 0,
	}
	req2 := &Prepare{
		ViewNumber: 0,
		Operation: ClientRequest{
			RequestNumber: 1,
			From:          555,
			Operation:     []byte("req2"),
		},
		OpNumber:     2,
		CommitNumber: 0,
	}

	afterReq2 := backup.Step(req2)
	afterReq1 := backup.Step(req1)

	if len(afterReq2) != 0 {
		t.Errorf("afterReq2 had len %d", len(afterReq2))
	}
	if len(afterReq1) != 1 {
		t.Errorf("afterReq1 had len %d", len(afterReq1))
	}
	resp := afterReq1[0]
	prepareOk, ok := resp.Msg.(*PrepareOK)
	if !ok {
		t.Errorf("req was not PrepareOK: %v", resp.Msg)
	}

	if prepareOk.OpNumber != 2 {
		t.Errorf("backup did not ok both ops: %d", prepareOk.OpNumber)
	}
}
