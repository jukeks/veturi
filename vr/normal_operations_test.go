package vr

import (
	"testing"
)

func NewTestReplica(id ReplicaNumber, role ReplicaRole, configs []ReplicaConfiguration) *Replica {
	r := NewReplica(id, configs, func(_ Payload) Payload { return nil }, 42)
	r.state.Role = role
	r.state.Status = StatusNormal

	return r
}

func TestGapsInPrepareOK(t *testing.T) {
	configs := []ReplicaConfiguration{
		{ID: 1, Hostname: "1"},
		{ID: 2, Hostname: "2"},
	}
	primary := NewTestReplica(0, RolePrimary, configs)
	backup := NewTestReplica(1, RoleBackup, configs)

	req1 := Incoming{
		From: PeerCorrespondent(primary.state.GetConfiguration()),
		Req: &Prepare{
			ViewNumber: 0,
			Operation: ProcessRequest{
				RequestNumber: 1,
				ClientId:      555,
				Operation:     []byte("req1"),
			},
			OpNumber:     1,
			CommitNumber: 0,
		},
	}
	req2 := Incoming{
		From: PeerCorrespondent(primary.state.GetConfiguration()),
		Req: &Prepare{
			ViewNumber: 0,
			Operation: ProcessRequest{
				RequestNumber: 1,
				ClientId:      555,
				Operation:     []byte("req2"),
			},
			OpNumber:     2,
			CommitNumber: 0,
		},
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
	prepareOk, ok := resp.Req.(*PrepareOK)
	if !ok {
		t.Errorf("req was not PrepareOK: %v", resp.Req)
	}

	if prepareOk.OpNumber != 2 {
		t.Errorf("backup did not ok both ops: %d", prepareOk.OpNumber)
	}
}
