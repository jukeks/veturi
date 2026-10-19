package vr

import (
	"testing"
)

func NewTestReplica(id ReplicaNumber, role ReplicaRole, configs []ReplicaConfiguration) *Replica {
	r := NewReplica(id, configs, func(_ Payload) Payload { return nil })
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

	output1 := backup.Step(req2)
	output2 := backup.Step(req1)

	if len(output1) != 0 {
		t.Errorf("output1 had len %d", len(output1))
	}
	if len(output2) != 1 {
		t.Errorf("output2 had len %d", len(output2))
	}
	resp := output2[0]
	prepareOk, ok := resp.Req.(*PrepareOK)
	if !ok {
		t.Errorf("req was not PrepareOK: %v", resp.Req)
	}

	if prepareOk.OpNumber != 2 {
		t.Errorf("backup did not ok both ops: %d", prepareOk.OpNumber)
	}

}
