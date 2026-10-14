package vr

import (
	"bytes"
	"fmt"
	"testing"
)

type TestingCluster struct {
	replicas []*Replica
	state    map[ReplicaNumber][]Payload
}

func NewTestingCluster() *TestingCluster {
	return &TestingCluster{
		state: map[ReplicaNumber][]Payload{},
	}
}

func (tc *TestingCluster) UpCallFor(i ReplicaNumber) UpCall {
	return func(req Payload) Payload {
		tc.state[i] = append(tc.state[i], req)
		return nil
	}
}

func (tc *TestingCluster) Transport() chan<- bool {
	publicStop := make(chan bool, 1)
	go func() {
		stops := []chan bool{}
		for _, replica := range tc.replicas {
			stop := make(chan bool, 1)
			stops = append(stops, stop)
			go func(stop chan bool, replica *Replica, id ReplicaNumber) {
				config := replica.state.ClusterMembers[id]
				for {
					select {
					case outgoing := <-replica.outgoing:
						targetReplica := tc.replicas[outgoing.Target.ID]
						targetReplica.incoming <- Incoming{
							From: config,
							Req:  outgoing.Req,
						}
					case <-stop:
						return
					}
				}
			}(stop, replica, replica.state.ReplicaNumber)
		}

		<-publicStop
		for _, stop := range stops {
			<-stop
		}
	}()

	return publicStop
}

type CleanUp func()

func createCluster(members int) (*TestingCluster, CleanUp) {
	config := []ReplicaConfiguration{}
	for i := range members {
		config = append(config, ReplicaConfiguration{
			ID:       ReplicaNumber(i),
			Hostname: fmt.Sprintf("%d", i),
		})
	}

	tc := NewTestingCluster()
	replicas := []*Replica{}
	for i := range members {
		id := ReplicaNumber(i)
		replicas = append(replicas, NewReplica(id, config, tc.UpCallFor(id)))
	}

	replicas[0].state.Role = RolePrimary
	replicas[1].state.Role = RoleBackup
	replicas[2].state.Role = RoleBackup

	go replicas[0].Run()
	go replicas[1].Run()
	go replicas[2].Run()

	tc.replicas = replicas
	transportStop := tc.Transport()
	return tc, func() {
		replicas[0].Stop()
		replicas[1].Stop()
		replicas[2].Stop()
		transportStop <- true
	}
}

func logContains(t *testing.T, r *Replica, req ProcessRequest) {
	for _, entry := range r.state.Log {
		if entry.RequestNumber != req.RequestNumber {
			continue
		}

		if !bytes.Equal(entry.Operation, req.Operation) {
			t.Error("correct entry but wrong content")
		}

		return
	}

	t.Errorf("replica %d did not have log entry", r.state.ReplicaNumber)
}

func TestProcess(t *testing.T) {
	tc, cleanup := createCluster(3)

	operation := []byte("test1")
	req := ProcessRequest{
		RequestNumber: 123,
		ClientId:      555,
		Operation:     operation,
	}
	replyChan := tc.replicas[0].Request(req)
	reply := <-replyChan

	cleanup()

	if reply.RequestNumber != 123 {
		t.Error("req number failed")
	}
	if bytes.Equal(reply.Reply, operation) {
		t.Error("operation failed")
	}

	for _, r := range tc.replicas {
		if r.state.OpNumber != 1 {
			t.Errorf("wrong commit number %d at replica %d", r.state.CommitNumber, r.state.ReplicaNumber)
		}
		logContains(t, r, req)
	}

	primary := tc.replicas[0]
	if primary.state.CommitNumber != 1 {
		t.Errorf("primary did not commit")
	}
}
