package vr

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"
)

type TestingCluster struct {
	replicas []*Replica
	mu       sync.Mutex
	state    map[ReplicaNumber][]Payload
}

func NewTestingCluster() *TestingCluster {
	return &TestingCluster{
		state: map[ReplicaNumber][]Payload{},
	}
}

func (tc *TestingCluster) UpCallFor(i ReplicaNumber) UpCall {
	return func(req Payload) Payload {
		tc.mu.Lock()
		defer tc.mu.Unlock()

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

func createCluster(members int, elected bool) (*TestingCluster, CleanUp) {
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

	if elected {
		replicas[0].state.Role = RolePrimary
		replicas[1].state.Role = RoleBackup
		replicas[2].state.Role = RoleBackup
	}

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
	tc, cleanup := createCluster(3, true)

	start := 123
	reqs := []ProcessRequest{}
	replies := []ProcessReply{}
	for i := range 5 {
		req := ProcessRequest{
			RequestNumber: RequestNumber(start + i),
			ClientId:      555,
			Operation:     []byte(fmt.Sprintf("%d", i)),
		}
		reqs = append(reqs, req)

		replyChan := tc.replicas[0].Request(req)
		reply := <-replyChan
		replies = append(replies, reply)
	}

	cleanup()

	for i := range len(reqs) {
		req := reqs[i]
		reply := replies[i]
		if reply.RequestNumber != req.RequestNumber {
			t.Error("req number failed")
		}

		for _, r := range tc.replicas {
			logContains(t, r, req)
		}
	}

	for _, r := range tc.replicas {
		if r.state.OpNumber != 5 {
			t.Errorf("wrong commit number %d at replica %d", r.state.CommitNumber, r.state.ReplicaNumber)
		}
	}

	primary := tc.replicas[0]
	if primary.state.CommitNumber != 5 {
		t.Errorf("primary did not commit")
	}
}

func TestViewChange(t *testing.T) {
	tc, cleanup := createCluster(3, false)
	time.Sleep(1 * time.Second)
	cleanup()

	primaries := 0
	backups := 0
	for _, peer := range tc.replicas {
		if peer.state.Role == RolePrimary {
			primaries++
		}
		if peer.state.Role == RoleBackup {
			backups++
		}
	}

	if primaries != 1 {
		t.Errorf("not 1 primary: %d", primaries)
	}

	if backups != 2 {
		t.Errorf("not 2 backups: %d", backups)
	}
}
