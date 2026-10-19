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
	clients  map[ClientId]TestingClient
	mu       sync.Mutex
	state    map[ReplicaNumber][]Payload
}

type TestingClient struct {
	replyChan chan ProcessReply
}

func NewTestingCluster(clients map[ClientId]TestingClient) *TestingCluster {
	return &TestingCluster{
		state:   map[ReplicaNumber][]Payload{},
		clients: clients,
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
						if outgoing.To.Type == CorrespondentPeer {
							targetReplica := tc.replicas[outgoing.To.Peer.ID]
							targetReplica.incoming <- Incoming{
								From: PeerCorrespondent(config),
								Req:  outgoing.Req,
							}
						} else {
							client := tc.clients[outgoing.To.Client]
							reply := outgoing.Req.(*ProcessReply)
							client.replyChan <- *reply
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

func createCluster(members int, elected bool, clientIds []ClientId) (*TestingCluster, CleanUp) {
	config := []ReplicaConfiguration{}
	for i := range members {
		config = append(config, ReplicaConfiguration{
			ID:       ReplicaNumber(i),
			Hostname: fmt.Sprintf("%d", i),
		})
	}

	clients := map[ClientId]TestingClient{}
	for _, id := range clientIds {
		clients[id] = TestingClient{
			replyChan: make(chan ProcessReply),
		}
	}

	tc := NewTestingCluster(clients)
	replicas := []*Replica{}
	for i := range members {
		id := ReplicaNumber(i)
		replicas = append(replicas, NewReplica(id, config, tc.UpCallFor(id), 42))
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
	clientId := ClientId(555)
	tc, cleanup := createCluster(3, true, []ClientId{clientId})

	start := 123
	reqs := []ProcessRequest{}
	replies := []ProcessReply{}
	for i := range 5 {
		req := ProcessRequest{
			RequestNumber: RequestNumber(start + i),
			ClientId:      clientId,
			Operation:     []byte(fmt.Sprintf("%d", i)),
		}
		reqs = append(reqs, req)

		tc.replicas[0].Request(req)
		replyChan := tc.clients[clientId].replyChan
		select {
		case reply := <-replyChan:
			replies = append(replies, reply)
		case <-time.After(10 * time.Second):
			continue
		}
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

func TestInitialViewChange(t *testing.T) {
	tc, cleanup := createCluster(3, false, nil)
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

func TestViewChange(t *testing.T) {
	tc, cleanup := createCluster(3, false, nil)
	time.Sleep(1 * time.Second)
	for _, replica := range tc.replicas {
		replica.Stop()
	}

	primary := tc.replicas[0]
	for _, replica := range tc.replicas {
		if replica.state.Role == RolePrimary {
			primary = replica
			break
		}
	}

	backups := []*Replica{}
	for _, replica := range tc.replicas {
		if replica != primary {
			go replica.Run()
			backups = append(backups, replica)
		}
	}

	time.Sleep(1 * time.Second)
	var newPrimary *Replica
	for _, replica := range backups {
		replica.Stop()
		if replica.state.Role == RolePrimary {
			newPrimary = replica
			break
		}
	}

	if newPrimary == nil {
		t.Error("no new primary")
	}

	for _, replica := range tc.replicas {
		go replica.Run()
	}
	cleanup()
}
