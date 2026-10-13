package vr

type ReplicaConfiguration struct {
	Hostname string
}

type ReplicaStatus int

const (
	StatusNormal ReplicaStatus = iota
	StatusViewChange
	StatusRecovering
)

type LogEntry struct{}

type Client struct{}

type ReplicaState struct {
	ClusterMembers []ReplicaConfiguration
	ReplicaNumber  uint64
	ViewNumber     uint64
	Status         ReplicaStatus
	OpNumber       uint64
	Log            []LogEntry
	CommitNumber   uint64
	ClientTable    []Client
}

func DefaultReplicaState() ReplicaState {
	return ReplicaState{}
}

type Replica struct {
	state    ReplicaState
	incoming chan Request
	stop     chan bool
	stopped  chan bool
}

func NewReplica(clusterMembers []ReplicaConfiguration) *Replica {
	state := DefaultReplicaState()
	state.ClusterMembers = clusterMembers

	return &Replica{
		state:    state,
		incoming: make(chan Request, 100),
		stop:     make(chan bool, 1),
		stopped:  make(chan bool, 1),
	}
}

func (r *Replica) Run() {
loop:
	for {
		select {
		case <-r.stop:
			break loop
		case req := <-r.incoming:
			r.handleReq(req)
		}
	}

	r.stopped <- true
}

func (r *Replica) Stop() {
	r.stop <- true
	<-r.stopped
}

func (r *Replica) handleReq(iReq Request) {
	switch req := iReq.(type) {
	case *ProcessRequest:
		r.handleProcessRequest(req)
	case *Prepare:
		r.handlePrepare(req)
	case *PrepareOK:
		r.handlePrepareOK(req)
	case *Commit:
		r.handleCommit(req)
	default:
		panic("lol")
	}
}

func (r *Replica) handleProcessRequest(req *ProcessRequest) {

}

func (r *Replica) handlePrepare(req *Prepare) {

}

func (r *Replica) handlePrepareOK(req *PrepareOK) {

}

func (r *Replica) handleCommit(req *Commit) {

}
