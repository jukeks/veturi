package vr

type ReplicaConfiguration struct {
	Hostname string
}

type ReplicaStatus int

const (
	StateNormal ReplicaStatus = iota
	StateViewChange
	StateRecovering
)

type LogEntry struct{}

type Client struct{}

type Replica struct {
	Replicas      []ReplicaConfiguration
	ReplicaNumber uint64
	ViewNumber    uint64
	Status        ReplicaStatus
	OpNumber      uint64
	Log           []LogEntry
	CommitNumber  uint64
	ClientTable   []Client
}
