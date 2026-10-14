package vr

type Request interface {
	request()
}

type ProcessRequest struct {
	RequestNumber RequestNumber
	ClientId      ClientId
	Operation     Payload
}

type ProcessReply struct {
	ViewNumber    ViewNumber
	RequestNumber RequestNumber
	Reply         Payload
}

type (
	Payload       []byte
	RequestNumber uint64
	ViewNumber    uint64
	OpNumber      uint64
	ReplicaNumber uint64
	ClientId      uint64
	CommitNumber  uint64
)

type Prepare struct {
	ViewNumber   ViewNumber
	Operation    ProcessRequest
	OpNumber     OpNumber
	CommitNumber CommitNumber
}

type PrepareOK struct {
	ViewNumber ViewNumber
	OpNumber   OpNumber
	BackupId   ReplicaNumber
}

type Commit struct {
	CommitNumber CommitNumber
	OpNumber     OpNumber
}

type StartViewChange struct {
	ViewNumber    ViewNumber
	ReplicaNumber ReplicaNumber
}

type DoViewChange struct {
	ViewNumber    ViewNumber
	Log           []*ProcessRequest
	LastView      ViewNumber
	OpNumber      OpNumber
	CommitNumber  CommitNumber
	ReplicaNumber ReplicaNumber
}

type StartView struct {
	ViewNumber   ViewNumber
	Log          []*ProcessRequest
	OpNumber     OpNumber
	CommitNumber CommitNumber
}

func (*Prepare) request()         {}
func (*PrepareOK) request()       {}
func (*Commit) request()          {}
func (*ProcessRequest) request()  {}
func (*ProcessReply) request()    {}
func (*StartViewChange) request() {}
func (*DoViewChange) request()    {}
func (*StartView) request()       {}
