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
	BackupId      uint64
	ClientId      uint64
	CommitNumber  uint64
)

type Prepare struct {
	ViewNumber   ViewNumber
	Operation    Payload
	OpNumber     OpNumber
	CommitNumber CommitNumber
}

type PrepareOK struct {
	ViewNumber ViewNumber
	OpNumber   OpNumber
	BackupId   BackupId
}

type Commit struct {
	CommitNumber CommitNumber
	OpNumber     OpNumber
}

func (*Prepare) request()        {}
func (*PrepareOK) request()      {}
func (*Commit) request()         {}
func (*ProcessRequest) request() {}
func (*ProcessReply) request()   {}
