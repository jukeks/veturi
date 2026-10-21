package vr

type Message interface {
	message()
}

/* Normal operations */

type ClientRequest struct {
	RequestNumber RequestNumber
	From          ClientId
	Operation     Payload
}

type ClientReply struct {
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
	Nonce         []byte
	Log           []*ClientRequest
)

type Prepare struct {
	ViewNumber   ViewNumber
	Operation    ClientRequest
	OpNumber     OpNumber
	CommitNumber CommitNumber
}

type PrepareOK struct {
	ViewNumber ViewNumber
	OpNumber   OpNumber
	From       ReplicaNumber
}

type Commit struct {
	ViewNumber   ViewNumber
	CommitNumber CommitNumber
}

/* View Changes */

type StartViewChange struct {
	ViewNumber ViewNumber
	From       ReplicaNumber
}

type DoViewChange struct {
	ViewNumber   ViewNumber
	Log          Log
	LastView     ViewNumber
	OpNumber     OpNumber
	CommitNumber CommitNumber
	From         ReplicaNumber
}

type StartView struct {
	ViewNumber   ViewNumber
	Log          Log
	OpNumber     OpNumber
	CommitNumber CommitNumber
}

/* State Transfer */

type GetState struct {
	ViewNumber ViewNumber
	OpNumber   OpNumber
	From       ReplicaNumber
}

type NewState struct {
	ViewNumber   ViewNumber
	Log          Log
	OpNumber     OpNumber
	CommitNumber CommitNumber
}

/* Recovery */

type Recovery struct {
	From  ReplicaNumber
	Nonce Nonce
}

type RecoveryResponse struct {
	ViewNumber   ViewNumber
	Nonce        Nonce
	Log          Log
	OpNumber     OpNumber
	CommitNumber CommitNumber
	From         ReplicaNumber
}

func (*Prepare) message()          {}
func (*PrepareOK) message()        {}
func (*Commit) message()           {}
func (*ClientRequest) message()    {}
func (*ClientReply) message()      {}
func (*StartViewChange) message()  {}
func (*DoViewChange) message()     {}
func (*StartView) message()        {}
func (*GetState) message()         {}
func (*NewState) message()         {}
func (*Recovery) message()         {}
func (*RecoveryResponse) message() {}
