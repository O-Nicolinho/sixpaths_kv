package main

type Command struct {
	Instruct CommandType
	ClientID string
	Seq      uint64
	Key      []byte
	Value    []byte
}

type CommandType uint8

const (
	CmdUnknown CommandType = iota // invalid or unset
	CmdPut     CommandType = 1    // = 1
	CmdDelete  CommandType = 2    // = 2
)

func validType(t CommandType) bool {
	return t == 1 || t == 2 // these are the only valid CommandType nums
}

func (s *Store) Apply(cmd Command, index uint64) {

}
