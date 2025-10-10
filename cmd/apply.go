package main

import (
	"errors"
)

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
	return t == CmdPut || t == CmdDelete // these are the only valid CommandType nums
}

type ApplyResult struct {
	Success   bool
	PrevValue []byte // To see what was deleted or overwritten
	LogIndex  uint64
}

func (s *Store) Apply(cmd Command, logindex uint64) (ApplyResult, error) {

	r := ApplyResult{
		Success:   false,
		PrevValue: []byte{},
		LogIndex:  logindex,
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if logindex != s.lastlogi+1 {
		return r, errors.New("error: new apply request index is not equal to last log index + 1")
	}

	// we check whether the SEQ num provided by the cmd is the equal (or older) than
	// the last SEQ num provided by this particular client's request.
	// Since SEQ nums are unique per request, if these two are the same
	// then we are dealing with a duplicate request.
	if cmd.Seq <= s.dedupMap[cmd.ClientID].seq {
		r.PrevValue = s.dedupMap[cmd.ClientID].result.PrevValue
		// return previous ApplyResult if we are dealing with a dupe
		return s.dedupMap[string(cmd.Key)].result, nil

	}

	switch cmd.Instruct {
	case CmdPut: // Put
		return r, nil // stub

	case CmdDelete: // Delete
		v, ok := s.kv[string(cmd.Key)]
		if !ok {
			return r, errors.New("error: value at key in store is already empty; nothing to delete")
		}
		// we make a copy of the byte slice to not alter it
		prev := append([]byte(nil), v...)

		delete(s.kv, string(cmd.Key))
		r.PrevValue = prev
		r.Success = true
		s.lastlogi += 1
		return r, nil
	default:
		return r, errors.New("error: Apply failed, invalid cmd passed.")
	}

}
