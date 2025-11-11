package sixpaths_kvs

import "testing"

// TODO: Test Dedup functionality, test out of order seq, deleting nonexistent keys

func TestApplyPut(t *testing.T) {
	s, _ := NewStore()

	s.lastlogi = 0

	com := Command{
		Instruct: CmdPut,
		ClientID: "00001",
		Seq:      1,
		Key:      []byte("Alpha"),
		Value:    []byte("Beta"),
	}
	out, err := s.Apply(com, 1)

	if err != nil {
		t.Fatalf("Apply put: %v", err)
	}

	if !out.Success {
		t.Fatal("Apply Put() success = false")
	}

	if out.LogIndex != 1 {
		t.Fatalf("Apply Put() logindex = %d, logindex=1 expected.", out.LogIndex)
	}

	v, ok := s.kv[string(com.Key)]
	if !ok {
		t.Fatal("No value in map at key: 'Alpha'")
	}

	if string(v) != "Beta" {
		t.Fatal("Wrong value in map at key: 'Alpha'")
	}

	if len(out.PrevValue) != 0 {
		t.Fatalf("PrevValue=%q, want empty", out.PrevValue)
	}

	com2 := Command{
		Instruct: CmdPut,
		ClientID: "00001",
		Seq:      2,
		Key:      []byte("Alpha"),
		Value:    []byte("Omega"),
	}
	out, err = s.Apply(com2, 2)

	if err != nil {
		t.Fatalf("Apply put: %v", err)
	}

	if !out.Success {
		t.Fatal("Apply Put() success = false")
	}

	if out.LogIndex != 2 {
		t.Fatalf("Apply Put() logindex = %d, logindex=1 expected.", out.LogIndex)
	}

	get, err := s.Get(string(com2.Key))

	if err != nil {
		t.Fatalf("Error with get %v", err)
	}
	if string(get) != "Omega" {
		t.Fatalf("Wrong value in map at key: 'Alpha', expected value: 'Omega', actual val: %v", get)
	}
}

func TestApplyDelete(t *testing.T) {
	s, _ := NewStore()

	s.lastlogi = 0

	com := Command{
		Instruct: CmdPut,
		ClientID: "00001",
		Seq:      1,
		Key:      []byte("Alpha"),
		Value:    []byte("Beta"),
	}
	_, err := s.Apply(com, 1)

	if err != nil {
		t.Fatal("Error with inserting new mapping in map.")
	}

	com2 := Command{
		Instruct: CmdDelete,
		ClientID: "00001",
		Seq:      2,
		Key:      []byte("Alpha"),
		Value:    []byte(""),
	}
	out, err := s.Apply(com2, 2)

	if err != nil {
		t.Fatalf("error: %v", err)
	}

	if !out.Success {
		t.Fatal("Apply Delete() success = false")
	}

	v, ok := s.kv[string(com2.Key)]

	if ok {
		t.Fatalf("Delete() operation failed, value at key: %v", v)
	}

}
