package sixpaths_kvs

import "testing"

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

}
