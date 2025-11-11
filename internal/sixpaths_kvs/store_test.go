package sixpaths_kvs

import (
	"fmt"
	"testing"
)

func TestNewStore(t *testing.T) {

	s, err := NewStore()
	if err != nil {

	}

	fmt.Println(s)
}
