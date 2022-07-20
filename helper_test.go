package dbresolver

import (
	"fmt"
	"runtime"
	"testing"
)

func TestParallelFunction(t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU())

	seq := []int{1, 2, 3, 4, 5, 6, 7, 8}
	err := doParallely(len(seq), func(i int) error {
		if seq[i]%2 == 1 {
			seq[i] *= seq[i]
			return nil
		}
		return fmt.Errorf("%d is an even number", seq[i])
	})

	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	want := []int{1, 2, 9, 4, 25, 6, 49, 8}
	for i := range want {
		if want[i] != seq[i] {
			t.Errorf("Wrong value at position %d. Want: %d, Got: %d", i, want[i], seq[i])
		}
	}
}
