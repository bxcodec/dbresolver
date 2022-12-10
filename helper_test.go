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

	want := []int{1, 2, 3, 4, 5, 6, 7, 8}
	for i, wanted := range want {
		if wanted != seq[i] {
			t.Errorf("Wrong value at position %d. Want: %d, Got: %d", i, wanted, seq[i])
		}
	}

	for i := range want { //FIXME bxcodec ? Can u figure why this test is failing at times...
		if want[i] != seq[i] {
			t.Errorf("Wrong value at position %d. Want: %d, Got: %d", i, want[i], seq[i])
		}
	}

	fmt.Println("end")
	//Output: end
}
