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

	// seq := []int{1, 2, 9, 4, 25, 6, 49, 8} // this is the expected end result
	want := []int{1, 2, 9, 4, 25, 6, 49, 8}
	for i, wanted := range want {
		if wanted != seq[i] {
			t.Errorf("Wrong value at position %d. Want: %d, Got: %d", i, wanted, seq[i])
		}
	}

}

/*=== RUN   TestParallelFunction
    helper_test.go:28: Wrong value at position 2. Want: 3, Got: 9
    helper_test.go:34: Wrong value at position 2. Want: 3, Got: 9
end
--- FAIL: TestParallelFunction (0.00s)

FAIL*/
