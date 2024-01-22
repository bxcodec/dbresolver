package dbresolver

import (
	"errors"
	"fmt"
	"go.uber.org/goleak"
	"net"
	"runtime"
	"testing"
)

func TestParallelFunction(t *testing.T) {
	defer goleak.VerifyNone(t)

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

	// this is the expected end result
	want := []int{1, 2, 9, 4, 25, 6, 49, 8}
	for i, wanted := range want {
		if wanted != seq[i] {
			t.Errorf("Wrong value at position %d. Want: %d, Got: %d", i, wanted, seq[i])
		}
	}
}

func TestIsDBConnectionError(t *testing.T) {
	// test connection timeout error
	timeoutError := &net.OpError{Op: "dial", Net: "tcp", Err: &net.DNSError{IsTimeout: true}}
	if !isDBConnectionError(timeoutError) {
		t.Error("Expected true for timeout error")
	}

	// test general network error
	networkError := &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("network error")}
	if !isDBConnectionError(networkError) {
		t.Error("Expected true for network error")
	}

	// test non-network error
	otherError := errors.New("other error")
	if isDBConnectionError(otherError) {
		t.Error("Expected false for non-network error")
	}
}
