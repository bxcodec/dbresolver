package dbresolver

import (
	"fmt"
	"log"
	"net"
	"sync"

	"go.uber.org/multierr"
)

func doParallely(n int, fn func(i int) error) error {
	errors := make(chan error, n)
	wg := &sync.WaitGroup{}
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			errors <- fn(i)
			wg.Done()
		}(i)
	}

	go func(wg *sync.WaitGroup) {
		wg.Wait()
		close(errors)
	}(wg)

	var arrErrs []error
	for err := range errors {
		if err != nil {
			arrErrs = append(arrErrs, err)
		}
	}

	return multierr.Combine(arrErrs...)
}

func isDBConnectionError(err error) bool {
	fmt.Println("MASUK SINI TANTE?")
	netErr, ok := err.(net.Error)
	if ok {
		if netErr.Timeout() {
			log.Println("connection timed out error")
		} else {
			log.Println("general network error")
		}
		return ok
	}
	_, ok = err.(*net.OpError)
	return ok
}
