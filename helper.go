package dbresolver

import (
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

	arrErrs := []error{}
	for err := range errors {
		if err != nil {
			arrErrs = append(arrErrs, err)
		}
	}

	return multierr.Combine(arrErrs...)
}
