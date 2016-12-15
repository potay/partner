package utils

// Function Execution Utility Helpers

import (
	"errors"
	"time"
)

// Executes the function fn for a maximum duration of timelimit. If function
// call takes longer than timelimit to run, it does not wait for the function to
// finish and immediately returns an error, else it returns the result of the
// function call.
func ExecuteWithinTimelimit(fn func() interface{}, timelimit time.Duration) (interface{}, error) {
	retCh := make(chan interface{})
	quitCh := make(chan bool)
	go func(retCh chan interface{}, quitCh chan bool) {
		result := fn()
		select {
		case <-quitCh:
			return
		default:
			retCh <- result
			return
		}
	}(retCh, quitCh)

	// We wait for whichever comes first, function completion or timeout
	select {
	case result := <-retCh:
		return result, nil
	case <-time.After(timelimit):
		go func() {
			quitCh <- true
		}()
		return nil, errors.New("Timed out")
	}
}
