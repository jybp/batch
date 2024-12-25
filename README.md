# Batch 

Package `batch` provides a way to run a fixed number of goroutines concurrently. Goroutines are executed in batches, allowing for error handling and result processing after each batch.  
It has a similar API to [errgroup.Group](https://pkg.go.dev/golang.org/x/sync/errgroup#Group). The difference with [SetLimit](https://pkg.go.dev/golang.org/x/sync/errgroup#Group.SetLimit) is that `Go` will block until all goroutines of the previous batch are finished, while `errgroup` will always schedule a new goroutine when a slot available.

# Usage 

```go
package main

import (
	"fmt"
	"time"

	"github.com/jybp/batch"
)

func main() {
	// Create a new batch with a maximum of 10 goroutines and a callback function.
	bg := batch.New(10, func(results []int) error {
		fmt.Printf("%v\n", results)
		return nil // No error to proceed.
	})

	for i := 0; i < 101; i++ {
		// Some slow tasks that will run concurrently.
		bg.Go(func() (int, error) {
			time.Sleep(time.Second)
			if i == 23 {
				// return -1, fmt.Errorf("error at %d", i)
			}
			return i, nil
		})
	}
	if err := bg.Wait(); err != nil {
		fmt.Printf("Wait: %v\n", err)
	}
}
```
