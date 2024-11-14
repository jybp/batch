package batch_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/jybp/batch"

	"github.com/stretchr/testify/require"
)

func ExampleGroup() {
	bg := batch.New(3, func(res []int, err error) error {
		if err != nil {
			return err // Decide to stop if any error occured.
		}
		fmt.Println(res) // Print the results of the batch.
		return nil       // Proceed to the next batch.
	})
	for i := 0; i < 10; i++ {
		bg.Go(func() (int, error) {
			time.Sleep(time.Millisecond * 10 * time.Duration(i)) // To force consistent output.
			return i, nil
		})
	}
	if err := bg.Wait(); err != nil {
		fmt.Println(err)
	}

	// Output:
	// [0 1 2]
	// [3 4 5]
	// [6 7 8]
	// [9]
}

func TestBatchGroup_Complete(t *testing.T) {
	c := 0
	bg := batch.New(3, func(res []int, err error) error {
		c++
		t.Logf("callback: %v %v", res, err)
		require.NoError(t, err)
		switch c {
		case 1:
			require.ElementsMatch(t, res, []int{0, 1, 2}, c)
		case 2:
			require.ElementsMatch(t, res, []int{3, 4, 5}, c)
		case 3:
			require.ElementsMatch(t, res, []int{6, 7, 8}, c)
		case 4:
			require.ElementsMatch(t, res, []int{9}, c)
		default:
			t.Fatalf("unexpected callback call #%d: %v %v", c, res, err)
		}
		return nil
	})
	for i := 0; i < 10; i++ {
		bg.Go(func() (int, error) {
			if i == 4 {
				// Simulate a slow function in a middle of a batch.
				time.Sleep(time.Millisecond * 100)
			}
			return i, nil
		})
	}
	require.NoError(t, bg.Wait())
	require.Equal(t, 4, c)
}

func TestBatchGroup_GoError_Stop(t *testing.T) {
	c := 0
	bg := batch.New(3, func(res []int, err error) error {
		c++
		t.Logf("callback: %v %v", res, err)
		switch c {
		case 1:
			require.ElementsMatch(t, res, []int{0, 1, 2}, c)
		case 2:
			require.ElementsMatch(t, res, []int{3, 4, 5}, c)
		default:
			t.Fatalf("unexpected callback call #%d: %v %v", c, res, err)
		}
		return err // Do not swallow the error to stop next batches.
	})
	for i := 0; i < 10; i++ {
		bg.Go(func() (int, error) {
			if i == 4 {
				time.Sleep(time.Millisecond * 100)
				return i, fmt.Errorf("error at %d", i)
			}
			return i, nil
		})
	}
	err := bg.Wait()
	t.Logf("bg.Wait: %v", err)
	require.Error(t, bg.Wait())
	require.Equal(t, 2, c)
}

func TestBatchGroup_GoError_Proceed(t *testing.T) {
	c := 0
	bg := batch.New(3, func(res []int, err error) error {
		c++
		t.Logf("callback: %v %v", res, err)
		switch c {
		case 1:
			require.NoError(t, err)
			require.ElementsMatch(t, res, []int{0, 1, 2}, c)
		case 2:
			require.Error(t, err)
			require.ElementsMatch(t, res, []int{3, 4, 5}, c)
		case 3:
			require.NoError(t, err)
			require.ElementsMatch(t, res, []int{6, 7, 8}, c)
		case 4:
			require.NoError(t, err)
			require.ElementsMatch(t, res, []int{9}, c)
		default:
			t.Fatalf("unexpected callback error at callback#%d: %v %v", c, res, err)
		}
		return nil // Swallow the errors to proceed to the next batch.
	})
	for i := 0; i < 10; i++ {
		bg.Go(func() (int, error) {
			if i == 4 {
				time.Sleep(time.Millisecond * 10)
				return i, fmt.Errorf("error at %d", i)
			}
			return i, nil
		})
	}
	require.NoError(t, bg.Wait())
	require.Equal(t, 4, c)
}

func TestBatchGroup_CallbackError(t *testing.T) {
	c := 0
	bg := batch.New(3, func(res []int, err error) error {
		c++
		t.Logf("callback: %v %v", res, err)
		require.NoError(t, err)
		switch c {
		case 1:
			require.ElementsMatch(t, res, []int{0, 1, 2}, c)
			return nil
		case 2:
			require.ElementsMatch(t, res, []int{3, 4, 5}, c)
			return fmt.Errorf("stopping at callback %d", c)
		default:
			t.Fatalf("unexpected callback call #%d: %v %v", c, res, err)
			return fmt.Errorf("unexpected callback call #%d: %v %v", c, res, err)
		}
	})
	for i := 0; i < 10; i++ {
		bg.Go(func() (int, error) {
			if i == 4 {
				time.Sleep(time.Millisecond * 10)
			}
			return i, nil
		})
	}
	err := bg.Wait()
	t.Logf("bg.Wait: %v", err)
	require.Error(t, bg.Wait())
	require.Equal(t, 2, c)
}

func TestBatchGroup_1_at_a_time(t *testing.T) {
	c := 0
	bg := batch.New(1, func(res []int, err error) error {
		c++
		t.Logf("callback: %v %v", res, err)
		require.NoError(t, err)
		switch c {
		case 1:
			require.ElementsMatch(t, res, []int{0}, c)
		case 2:
			require.ElementsMatch(t, res, []int{1}, c)
		case 3:
			require.ElementsMatch(t, res, []int{2}, c)
		default:
			t.Fatalf("unexpected callback call #%d: %v %v", c, res, err)
		}
		return nil
	})
	for i := 0; i < 3; i++ {
		bg.Go(func() (int, error) {
			time.Sleep(time.Millisecond * 10 * time.Duration(i))
			return i, nil
		})
	}
	require.NoError(t, bg.Wait())
	require.Equal(t, 3, c)
}

func TestBatchGroup_several_errors(t *testing.T) {
	c := 0
	bg := batch.New(2, func(res []int, err error) error {
		c++
		t.Logf("callback: %v %v", res, err)
		switch c {
		case 1:
			require.Error(t, err)
			require.ElementsMatch(t, res, []int{0, 1}, c)
			return nil // Swallow the error to proceed to batch #2.
		case 2:
			require.Error(t, err)
			require.ElementsMatch(t, res, []int{2, 3}, c)
			return err // Do not swallow the error to stop.
		default:
			t.Fatalf("unexpected callback call #%d: %v %v", c, res, err)
		}
		return nil
	})
	for i := 0; i < 5; i++ {
		bg.Go(func() (int, error) {
			if i == 0 || i == 2 {
				// error for first and second batch.
				return i, fmt.Errorf("error at %d", i)
			}
			return i, nil
		})
	}
	require.Error(t, bg.Wait())
	require.Equal(t, 2, c)
}
