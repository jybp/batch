package batch_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/jybp/batch/v2"

	"github.com/stretchr/testify/require"
)

func ExampleGroup() {
	bg := batch.New(3, func(res []int) error {
		fmt.Println(res) // Print the results of the batch.
		return nil       // Proceed to the next batch.
	})
	for i := 0; i < 10; i++ {
		bg.Go(func() ([]int, error) {
			time.Sleep(time.Millisecond * 10 * time.Duration(i)) // To force consistent output.
			return []int{i}, nil
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
	bg := batch.New(3, func(res []int) error {
		c++
		t.Logf("callback %d: %v", c, res)
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
			t.Fatalf("unexpected callback call #%d: %v", c, res)
		}
		return nil
	})
	for i := 0; i < 10; i++ {
		bg.Go(func() ([]int, error) {
			if i == 4 {
				// Simulate a slow function in a middle of a batch.
				time.Sleep(time.Millisecond * 100)
			}
			return []int{i}, nil
		})
	}
	require.NoError(t, bg.Wait())
	require.Equal(t, 4, c)
}

func TestBatchGroup_Limit_Higher_Than_Go(t *testing.T) {
	c := 0
	bg := batch.New(4, func(res []int) error {
		c++
		t.Logf("callback %d: %v", c, res)
		switch c {
		case 1:
			require.ElementsMatch(t, res, []int{0, 1}, c)
		default:
			t.Fatalf("unexpected callback call #%d: %v", c, res)
		}
		return nil
	})
	for i := 0; i < 2; i++ {
		bg.Go(func() ([]int, error) {
			return []int{i}, nil
		})
	}
	require.NoError(t, bg.Wait())
	require.Equal(t, 1, c)
}

func TestBatchGroup_CallbackError(t *testing.T) {
	c := 0
	bg := batch.New(3, func(res []int) error {
		c++
		t.Logf("callback %d: %v", c, res)
		switch c {
		case 1:
			require.ElementsMatch(t, res, []int{0, 1, 2}, c)
			return nil
		case 2:
			require.ElementsMatch(t, res, []int{3, 4, 5}, c)
			return fmt.Errorf("stopping at callback %d: %v", c, res)
		default:
			t.Fatalf("unexpected callback call #%d: %v", c, res)
			return fmt.Errorf("unexpected callback call #%d: %v", c, res)
		}
	})
	for i := 0; i < 10; i++ {
		bg.Go(func() ([]int, error) {
			if i == 4 {
				time.Sleep(time.Millisecond * 10)
			}
			return []int{i}, nil
		})
	}
	err := bg.Wait()
	t.Logf("bg.Wait: %v", err)
	require.Error(t, bg.Wait())
	require.Equal(t, 2, c)
}

func TestBatchGroup_1_at_a_time(t *testing.T) {
	c := 0
	bg := batch.New(1, func(res []int) error {
		c++
		t.Logf("callback %d: %v", c, res)
		switch c {
		case 1:
			require.ElementsMatch(t, res, []int{0}, c)
		case 2:
			require.ElementsMatch(t, res, []int{1}, c)
		case 3:
			require.ElementsMatch(t, res, []int{2}, c)
		default:
			t.Fatalf("unexpected callback call #%d: %v", c, res)
			return fmt.Errorf("unexpected callback call #%d: %v", c, res)
		}
		return nil
	})
	for i := 0; i < 3; i++ {
		bg.Go(func() ([]int, error) {
			time.Sleep(time.Millisecond * 10 * time.Duration(i))
			return []int{i}, nil
		})
	}
	require.NoError(t, bg.Wait())
	require.Equal(t, 3, c)
}

func TestBatchGroup_several_errors(t *testing.T) {
	c := 0
	bg := batch.New(3, func(res []int) error {
		c++
		t.Fatalf("unexpected callback call #%d: %v", c, res)
		return fmt.Errorf("unexpected callback call #%d: %v", c, res)
	})
	for i := 0; i < 5; i++ {
		bg.Go(func() ([]int, error) {
			// To force ordered errors.
			time.Sleep(time.Millisecond * 10 * time.Duration(i))
			if i == 0 || i == 2 {
				// error for 1st and 3rd Go.
				return []int{i}, fmt.Errorf("error at %d", i)
			}
			return []int{i}, nil
		})
	}
	err := bg.Wait()
	require.Error(t, err)
	require.Equal(t, "error at 0", err.Error())
	require.Equal(t, 0, c)
}

func TestBatchGroup_Slice_Complete(t *testing.T) {
	c := 0
	bg := batch.New(3, func(res []int) error {
		c++
		t.Logf("callback %d: %v", c, res)
		switch c {
		case 1:
			require.ElementsMatch(t, res, []int{0, 100, 1, 101, 2, 102}, c)
		case 2:
			require.ElementsMatch(t, res, []int{3, 103, 4, 104, 5, 105}, c)
		case 3:
			require.ElementsMatch(t, res, []int{6, 106, 7, 107, 8, 108}, c)
		case 4:
			require.ElementsMatch(t, res, []int{9, 109}, c)
		default:
			t.Fatalf("unexpected callback call #%d: %v", c, res)
		}
		return nil
	})
	for i := 0; i < 10; i++ {
		bg.Go(func() ([]int, error) {
			if i == 4 {
				// Simulate a slow function in a middle of a batch.
				time.Sleep(time.Millisecond * 100)
			}
			return []int{i, i + 100}, nil
		})
	}
	require.NoError(t, bg.Wait())
	require.Equal(t, 4, c)
}

func TestBatchGroup_Slice_Nil(t *testing.T) {
	c := 0
	bg := batch.New(3, func(res []int) error {
		c++
		t.Logf("callback %d: %v", c, res)
		switch c {
		case 1:
			require.ElementsMatch(t, res, []int{0, 100, 1, 101, 2, 102}, c)
		case 2:
			require.ElementsMatch(t, res, []int{3, 103, 5, 105}, c)
		case 3:
			require.ElementsMatch(t, res, []int{6, 106, 7, 107, 8, 108}, c)
		case 4:
			require.ElementsMatch(t, res, []int{9, 109}, c)
		default:
			t.Fatalf("unexpected callback call #%d: %v", c, res)
		}
		return nil
	})
	for i := 0; i < 10; i++ {
		bg.Go(func() ([]int, error) {
			if i == 4 {
				// Simulate a slow function in a middle of a batch.
				time.Sleep(time.Millisecond * 100)
				return nil, nil
			}
			return []int{i, i + 100}, nil
		})
	}
	require.NoError(t, bg.Wait())
	require.Equal(t, 4, c)
}

func TestBatchGroup_Less_Go_Than_Limit(t *testing.T) {
	c := 0
	bg := batch.New(10, func(res []int) error {
		c++
		t.Logf("callback %d: %v", c, res)
		switch c {
		case 1:
			require.ElementsMatch(t, res, []int{0, 100, 1, 101}, c)
		default:
			t.Fatalf("unexpected callback call #%d: %v", c, res)
		}
		return nil
	})
	for i := 0; i < 2; i++ {
		bg.Go(func() ([]int, error) {
			return []int{i, i + 100}, nil
		})
	}
	require.NoError(t, bg.Wait())
	require.Equal(t, 1, c)
}
