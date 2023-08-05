// sum_test.go
package main

import "testing"

func Sum(x int, y int) int {
	return x + y
}

func TestSum(t *testing.T) {
	total := Sum(5, 5)
	if total != 10 {
		t.Errorf("Sum was incorrect, got: %d, want: %d.", total, 10)
	}
}
