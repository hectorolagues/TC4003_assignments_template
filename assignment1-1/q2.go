//********************************************************
//********************************************************
//*** Name: Hector Gabriel Olagues Torres
//*** UserID: hectorolagues
//*** Assignment 1-1, Q2 - Parallel sum: intended to sum a
//*** list of numbers in a file in parallel.
//********************************************************
//********************************************************

package cos418_hw1_1

import (
	"bufio"
	"io"
	"os"
	"strconv"
)

// Sum numbers from channel `nums` and output sum to `out`.
// You should only output to `out` once.
// Do NOT modify function signature.
func sumWorker(nums chan int, out chan int) {
	total := 0
	for number := range nums {
		total += number
	}
	out <- total
}

// Read integers from the file `fileName` and return sum of all values.
// This function must launch `num` go routines running
// `sumWorker` to find the sum of the values concurrently.
// You should use `checkError` to handle potential errors.
// Do NOT modify function signature.
func sum(num int, fileName string) int {
	// Open the file stored in fileName and check for errors.
	file, err := os.Open(fileName)
	checkError(err)
	// Close the file when returning from this function, by using keyword defer.
	defer file.Close()

	integers, err := readInts(file)
	checkError(err)

	// Create buffered channels
	// The length of numbers is divided by the number of go routines or workers in chNums, in order to split numbers between workers.
	// chOut has a buffer length equal to the number of go routines or workers.
	chNums := make(chan int, len(integers)/num)
	chOut := make(chan int, num)

	// Sum the values concurrently with go routines
	for i := 0; i < num; i++ {
		go sumWorker(chNums, chOut)
	}

	// Send values from integers list to channel chNums
	for _, val := range integers {
		chNums <- val
	}
	close(chNums)

	// Total receives from channel chOut the sum calculated in go routines
	total := 0
	for i := 0; i < num; i++ {
		total += <-chOut
	}
	close(chOut)

	// Returns the total amount calculated concurrently in sumWorker
	return total
}

// Read a list of integers separated by whitespace from `r`.
// Return the integers successfully read with no error, or
// an empty slice of integers and the error that occurred.
// Do NOT modify this function.
func readInts(r io.Reader) ([]int, error) {
	scanner := bufio.NewScanner(r)
	scanner.Split(bufio.ScanWords)
	var elems []int
	for scanner.Scan() {
		val, err := strconv.Atoi(scanner.Text())
		if err != nil {
			return elems, err
		}
		elems = append(elems, val)
	}
	return elems, nil
}
