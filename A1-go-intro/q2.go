package cos418_hw1_1

import (
	"bufio"
	"io"
	"strconv"
)

// Sum numbers from channel `nums` and output sum to `out`.
// You should only output to `out` once.
// Do NOT modify function signature.
func sumWorker(nums chan int, out chan int) {
	// TODO: implement me
	// HINT: use for loop over `nums`
  sum := 0
  for num := range nums {
    sum += num
  }
  out <- sum
}

// Read integers from the file `fileName` and return sum of all values.
// This function must launch `num` go routines running
// `sumWorker` to find the sum of the values concurrently.
// You should use `checkError` to handle potential errors.
// Do NOT modify function signature.
func sum(num int, fileName string) int {
	// TODO: implement me
	// HINT: use `readInts` and `sumWorkers`
	// HINT: used buffered channels for splitting numbers between workers

  // Read nums from `fileName`
  // TODO: implement this part with a Reader
  ints := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

  workerSize := len(ints) / num

  // Create channels to communicate with workers
  inChan := make(chan int)
  workerChans := make([]chan int, num)
  for i := range workerChans {
    workerChans[i] = make(chan int, workerSize)
  }

  // Divide up work, computing the sum among `num` workers
  for i := range workerChans {
    go sumWorker(workerChans[i], inChan)
  }

  // Distribute the ints over the workers to be summed
  for _, n := range ints {
    worker := n % num
    workerChans[worker] <- n
  }

  for i := range workerChans {
    close(workerChans[i])
  }

  // Add up sums computed by each worker
  sum := 0
  for i := 0; i < num; i++ {
    sum += <-inChan
  }

  return sum
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
