package mapreduce

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	debug("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO

	// Store incomplete tasks on a channel. Failed tasks will be re-added
	// to this channel to indicate that they need to be run again.
	//
	// We will know all tasks are done when this channel is closed. The
	// worker that completes the last task will close this channel.
	incompleteTasks := make(chan int, ntasks)
	for taskNum := 0; taskNum < ntasks; taskNum++ {
		incompleteTasks <- taskNum
	}

	// Store number of completed tasks on a channel (so goroutines can
	// access it asynchronously). Workers check this value to determine
	// when all tasks are done.
	nCompletedTasks := make(chan int, 1)
	nCompletedTasks <- 0

	//-------------------------------------------

	// Map phase
	if phase == mapPhase {
		// Assign each task to a worker
		for i := range incompleteTasks {
			// Set up args for worker
			worker := <-mr.registerChannel
			taskNum := i
			args := DoTaskArgs{
				mr.jobName,
				mr.files[taskNum], // input filename
				phase,
				taskNum,
				nios, // number of intermediate output files
			}

			// Start worker
			go func() {
				ok := call(worker, "Worker.DoTask", args, nil)
				if ok == false {
					// Worker failed. Put the failed task back on incompleteTasks
					incompleteTasks <- taskNum
				} else {
					// Worker completed successfully. Check if all tasks are completed
					nOld := <-nCompletedTasks
					nNew := nOld + 1
					if nNew == ntasks {
						close(incompleteTasks) // indicates no more tasks to be done
					} else {
						nCompletedTasks <- nNew
					}

					// Put worker back on "idle" channel
					mr.registerChannel <- worker
				}
			}()
		}
	}

	//-------------------------------------------

	// Reduce phase
	if phase == reducePhase {
		// Assign eah task to a worker
		for i := range incompleteTasks {
			// Set up args for worker
			worker := <-mr.registerChannel
			taskNum := i
			args := DoTaskArgs{
				mr.jobName,
				"", // no filename
				phase,
				taskNum,
				nios, // number of original input files
			}

			// Start worker
			go func() {
				ok := call(worker, "Worker.DoTask", args, nil)
				if ok == false {
					// Worker failed. Put the failed task back on incompleteTasks
					incompleteTasks <- taskNum
				} else {
					// Worker completed successfully. Check if all tasks are completed
					nOld := <-nCompletedTasks
					nNew := nOld + 1
					if nNew == ntasks {
						close(incompleteTasks) // indicates no more tasks to be done
					} else {
						nCompletedTasks <- nNew
					}

					// Put worker back on "idle" channel
					mr.registerChannel <- worker
				}
			}()
		}
	}

	debug("Schedule: %v phase done\n", phase)
}
