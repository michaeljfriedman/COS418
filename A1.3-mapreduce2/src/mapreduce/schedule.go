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

	// Map phase
	if phase == mapPhase {
		// Assign each input file to a worker
		for i := 0; i < ntasks; i++ {
			// Set up args for worker
			worker := <-mr.registerChannel
			taskNum := i
			args := DoTaskArgs{
				mr.jobName,
				mr.files[taskNum],	// input filename
				phase,
				taskNum,
				nios,								// number of intermediate output files
			}

			// Start worker
			go func() {
				ok := call(worker, "Worker.DoTask", args, nil)
				if ok == false {
					// TODO: Handle failed worker. For now, assume workers are all successful.
				} else {
					// Worker completed successfully. Put worker back on "idle" queue
					mr.registerChannel <- worker
				}
			}()
		}
	}

	//-------------------------------------------

	// Reduce phase
	if phase == reducePhase {
		// Assign each intermediary file to a worker
		for i := 0; i < ntasks; i++ {
			// Set up args for worker
			worker := <-mr.registerChannel
			taskNum := i
			args := DoTaskArgs{
				mr.jobName,
				"",				// no filename
				phase,
				taskNum,
				nios,			// number of original input files
			}

			// Start worker
			go func() {
				ok := call(worker, "Worker.DoTask", args, nil)
				if ok == false {
					// TODO: Handle failed worker. For now, assume workers are all successful.
				} else {
					// Worker completed successfully. Put worker back on "idle" queue
					mr.registerChannel <- worker
				}
			}()
		}
	}

	debug("Schedule: %v phase done\n", phase)
}
