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
	if jobPhase == mapPhase {
		// Assign each input file to a worker
		for taskNum, file := range mr.files {
			// Set up args for worker
			worker := <-mr.registerChannel
			args := DoTaskArgs{
				mr.jobName,
				file,
				jobPhase,
				taskNum,
				nios				// number of intermediate output files
			}

			// Start worker
			go func() {
				ok := call(worker, "Worker.DoTask", args, nil)
				if ok == false {
					// TODO: Handle failed worker. For now, assume workers are all successful.
				}
			}()
		}
	}

	//-------------------------------------------

	// Reduce phase
	if jobPhase == reducePhase {
		// Assign each intermediary file to a worker
		for taskNum := 0; taskNum < ntasks; taskNum++ {
			// Set up args for worker
			worker := <-mr.registerChannel
			args := DoTaskArgs{
				mr.jobName,
				nil,			// no filename
				jobPhase,
				taskNum,
				nios			// number of original input files
			}

			// Start worker
			go func() {
				ok := call(worker, "Worker.DoTask", args, nil)
				if ok == false {
					// TODO: Handle failed worker. For now, assume workers are all successful.
				}
			}
		}
	}

	debug("Schedule: %v phase done\n", phase)
}
