package mapreduce

import (
	"encoding/json"
	"os"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	// TODO:
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).
	// Remember that you've encoded the values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger that combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Use checkError to handle errors.

	// Group intermediate key-value pairs by key, for each intermediate file
	valuesByKey := make(map[string][]string) // map: key -> list of values
	for m := 0; m < nMap; m++ {
		// Read intermediate file from map task m
		filename := reduceName(jobName, m, reduceTaskNumber)
		inFile, err := os.Open(filename)
		checkError(err)

		// Decode JSON for each key-value pair, and add it to the map
		dec := json.NewDecoder(inFile)
		for dec.More() {
			// Decode
			var kv KeyValue
			err := dec.Decode(&kv)
			checkError(err)

			// Add key-value pair to map
			valuesByKey[kv.Key] = append(valuesByKey[kv.Key], kv.Value)
		}

		inFile.Close()
	}

	// Reduce each key, and write output to disk in JSON
	outFilename := mergeName(jobName, reduceTaskNumber)
	outFile, err := os.Create(outFilename)
	checkError(err)
	enc := json.NewEncoder(outFile)
	for key, values := range valuesByKey {
		// Reduce
		results := reduceF(key, values)

		// Write output
		err := enc.Encode(KeyValue{key, results})
		checkError(err)
	}
	outFile.Close()
}
