package mapreduce

import (
	"encoding/json"
	"hash/fnv"
	"io/ioutil"
	"os"
)

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	// TODO:
	// You will need to write this function.
	// The output of a map task is stored in the file system as files whose name
	// indicates which map task produced them and which reduce task they are for.
	// This map task should write output to a different file for each reduce task,
	// based on the keys of the intermediate key/value pairs. You can map each key
	// to a reduce task number r using ihash(). Then the filename to write to for
	// the r'th reduce task can be found using
	// reduceName(jobName, mapTaskNumber, r).
	//
	// Coming up with a scheme for how to store the key/value pairs on disk can be
	// tricky, especially when taking into account that both keys and values could
	// contain newlines, quotes, and any other character you can think of.
	//
	// One format often used for serializing data to a byte stream that the
	// other end can correctly reconstruct is JSON. You are not required to
	// use JSON, but as the output of the reduce tasks *must* be JSON,
	// familiarizing yourself with it here may prove useful. You can write
	// out a data structure as a JSON string to a file using the commented
	// code below. The corresponding decoding functions can be found in
	// common_reduce.go.
	//
	//   enc := json.NewEncoder(file)
	//   for _, kv := ... {
	//     err := enc.Encode(&kv)
	//
	// Remember to close the file after you have written all the values!
	// Use checkError to handle errors.

	// Read file's contents
	contentsBytes, err := ioutil.ReadFile(inFile)
	checkError(err)
	contents := string(contentsBytes)

	// Map
	kvs := mapF(inFile, contents)

	// Divide up intermediate key-values by output file
	kvsByOutFile := make(map[string][]int)  // map: output filename -> indices of key-values
	for i := range kvs {
		key := kvs[i].Key
		outFile := reduceName(jobName, mapTaskNumber, int(ihash(key)))
		kvsByOutFile[outFile] = append(kvsByOutFile[outFile], i)
	}

	// Write key-values to their corresponding output files
	for outFile, indices := range kvsByOutFile {
		out, err := os.Create(outFile)
		checkError(err)
		enc := json.NewEncoder(out)
		for _, i := range indices {
			// JSON encode i'th key-value to output file
			err := enc.Encode(&kvs[i])
			checkError(err)
		}
		out.Close()
	}
}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
