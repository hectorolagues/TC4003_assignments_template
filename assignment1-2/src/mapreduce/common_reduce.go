package mapreduce

import (
	"encoding/json"
	"io"
	"os"
	"sort"
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
	// Slice to map the key-values mapped in the MapReduce job.
	keyValuesMapped := make(map[string][]string)
	// Reduce operation with the number of map tasks executed as threshold.
	for m := 0; m < nMap; m++ {
		fileName := reduceName(jobName, m, reduceTaskNumber)
		// Open the intermediate file of each map task and check for errors.
		intermediateFile, err := os.Open(fileName)
		checkError(err)
		// Close the file when returning from this function, by using keyword defer.
		defer intermediateFile.Close()

		// As intermediate files were encoded, it will be needed to decode them from JSON format until EOF is reached.
		intermediateDecoder := json.NewDecoder(intermediateFile)
		for {
			var keyVal KeyValue
			if err := intermediateDecoder.Decode(&keyVal); err == io.EOF {
				break
			} else {
				checkError(err)
			}
			keyValuesMapped[keyVal.Key] = append(keyValuesMapped[keyVal.Key], keyVal.Value)
		}
	}

	// Create the file with the reduced output from mergeName function.
	reducedFileName := mergeName(jobName, reduceTaskNumber)
	writeFile, err := os.Create(reducedFileName)
	checkError(err)
	// Close the file when returning from this function, by using keyword defer.
	defer writeFile.Close()

	// Sort the intermediate key-value pairs by key.
	var sortedKeys []string
	for k := range keyValuesMapped {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)

	// Encode to JSON marshalling format, as expected by the merger that combines the output from all reduce tasks.
	outputEncoder := json.NewEncoder(writeFile)
	for _, key := range sortedKeys {
		outputEncoder.Encode(KeyValue{key, reduceF(key, keyValuesMapped[key])})
	}
}
