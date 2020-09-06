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
	// Open the input file and check for errors.
	file, err := ioutil.ReadFile(inFile)
	checkError(err)
	// Map key-values of the input file
	keyValues := mapF(inFile, string(file))
	
	// Slice to map the MapReduce job in JSON format.
	encoderSlice := make(map[string]*json.Encoder)
	
	// Execute the reduce task.
	for i := 0; i < nReduce; i++ {
		fileName := reduceName(jobName, mapTaskNumber, i)
		// Create each intermediate file and check for errors.
		intermediateFile, err := os.Create(fileName)
		checkError(err)
		// Close the file when returning from this function, by using keyword defer.
		defer intermediateFile.Close()
		// Encoded to JSON format in each of the nReduce intermediate files.
		middleEncoder := json.NewEncoder(intermediateFile)
		encoderSlice[fileName] = middleEncoder
	}
	
	// Call ihash function to decide which file a given key belongs into each of the keyValues mapped that will be reduced.
	for _, kV := range keyValues {
		fileID := int(ihash(kV.Key)) % nReduce
		fileName := reduceName(jobName, mapTaskNumber, fileID)
		err := encoderSlice[fileName].Encode(&kV)
		checkError(err)
	}
}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
