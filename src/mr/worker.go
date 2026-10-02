package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname
	for {
		// get a task
		args := GetTaskArgs{}
		reply := GetTaskReply{}
		ok := call("Coordinator.GetTask", &args, &reply)
		if !ok {
			log.Fatalf("GetTask Call Failed!\n")
		}

		// do the task
		switch reply.Type {
		case "map":
			{
				filename := reply.File
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				content, err := os.ReadFile(filename)
				if err != nil {
					log.Fatalf("cannot read %v", filename)
				}
				file.Close()
				kvs := mapf(filename, string(content))

				// prepare file handlers
				files := make([]*os.File, reply.NReduce)
				encoders := make([]*json.Encoder, reply.NReduce)

				// open all
				for i := 0; i < reply.NReduce; i++ {
					// create tmp files with os.CreateTemp and atomically rename
					// to avoid unexpected reader of Reduce Task
					tempFile, err := os.CreateTemp(".", "mr-tmp-*")
					if err != nil {
						log.Fatalf("Error when creating tmp file mr-tmp-%d\n", i)
					}
					files[i] = tempFile
					encoders[i] = json.NewEncoder(tempFile) // encode with json
				}
				for _, kv := range kvs {
					reduceID := ihash(kv.Key) % reply.NReduce

					// write files accordingly
					err := encoders[reduceID].Encode(&kv)
					if err != nil {
						log.Fatalf("Error when encoding %s\n", kv.Key)
					}
				}

				for i := 0; i < reply.NReduce; i++ {
					files[i].Close()
					// format: mr-mapID-reduceID
					intermediateName := fmt.Sprintf("mr-%d-%d", reply.TaskID, i)
					// ultimately rename
					err := os.Rename(files[i].Name(), intermediateName)
					if err != nil {
						log.Fatalf("Error when renaming file from %s to %s\n", files[i].Name(), intermediateName)
					}
				}
			}
		case "reduce":
			{
				intermediate := []KeyValue{}
				// open all
				for i := 0; i < reply.NMap; i++ {
					intermediateName := fmt.Sprintf("mr-%d-%d", i, reply.TaskID)
					file, err := os.Open(intermediateName)
					if err != nil {
						log.Fatalf("cannot open %v", intermediateName)
					}
					dec := json.NewDecoder(file)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							break // EOF or error
						}
						intermediate = append(intermediate, kv)
					}
					file.Close()
				}
				sort.Sort(ByKey(intermediate))

				oname := fmt.Sprintf("mr-out-%d-temp", reply.TaskID)
				ofile, err := os.Create(oname)
				if err != nil {
					log.Fatalf("Error when creating temp file %v", oname)
				}
				//
				// call Reduce on each distinct key in intermediate[],
				// and print the result to mr-out-0.
				//
				i := 0
				for i < len(intermediate) {
					j := i + 1
					for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
						j++
					}
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, intermediate[k].Value)
					}
					output := reducef(intermediate[i].Key, values)

					// this is the correct format for each line of Reduce output.
					fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

					i = j
				}

				ofile.Close()
				ultimateName := fmt.Sprintf("mr-out-%d", reply.TaskID)
				err = os.Rename(ofile.Name(), ultimateName)
				if err != nil {
					log.Fatalf("Error when renaming file from %s to %s\n", ofile.Name(), ultimateName)
				}
			}
		case "wait":
			{
				time.Sleep(500 * time.Millisecond)
				continue
			}
		case "done":
			return
		}

		// report the task
		ReportArgs := ReportTaskArgs{
			TaskID: reply.TaskID,
			Type:   reply.Type,
		}
		ReportReply := ReportTaskReply{}
		ok = call("Coordinator.ReportTask", &ReportArgs, &ReportReply)
		if !ok {
			log.Fatalf("ReportTask Call Failed!")
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
