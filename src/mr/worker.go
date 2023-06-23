package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	nReduce, err := CallGetNReduce()
	if err != nil {
		fmt.Printf("could not get nReduce: %v", err)
		return
	}

	for {
		task := GetTaskReply{}
		if ok := call("Coordinator.GetTask", &GetTaskArgs{}, &task); !ok {
			log.Printf("could not get task from coordinator: exit")
			return
		}
		switch task.Type {
		case "sleep":
			time.Sleep(1 * time.Second)
			continue
		case "exit":
			return
		case "map":
			merr := Map(mapf, task.Filenames[0], task.ID, nReduce)
			if merr != nil {
				log.Printf("could not run map stage: %v", merr)
				return
			}
			err := CallFinished(task.ID, task.Type)
			if err != nil {
				log.Printf("could not request coordinator: %v", err)
				return
			}
		case "reduce":
			rerr := Reduce(reducef, task.Filenames, task.ID)
			if rerr != nil {
				log.Printf("could not run reduce stage: %v", rerr)
				return
			}
			err := CallFinished(task.ID, task.Type)
			if err != nil {
				log.Printf("could not request coordinator: %v", err)
				return
			}
		}
	}
}

func Map(mapf func(string, string) []KeyValue, filename string, tid int, nReduce int) error {
	f, err := os.Open(filename)
	defer func() {
		e := f.Close()
		if e != nil {
			log.Fatalf("could not close file: %v", err)
		}
	}()
	if err != nil {
		return err
	}

	content, err := io.ReadAll(f)
	if err != nil {
		return err
	}

	kva := mapf(filename, string(content))
	intermediateFiles := make([]*os.File, 0, nReduce)
	encoders := make([]*json.Encoder, 0, nReduce)
	for i := 0; i < nReduce; i++ {
		tmp, err := os.CreateTemp("", "*")
		if err != nil {
			return err
		}

		intermediateFiles = append(intermediateFiles, tmp)
		encoders = append(encoders, json.NewEncoder(tmp))
	}

	for _, kv := range kva {
		reduceID := ihash(kv.Key) % nReduce
		err := encoders[reduceID].Encode(&kv)
		if err != nil {
			return err
		}
	}
	for reducerID, f := range intermediateFiles {
		err := f.Sync()
		if err != nil {
			return err
		}
		err = f.Close()
		if err != nil {
			return err
		}
		err = os.Rename(f.Name(), fmt.Sprintf("mr-%d-%d", tid, reducerID))
		if err != nil {
			return err
		}
	}

	return nil
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func Reduce(reducef func(string, []string) string, filenames []string, tid int) error {
	var kva []KeyValue
	for _, name := range filenames {
		f, err := os.Open(name)
		if err != nil {
			return err
		}
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	sort.Sort(ByKey(kva))

	tmp, err := os.CreateTemp("", "*")
	if err != nil {
		return err
	}

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		fmt.Fprintf(tmp, "%v %v\n", kva[i].Key, output)
		i = j
	}

	err = tmp.Sync()
	if err != nil {
		return err
	}
	err = tmp.Close()
	if err != nil {
		return err
	}
	return os.Rename(tmp.Name(), fmt.Sprintf("mr-out-%d", tid))
}

func CallGetNReduce() (int, error) {
	r := GetNReduceReply{}
	ok := call("Coordinator.GetNReduce", &GetNReduceArgs{}, &r)
	if !ok {
		return 0, errors.New("RPC call GetNReduce failed")
	}
	return r.NReduce, nil
}

func CallFinished(id int, typ string) error {
	ok := call("Coordinator.Finished", &FinishedArgs{id, typ}, &FinishedReply{})
	if !ok {
		return errors.New("could not finish task")
	}
	return nil
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
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
