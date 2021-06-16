package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
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

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	rand1 := rand.New(rand.NewSource(time.Now().UnixNano()))
	id := rand1.Intn(1e9)
	for { //[Shash]: Try periodically to get a task from coordinator
		time.Sleep(time.Second)
		args, reply := Args{}, Reply{}
		RequestTask(id, &args, &reply)
		if reply.Id == -1 {
			continue
		}

		Process(id, &reply, mapf, reducef)

		FinishTask(id, &args, &reply)

	}
	// uncomment to send the Example RPC to the coordinator.
	//CallExample()

}

func Process(id int, reply *Reply, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	if reply.Tasktype == 0 {
		filename := reply.Fileinp
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()

		//Get kva and assign to partition
		intermediate := make(map[int][]KeyValue)
		kva := mapf(filename, string(content))
		sort.Sort(ByKey(kva))
		for i := 0; i < len(kva); i++ {
			bucket := ihash(kva[i].Key) % reply.NReduce
			bucket++ //1Indexing
			/*if kva[i].Key == "absent" {
				fmt.Printf("Map worker %v task %v file %v partition - %v\n", id, reply.Id, reply.Fileinp, bucket)
			}*/
			intermediate[bucket] = append(intermediate[bucket], kva[i])
		}
		//Print intermediate to write files
		for key := 1; key <= reply.NReduce; key++ {
			value := intermediate[key]
			oname := fmt.Sprintf("mr-%v-%v", reply.Id, key)
			ofile, _ := os.Create(oname)
			enc := json.NewEncoder(ofile)
			enc.Encode(value)
			/*jsonval, _ := json.Marshal(value)
			ofile.Write(jsonval)*/
			ofile.Close()
		}

	} else {
		intermediate := make(map[string][]string)
		for i := 1; i <= reply.NMap; i++ {
			oname := fmt.Sprintf("mr-%v-%v", i, reply.Id)
			ofile, err := os.Open(oname)
			if err != nil {
				panic(err)
			}
			dec := json.NewDecoder(ofile)
			for {
				var kvarr []KeyValue
				if err := dec.Decode(&kvarr); err != nil {
					//fmt.Printf("Reduce worker %v partition %v had error during map %v\n - Error: %v\n", id, reply.Id, i, err)
					break
				}
				for _, kv := range kvarr {
					/*if kv.Key == "absent" {
						fmt.Printf("Reduce worker %v mr-%v-%v\n", id, i, reply.Id)
					}*/
					intermediate[kv.Key] = append(intermediate[kv.Key], kv.Value)
				}
			}
			ofile.Close()
		}
		oname := fmt.Sprintf("mr-out-%d", reply.Id)
		os.Create(oname)
		for key, values := range intermediate {
			output := reducef(key, values)
			//fmt.Printf("Reduce worker %v finished reducef\n", id)
			oname := fmt.Sprintf("mr-out-%d", reply.Id)
			ofile, _ := os.OpenFile(oname, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
			fmt.Fprintf(ofile, "%v %v\n", key, output)
			ofile.Close()
		}
	}
	/*
		//Random sleep for testing
		rand1 := rand.New(rand.NewSource(time.Now().UnixNano()))
		processdur := rand1.Intn(20)
		time.Sleep(time.Duration(processdur) * time.Second)
	*/
	fmt.Printf("Worker %v has finished processing\n", id)
}

func RequestTask(id int, args *Args, reply *Reply) {
	args.Id = id
	fmt.Printf("Worker %v has requested a task\n", id)
	call("Coordinator.GetTask", args, reply)
}

func FinishTask(id int, args *Args, reply *Reply) {
	args.Taskid = reply.Id
	args.Tasktype = reply.Tasktype
	fmt.Printf("Worker %v has finished task %v of type %v\n", args.Id, args.Taskid, args.Tasktype)
	call("Coordinator.FinishTask", args, reply)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
