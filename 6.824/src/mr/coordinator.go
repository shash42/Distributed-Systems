package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	mu         sync.Mutex
	phase      int
	numTasks   [2]int
	taskWorker [2][]int
	remTasks   [2]map[int]int
	inpfiles   []string
	nReduce    int
	tasksDone  [2]int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) findPending(mapOrReduce int) int {
	for key := range c.remTasks[mapOrReduce] {
		return key
	}
	return -1
}

func (c *Coordinator) WaitWorker(tasktype int, taskid int) {
	time.Sleep(10 * time.Second)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.taskWorker[tasktype][taskid] != -1 { //if not done, unassign
		c.taskWorker[tasktype][taskid] = 0
		c.remTasks[tasktype][taskid] = 0 //add to tasks remaining
	}
}

func (c *Coordinator) GetTask(args *Args, reply *Reply) error {
	c.mu.Lock()
	reply.NReduce = c.numTasks[1]
	reply.NMap = c.numTasks[0]
	fmt.Printf("Phase %v: Worker %v request received\n", c.phase, args.Id)
	if c.phase == 0 {
		reply.Tasktype = 0
		reply.Id = c.findPending(0)
		if reply.Id != -1 {
			delete(c.remTasks[0], reply.Id)
			reply.Fileinp = c.inpfiles[reply.Id-1]
			c.taskWorker[0][reply.Id] = args.Id
		}
	} else {
		reply.Tasktype = 1
		reply.Id = c.findPending(1)
		if reply.Id != -1 {
			delete(c.remTasks[1], reply.Id)
			c.taskWorker[1][reply.Id] = args.Id
		}
	}
	c.mu.Unlock()
	if reply.Id != -1 {
		go c.WaitWorker(reply.Tasktype, reply.Id)
	}
	fmt.Printf("Phase %v: Worker %v assigned task %v nreduce: %v\n", c.phase, args.Id, reply.Id, reply.NReduce)
	return nil
}

func (c *Coordinator) FinishTask(args *Args, reply *Reply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.taskWorker[args.Tasktype][args.Taskid] != args.Id { //this worker has timed out
		fmt.Printf("Phase %v: Worker %v completed task %v of type %v but timed out! :(\n", c.phase, args.Id, args.Taskid, args.Tasktype)
		return nil
	}
	c.tasksDone[args.Tasktype]++
	fmt.Printf("Phase %v: Worker %v completed task %v of type %v successfully :)\n", c.phase, args.Id, args.Taskid, args.Tasktype)
	if c.tasksDone[args.Tasktype] == c.numTasks[args.Tasktype] {
		fmt.Printf("Phase %v over! Next phase\n", c.phase)
		c.phase++
	}
	delete(c.remTasks[args.Tasktype], args.Taskid)
	c.taskWorker[args.Tasktype][args.Taskid] = -1 //marks done
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false
	c.mu.Lock()
	if c.phase == 2 {
		ret = true
	}
	c.mu.Unlock()
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.inpfiles = files
	c.nReduce = nReduce
	c.phase = 0
	c.numTasks[0] = len(files)
	c.numTasks[1] = nReduce
	for i := 0; i < 2; i++ {
		c.taskWorker[i] = make([]int, c.numTasks[i]+1)
		c.remTasks[i] = make(map[int]int)
		for j := 1; j <= c.numTasks[i]; j++ {
			c.remTasks[i][j] = 0
		}
	}

	c.server()
	return &c
}
