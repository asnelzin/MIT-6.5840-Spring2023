package mr

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

func ReduceFilenames(nMap int, reduceID int) []string {
	filenames := make([]string, nMap)
	for i := 0; i < nMap; i++ {
		filenames[i] = fmt.Sprintf("mr-%d-%d", i, reduceID)
	}
	return filenames
}

type Task struct {
	ID        int
	Type      string
	Filenames []string

	assignedAt time.Time
	finished   bool
}

func (t *Task) ToGetTaskReply() *GetTaskReply {
	return &GetTaskReply{
		ID:        t.ID,
		Type:      t.Type,
		Filenames: t.Filenames,
	}
}

func (t *Task) IsAssignable() bool {
	if t.finished {
		return false
	}

	if time.Since(t.assignedAt) < 10*time.Second {
		return false
	}

	return true
}

type Coordinator struct {
	mtasks    map[int]Task
	rtasks    map[int]Task
	tasksLock sync.RWMutex

	nReduce               int
	inputFilenames        []string
	intermediateFilenames []string
}

func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetNReduce(args *GetNReduceArgs, reply *GetNReduceReply) error {
	reply.NReduce = c.nReduce
	return nil
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.tasksLock.Lock()
	defer c.tasksLock.Unlock()

	t, err := c.findNext()
	if errors.Is(err, shouldWaitError) {
		reply.Type = "sleep"
		return nil
	} else if errors.Is(err, jobFinishedError) {
		reply.Type = "exit"
		return nil
	}

	reply.ID = t.ID
	reply.Type = t.Type
	reply.Filenames = t.Filenames
	t.assignedAt = time.Now()
	if t.Type == "map" {
		c.mtasks[t.ID] = t
	} else {
		c.rtasks[t.ID] = t
	}

	return nil
}

var (
	shouldWaitError  = errors.New("find no assignable tasks: should wait")
	jobFinishedError = errors.New("job is finished")
)

func (c *Coordinator) findNext() (Task, error) {
	mapStageFinished := true
	for _, t := range c.mtasks {
		if t.finished {
			continue
		}
		mapStageFinished = false
		if t.IsAssignable() {
			return t, nil
		}
	}
	if !mapStageFinished {
		return Task{}, shouldWaitError
	}

	reduceStageFinished := true
	for _, t := range c.rtasks {
		if t.finished {
			continue
		}
		reduceStageFinished = false
		if t.IsAssignable() {
			return t, nil
		}
	}
	if !reduceStageFinished {
		return Task{}, shouldWaitError
	}
	return Task{}, jobFinishedError
}

func (c *Coordinator) Finished(args *FinishedArgs, reply *FinishedReply) error {
	c.tasksLock.Lock()
	defer c.tasksLock.Unlock()

	if args.Type == "map" {
		t := c.mtasks[args.ID]
		t.finished = true
		c.mtasks[args.ID] = t
	} else {
		t := c.rtasks[args.ID]
		t.finished = true
		c.rtasks[args.ID] = t
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// Done is called by main/mrcoordinator.go periodically to find out if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.tasksLock.RLock()
	defer c.tasksLock.RUnlock()

	_, err := c.findNext()
	if errors.Is(err, jobFinishedError) {
		return true
	}

	return false
}

// MakeCoordinator creates a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	mtasks := make(map[int]Task, len(files))
	for idx, name := range files {
		t := Task{
			ID:        idx,
			Type:      "map",
			Filenames: []string{name},
			finished:  false,
		}
		mtasks[t.ID] = t
	}

	rtasks := make(map[int]Task, nReduce)
	for i := 0; i < nReduce; i++ {
		t := Task{
			ID:        i,
			Type:      "reduce",
			Filenames: ReduceFilenames(len(files), i),
			finished:  false,
		}
		rtasks[t.ID] = t
	}

	c := Coordinator{
		mtasks:         mtasks,
		rtasks:         rtasks,
		inputFilenames: files,
		nReduce:        nReduce,
	}

	c.server()
	return &c
}
