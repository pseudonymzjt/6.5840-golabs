package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskState int

// enum task state
const (
	IDLE TaskState = iota
	InProgress
	Completed
)

type Task struct {
	TaskID    int
	Type      string
	State     TaskState
	StartTime time.Time
	InputFile string
}

type Coordinator struct {
	mu          sync.Mutex
	files       []string
	NReduce     int
	mapTasks    []Task
	reduceTasks []Task
}

// Your code here -- RPC handlers for the worker to call.
// get a task for a worker
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 检查是否有超时的任务需要重新分配
	// 遍历 c.mapTasks 和 c.reduceTasks，如果 InProgress 且超时，改回 IDLE
	for i := range c.mapTasks {
		if c.mapTasks[i].State == InProgress && time.Since(c.mapTasks[i].StartTime) >= 10*time.Second {
			c.mapTasks[i].State = IDLE // 真正修改到底层切片里的数据
			log.Printf("Map Task %d timed out, resetting to Idle...", c.mapTasks[i].TaskID)
		}
	}

	for i := range c.reduceTasks {
		if c.reduceTasks[i].State == InProgress && time.Since(c.reduceTasks[i].StartTime) >= 10*time.Second {
			c.reduceTasks[i].State = IDLE // 真正修改到底层切片里的数据
			log.Printf("Reduce Task %d timed out, resetting to Idle...", c.reduceTasks[i].TaskID)
		}
	}

	// 优先分配 Map 任务
	for i := range c.mapTasks {
		if c.mapTasks[i].State == IDLE {
			c.mapTasks[i].State = InProgress
			c.mapTasks[i].StartTime = time.Now()

			reply.Type = "map"
			reply.TaskID = c.mapTasks[i].TaskID
			reply.File = c.mapTasks[i].InputFile
			reply.NMap = len(c.mapTasks)
			reply.NReduce = c.NReduce
			return nil
		}
	}

	// 判断 Map 阶段是否全部完成
	mapDone := true
	for _, task := range c.mapTasks {
		if task.State != Completed {
			mapDone = false
			break
		}
	}

	if !mapDone {
		// Map 有任务在执行中，让 Worker 等等
		reply.Type = "wait"
		return nil
	}

	// 分配 Reduce 任务
	for i := range c.reduceTasks {
		if c.reduceTasks[i].State == IDLE {
			c.reduceTasks[i].State = InProgress
			c.reduceTasks[i].StartTime = time.Now()

			reply.Type = "reduce"
			reply.TaskID = c.reduceTasks[i].TaskID
			reply.NMap = len(c.mapTasks) // Reduce 需要知道去读哪些中间文件
			reply.NReduce = c.NReduce
			return nil
		}
	}

	// 判断 Reduce 阶段是否全部完成
	reduceDone := true
	for _, task := range c.reduceTasks {
		if task.State != Completed {
			reduceDone = false
			break
		}
	}

	if reduceDone {
		reply.Type = "done"
	} else {
		// Reduce 还没全做完，让 Worker 等等
		reply.Type = "wait"
	}

	return nil
}

// receive a task report from a worker
func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch args.Type {
	case "map":
		for i := range c.mapTasks {
			if c.mapTasks[i].TaskID == args.TaskID {
				if c.mapTasks[i].State != Completed {
					c.mapTasks[i].State = Completed
				}
				return nil
			}
		}
	case "reduce":
		for i := range c.reduceTasks {
			if c.reduceTasks[i].TaskID == args.TaskID {
				if c.reduceTasks[i].State != Completed {
					c.reduceTasks[i].State = Completed
				}
				return nil
			}
		}
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 遍历所有 reduce 任务，只要有一个没完成，就返回 false
	for _, task := range c.reduceTasks {
		if task.State != Completed {
			return false
		}
	}
	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, NReduce int) *Coordinator {
	c := Coordinator{
		files:   files,
		NReduce: NReduce,
	}

	for i, filename := range files {
		c.mapTasks = append(c.mapTasks, Task{
			TaskID:    i,
			Type:      "map",
			State:     IDLE,
			InputFile: filename,
		})
	}

	for j := range NReduce {
		c.reduceTasks = append(c.reduceTasks, Task{
			TaskID: j,
			Type:   "reduce",
			State:  IDLE,
		})
	}
	go c.server(sockname)
	return &c
}
