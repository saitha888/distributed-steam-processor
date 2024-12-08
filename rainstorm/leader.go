package rainstorm

import (
	"distributed_system/util"
	"distributed_system/global"
	"distributed_system/hydfs"
	"strconv"
	"encoding/json"
	"fmt"
	"sync"
	"net"
)

var worker_tasks = make(map[string][]map[string]string)
var workers []string

func InitiateJob(params map[string]string) {
	CreateSchedule(params)
	SendSchedule()
	num_tasks, _ := strconv.Atoi(params["num_tasks"])
	SendPartitions(params["src_file"], params["dest_file"], global.Schedule[0],num_tasks )
}

func CreateSchedule(params map[string]string) {
	// populate workers dictionary with empty task lists
	// create worker queue
	for _,node := range(global.Membership_list) {
		if node.NodeID[:36] != global.Introducer_address {
			worker_tasks[GetRainstormVersion(node.NodeID[:36])] = []map[string]string{}
			workers = append(workers,GetRainstormVersion(node.NodeID[:36]))
		}
    }

	// go through workers list and assign tasks, each stage should have num_tasks workers assigned
	num_tasks, _ := strconv.Atoi(params["num_tasks"])
	pattern := params["pattern"]
	// populating source stage
	Populate_Stage(num_tasks, 0, "source", pattern, params["dest_file"])
	// populating op_1 stage
	Populate_Stage(num_tasks, 1, params["op_1"], pattern, params["dest_file"])
	// populating op_2 stage
	Populate_Stage(num_tasks, 2, params["op_2"], pattern, params["dest_file"])
}

func Populate_Stage(num_tasks int, stage int, op string, pattern string, dest_file string) {
	global.Schedule[stage] = []map[string]string{}
	for i := 0; i < num_tasks; i++ {
		task := map[string]string{
			"Op":      op,
			"Port":    workers[0],
			"Pattern":      pattern,
			"Log_filename":  op + "-" + strconv.Itoa(i) + "-log",
			"Dest_filename": dest_file,
		}
		hydfs.CreateFile("empty.txt",task["Log_filename"])
        global.Schedule[stage] = append(global.Schedule[stage], task)
		// add task to workers task list
		worker_tasks[workers[0]] = append(worker_tasks[workers[0]], task)
		// move worker to back of queue
		workers = append(workers[1:], workers[0])
    }
}

func SendSchedule() {
	for _,node := range global.Membership_list {
		// connect to node in membership list
		port := GetRainstormVersion(node.NodeID[:36])
		conn, err := util.DialTCPClient(port)
		defer conn.Close()
	
		// send the rainstorm schedule to the machine
		encoder  := json.NewEncoder(conn)
		err = encoder.Encode(global.Schedule)
		if err != nil {
			fmt.Println("Error encoding data in send schedule", err)
		}
	}
}

func GetPartitions(hydfs_file string, num_tasks int) {
	// calculate num lines for each partition
	num_lines := CountLines(hydfs_file)
	lines_per_task := num_lines / num_tasks
	extra_lines := num_lines % num_tasks

	// make an empty structure to populate
	partitions := make([][]int, num_tasks)

	start := 1

	for i := 0; i < num_tasks; i++ {
		end := start + lines_per_task - 1
		// add an extra line to the first few machines to get them covered
		if i < extra_lines { 
			end++
		}
		partitions[i] = []int{start, end} // add start and end index
		start = end + 1
	}

	global.Partitions = partitions

}

func SendPartitions(src_file string, dest_file string, Tasks []map[string]string, num_tasks int) {
	GetPartitions(src_file, num_tasks)
	var wg sync.WaitGroup

	// go through each port in the source stage
	for i := 0; i < len(Tasks); i++ {
		wg.Add(1)

		partition := global.Partitions[i]

		data := global.SourceTask{
			Start: partition[0],
			End: partition[1],
			Src_file: src_file,
		}

		port := Tasks[i]["Port"]
		// start a go routine to send all the tasks concurrently
		conn, err := net.Dial("tcp", port)
		if err != nil {
			fmt.Println("Error connecting to port:", err)
			return
		}
		defer conn.Close()

		// Send the data
		encoder := json.NewEncoder(conn)
		err = encoder.Encode(data)
		if err != nil {
			fmt.Println("Error encoding structure to JSON:", err)
			return
		}
	}
}	