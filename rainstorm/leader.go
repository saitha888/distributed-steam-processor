package rainstorm

import (
	"distributed_system/util"
	"distributed_system/global"
	"strconv"
	"encoding/json"
	"fmt"
	"sync"
	"net"
)

var worker_tasks = make(map[string][]string)
var workers []string

func InitiateJob(params map[string]string) {
	CreateSchedule(params)
	SendSchedule()
	num_tasks, _ := strconv.Atoi(params["num_tasks"])
	SendPartitions(params["src_file"], params["dest_file"], global.Schedule["0-source"],num_tasks )
}

func CreateSchedule(params map[string]string) {
	// populate workers dictionary with empty task lists
	// create worker queue
	for _,node := range(global.Membership_list) {
		if node.NodeID[:36] != global.Leader_address {
			worker_tasks[GetRainstormVersion(node.NodeID[:36])] = []string{}
			workers = append(workers,GetRainstormVersion(node.NodeID[:36]))
		}
    }

	// go through workers list and assign tasks, each stage should have num_tasks workers assigned
	num_tasks, _ := strconv.Atoi(params["num_tasks"])
	// populating source stage
	Populate_Stage(num_tasks, "0-source")
	// populating op_1 stage
	Populate_Stage(num_tasks, "1-" + params["op_1"])
	// populating op_2 stage
	Populate_Stage(num_tasks, "2-" + params["op_2"])
	global.Schedule["dest_file"] = []string{params["dest_file"]}
}

func Populate_Stage(num_tasks int, stage string) {
	global.Schedule[stage] = []string{}
	for i := 0; i < num_tasks; i++ {
		// add first worker in queue to schedule for task
        global.Schedule[stage] = append(global.Schedule[stage], workers[0])
		// add task to workers task list
		worker_tasks[workers[0]] = append(worker_tasks[workers[0]], stage)
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
	fmt.Println("num lines: ", num_lines)
	lines_per_task := num_lines / num_tasks
	fmt.Println("num lines per task: ",lines_per_task)
	extra_lines := num_lines % num_tasks

	// make an empty structure to populate
	partitions := make([][]int, num_tasks)

	start := 0

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

func SendPartitions(src_file string, dest_file string, ports []string, num_tasks int) {
	GetPartitions(src_file, num_tasks)
	var wg sync.WaitGroup

	// go through each port in the source stage
	for i := 0; i < len(ports); i++ {
		wg.Add(1)

		partition := global.Partitions[i]

		data := global.SourceTask{
			Start: partition[0],
			End: partition[1],
			Src_file: src_file,
			Dest_file: dest_file,
		}

		port := ports[i]
		fmt.Println("sending this interval to port " + port + ": ", partition)
		// start a go routine to send all the tasks concurrently
		go func(port string, data global.SourceTask) {
			defer wg.Done() // Decrement the wait group counter when done

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

			// Listen for acknowledgements and process them
			decoder := json.NewDecoder(conn)
			for {
				var line_number int
				if err := decoder.Decode(&line_number); err != nil {
					fmt.Println("Error receiving acknowledgment:", err)
					break
				}

				// Process the acknowledgment immediately
				fmt.Println("line number processed: ", line_number)
				line_num := strconv.Itoa(line_number)
				ProcessAcknowledgement(port, line_num)
			}
		}(port, data)
	}
	wg.Wait()
}	

func ProcessAcknowledgement(port string, line_number string) {
	// get the partition that the port is handling
	schedule_ports := global.Schedule["0-source"]
	partition_index := -1
	for index,curr_port := range schedule_ports {
		if curr_port == port {
			partition_index = index 
			break
		}
	}
	// update the start index based on what has already been handled
	line_num, _ := strconv.Atoi(line_number)
	global.Partitions[partition_index][0] = line_num + 1 
}