package rainstorm

import (
	"distributed_system/util"
	"distributed_system/global"
	"distributed_system/hydfs"
	"strconv"
	"encoding/json"
	"fmt"
	"net"
	"sync"
)

var workers []string

func InitiateJob(params map[string]string) {
	Reset()
	CreateSchedule(params)
	SendSchedule("")
	num_tasks, _ := strconv.Atoi(params["num_tasks"])
	SendPartitions(params["src_file"], params["dest_file"], global.Schedule[0],num_tasks )
}

func CreateSchedule(params map[string]string) {
	// populate workers dictionary with empty task lists
	// create worker queue
	for _,node := range(global.Membership_list) {
		if node.NodeID[:36] != global.Introducer_address {
			workers = append(workers,GetRainstormVersion(node.NodeID[:36]))
		}
    }

	// go through workers list and assign tasks, each stage should have num_tasks workers assigned
	num_tasks, _ := strconv.Atoi(params["num_tasks"])
	pattern := params["pattern"]
	// populating source stage
	Populate_Stage(num_tasks, 0, "source", "", params["dest_file"], "false")
	// populating op_1 stage
	Populate_Stage(num_tasks, 1, params["op_1"], pattern, params["dest_file"], "false")
	// populating op_2 stage
	Populate_Stage(num_tasks, 2, params["op_2"], "", params["dest_file"], params["stateful"])
}

func Populate_Stage(num_tasks int, stage int, op string, pattern string, dest_file string, stateful string) {
	global.Schedule[stage] = []map[string]string{}
	for i := 0; i < num_tasks; i++ {
		state_filename := ""
		if stateful == "true" {
			state_filename = op + "-" + strconv.Itoa(i) + "-state_log"
			hydfs.CreateFile("empty.txt",state_filename)
		}
		task := map[string]string{
			"Op":      op,
			"Port":    workers[0],
			"Pattern":      pattern,
			"Log_filename":  op + "-" + strconv.Itoa(i) + "-log",
			"Dest_filename": dest_file,
			"State_filename": state_filename,
		}
		hydfs.CreateFile("empty.txt",task["Log_filename"])
        global.Schedule[stage] = append(global.Schedule[stage], task)
		// move worker to back of queue
		workers = append(workers[1:], workers[0])
    }
}


func SendSchedule(deleted_port string) {
	for _,node := range global.Membership_list {
		// connect to node in membership list
		port := GetRainstormVersion(node.NodeID[:36])
		conn, err := util.DialTCPClient(port)
		if err != nil {
			fmt.Println("Error dialing client in send schedule", err)
			continue
		}
		defer conn.Close()
	
		// send the rainstorm schedule to the machine
		encoder  := json.NewEncoder(conn)
		err = encoder.Encode(global.Schedule)
		if err != nil {
			fmt.Println("Error encoding data in send schedule", err)
			continue
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
		wg.Add(1) // Increment the WaitGroup counter

		go func(i int) {
			defer wg.Done() // Decrement the counter when the goroutine completes

			partition := global.Partitions[i]

			data := global.SourceTask{
				Start:    partition[0],
				End:      partition[1],
				Src_file: src_file,
			}

			port := Tasks[i]["Port"]

			// Establish a connection
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
		}(i) // Pass 'i' as an argument to the goroutine
	}

	// Wait for all goroutines to finish
	wg.Wait()

}	

func WriteToDest(tuples []global.Tuple) {
	dest_string := ""
	for _, tuple := range tuples {
		id := tuple.ID
		key := tuple.Key 
		value := tuple.Value 
		src := tuple.Src
		curr_stage := tuple.Stage 

		tup := fmt.Sprintf("%s, %s\n", key, value)
		dest_string += tup
		//send ack back to sender machine
		global.AckBatchesMutex.Lock()
		filename := GetAppendLogAck(curr_stage - 1, src)
		if _, exists := global.AckBatches[filename]; exists {
			global.AckBatches[filename] += id + "|ack\n"
		} else {
			global.AckBatches[filename] = id + "|ack\n"
		}
		global.AckBatchesMutex.Unlock()
	}
	if len(dest_string) > 0 {
		global.DestMutex.Lock()
		hydfs.AppendStringToDest(dest_string, global.Schedule[0][0]["Dest_filename"])
		global.DestMutex.Unlock()
	}
	fmt.Println(dest_string)
}

func Reschedule(addr string) {
	global.ScheduleMutex.Lock()
	rainstorm_addr := GetRainstormVersion(addr)
	for _, tasks := range global.Schedule {
		for _, task := range tasks {
			if task["Port"] == rainstorm_addr {
				//reassign port
				reassign_port := workers[0]
				task["Port"] = reassign_port
				workers = append(workers[1:], workers[0])
			}
		}
	}
	SendSchedule(addr)
	global.ScheduleMutex.Unlock()
}

func Reset() {
	global.Schedule = make(map[int][]map[string]string)
	global.Partitions = nil
	global.Batches = make(map[string][]global.Tuple)
	global.AckBatches = make(map[string]string) 
	global.Reschedule_called = false
	for _,node := range global.Membership_list {
		// connect to node in membership list
		port := GetRainstormVersion(node.NodeID[:36])
		conn, err := util.DialTCPClient(port)
		if err != nil {
			fmt.Println("Error dialing client in send schedule", err)
			continue
		}
		defer conn.Close()
	
		// send reset message
		message := make(map[string]string)
		message["reset"] = "reset"
		encoder  := json.NewEncoder(conn)
		err = encoder.Encode(message)
		if err != nil {
			fmt.Println("Error encoding data in send schedule", err)
			continue
		}
	}
}