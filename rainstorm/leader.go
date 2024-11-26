package rainstorm

import (
	"distributed_system/util"
	"distributed_system/global"
	"strconv"
	"encoding/json"
	"fmt"
	"os"
)

var worker_tasks = make(map[string][]string)
var workers []string

func InitiateJob(params map[string]string) {
	CreateSchedule(params)
	fmt.Println("schedule: ", global.Schedule)
	SendSchedule()
}

func CreateSchedule(params map[string]string) {
	// populate workers dictionary with empty task lists
	// create worker queue
	for _,node := range(global.Membership_list) {
        worker_tasks[util.GetTCPVersion(node.NodeID[:36])] = []string{}
		workers = append(workers,util.GetTCPVersion(node.NodeID[:36]))
    }

	// go through workers list and assign tasks, each stage should have num_tasks workers assigned
	num_tasks, _ := strconv.Atoi(params["num_tasks"])
	// populating source stage
	Populate_Stage(num_tasks, global.Stage{"source", 0})
	// populating op_1 stage
	Populate_Stage(num_tasks, global.Stage{params["op_1"], 1})
	// populating op_2 stage
	Populate_Stage(num_tasks, global.Stage{params["op_2"], 2})
	fmt.Println("worker to task: ", worker_tasks)
}

func Populate_Stage(num_tasks int, stage global.Stage) {
	global.Schedule[stage] = []string{}
	for i := 0; i < num_tasks; i++ {
		// add first worker in queue to schedule for task
        global.Schedule[stage] = append(global.Schedule[stage], workers[0])
		// add task to workers task list
		worker_tasks[workers[0]] = append(worker_tasks[workers[0]], stage.Name)
		// move worker to back of queue
		workers = append(workers[1:], workers[0])
    }
}

func SendSchedule() {
	for _,node := range global.Membership_list {
		port := util.GetTCPVersion(node.NodeID[:36])
		conn, err := util.DialTCPClient(port)
		defer conn.Close()
	
		// send the rainstorm parameters to the machine
		encoder  := json.NewEncoder(conn)
		err = encoder.Encode(global.Schedule)
		if err != nil {
			fmt.Println("Error encoding data in create", err)
		}
	}
}

func SendPartitions() {
	fmt.Println("rainstorm")
}


func CallRainstorm(params map[string]string) {
	leader_port := os.Getenv("LEADER_ADDRESS")
	conn, err := util.DialTCPClient(leader_port)
	defer conn.Close()

	// send the rainstorm parameters to the machine
	encoder := json.NewEncoder(conn)
	err = encoder.Encode(params)
	if err != nil {
		fmt.Println("Error encoding data in create", err)
	}
}