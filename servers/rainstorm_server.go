package servers

import (
	"net"
	"fmt"
	"distributed_system/global"
	"encoding/json"
	"distributed_system/rainstorm"
	"os"
	"time"
	"syscall"
)



//starts tcp server that listens for grep commands
func RainstormServer() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	go func() {
		for range ticker.C {
			rainstorm.SendBatches()
			rainstorm.SendAckBatches()
		}
	}()
    // listen for connection from other machine 
    ln, err := net.Listen("tcp", ":" + global.Rainstorm_port)
    if err != nil {
        fmt.Println(err) 
        return
    }

    // run subroutine to handle the connection
    for {
        conn, err := ln.Accept()
        if err != nil {
            fmt.Println(err)
            continue
        }


        // Handle the connection in a go routine
        go handleRainstormConnection(conn)
    }
}

//handler of any incoming connection from other machines
func handleRainstormConnection(conn net.Conn) {

    // Close the connection when we're done
    defer conn.Close()
	
	var data map[string]interface{}
	decoder := json.NewDecoder(conn)
	_ = decoder.Decode(&data)
	json_data, err := json.Marshal(data)
    if err != nil {
        fmt.Println("Error marshaling map:", err)
        return
    }

    message_type := ""

	if _, ok := data["1"]; ok {
		message_type = "schedule"
	} else if _, ok := data["op_1"]; ok {
        message_type = "rainstorm_init"
    } else if _, ok := data["Start"]; ok {
		message_type = "source"
	} else if _,ok := data["tuples"]; ok {
		message_type = "tuples"
	} else if _,ok := data["grep"]; ok {
		message_type = "grep"
	} else if _, ok := data["reschedule"]; ok {
		message_type = "reschedule"
	} else if _, ok := data["reset"]; ok {
		message_type = "reset"
	} else if _, ok := data["kill"]; ok {
		message_type = "kill"
	} else if _, ok := data["message"]; ok {
		message_type = "complete"
	}

	if message_type == "schedule" {
		var schedule map[int][]map[string]string
		err = json.Unmarshal(json_data, &schedule)
		if err != nil {
			fmt.Println("Error unmarshaling JSON to struct:", err)
			return
		}
		// set schedule
		global.Schedule = schedule
		for _, tasks := range global.Schedule {
			for _, task := range tasks {
				// Check if the "port" matches the RainstormAddress
				if task["Port"] == global.Rainstorm_address {
					rainstorm.ResendTuples(task["Log_filename"])
				}
			}
		}
	} else if message_type == "rainstorm_init" {
		var params map[string]string
		err = json.Unmarshal(json_data, &params)
		if err != nil {
			fmt.Println("Error unmarshaling JSON to struct:", err)
			return
		}
		// clear the counts file 
		file, err := os.OpenFile("counts.txt", os.O_WRONLY|os.O_TRUNC, 0644)
		if err != nil {
			fmt.Println("Error opening file: ", err)
			return
		}
		defer file.Close()
		rainstorm.InitiateJob(params)
	} else if message_type == "source" {
		var source_task global.SourceTask
		err = json.Unmarshal(json_data, &source_task)
		rainstorm.CompleteSourceTask(source_task.Src_file, source_task.Start, source_task.End)
	} else if message_type == "tuples" {
		var tuples map[string][]global.Tuple
		_ = json.Unmarshal([]byte(json_data), &tuples)
		if global.Leader_address == global.Rainstorm_address{
			rainstorm.WriteToDest(tuples["tuples"])
		} else {
			rainstorm.CompleteTask(tuples["tuples"])
		}
	} else if message_type == "reschedule" {
		if global.Reschedule_called == false {
			fmt.Println("RESCHEDULE MESSAGE RECEIVED")

			var message map[string]string
			_ = json.Unmarshal([]byte(json_data), &message)
			failed_port := message["reschedule"]

			rainstorm.Reschedule(failed_port[:36])
			global.Reschedule_called = true
		}
	} else if message_type == "reset" {
		global.Schedule = make(map[int][]map[string]string)
		global.Batches = make(map[string][]global.Tuple)
		global.AckBatches = make(map[string]string) 
	} else if message_type == "kill" {
		syscall.Kill(os.Getpid(), syscall.SIGINT) 
	} else if message_type == "complete" {
		var output map[string]string
		err = json.Unmarshal(json_data, &output)
		fmt.Println(output["message"])
	}
}