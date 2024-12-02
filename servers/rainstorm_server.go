package servers

import (
	"net"
	"fmt"
	"distributed_system/global"
	"encoding/json"
	"distributed_system/rainstorm"
	"time"
	"distributed_system/util"
	"strconv"
)


//starts tcp server that listens for grep commands
func RainstormServer() {
	// check if machine is assigned to op2 task
	if _, exists := global.Tasks[2]; exists {
		// send batch message to destination file every 100 ms
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		go func() {
			for range ticker.C {
				rainstorm.SendSinkBatch()
			}
		}()
	}
	

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

	if _, ok := data["0-source"]; ok {
		message_type = "schedule"
	} else if _, ok := data["op_1"]; ok {
        message_type = "rainstorm_init"
    } else if _, ok := data["Start"]; ok {
		message_type = "source"
	} else if _,ok := data["Dest_file"]; ok {
		message_type = "stream"
	} 

	if message_type == "schedule" {
		var schedule map[string][]string 
		err = json.Unmarshal(json_data, &schedule)
		if err != nil {
			fmt.Println("Error unmarshaling JSON to struct:", err)
			return
		}
		// set schedule
		global.Schedule = schedule

		// set tasks of machine
		for stage, machines := range global.Schedule {
			if stage == "dest_file" {
				continue
			}
			if util.Contains(machines, global.Rainstorm_address) {
				stage_num, _ := strconv.Atoi(stage[:1])
				global.Tasks[stage_num] = stage[2:]
			}
		}
	} else if message_type == "rainstorm_init" {
		var params map[string]string
		err = json.Unmarshal(json_data, &params)
		if err != nil {
			fmt.Println("Error unmarshaling JSON to struct:", err)
			return
		}
		rainstorm.InitiateJob(params)
	} else if message_type == "source" {
		var source_task global.SourceTask
		err = json.Unmarshal(json_data, &source_task)
		rainstorm.CompleteSourceTask(source_task.Src_file, source_task.Dest_file, source_task.Start, source_task.End, conn)
	} else if message_type == "stream" {
		var stream global.Stream
		err = json.Unmarshal(json_data, &stream)
		rainstorm.CompleteTask(stream.Src_file, stream.Dest_file, stream.Tuple, stream.Stage, conn)
	}
}