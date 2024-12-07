package servers

import (
	"net"
	"fmt"
	"distributed_system/global"
	"encoding/json"
	"distributed_system/rainstorm"
	"os"
)


//starts tcp server that listens for grep commands
func RainstormServer() {
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
	} else if _,ok := data["Key"]; ok {
		message_type = "stream"
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
		// global.IsSinkMachine = false
		// global.LastSentLine = 0
		rainstorm.InitiateJob(params)
	} else if message_type == "source" {
		var source_task global.SourceTask
		err = json.Unmarshal(json_data, &source_task)
		rainstorm.CompleteSourceTask(source_task.Src_file, source_task.Start, source_task.End)
	} else if message_type == "stream" {
		var stream global.Tuple
		err = json.Unmarshal(json_data, &stream)
		rainstorm.CompleteTask(stream.Key, stream.Value, stream.Src, stream.Stage)
	}
}