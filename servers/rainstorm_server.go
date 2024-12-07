package servers

import (
	"net"
	"fmt"
	"distributed_system/global"
	"encoding/json"
	"distributed_system/rainstorm"
	"os"
	"time"
	"os/exec"
)


//starts tcp server that listens for grep commands
func RainstormServer() {
	ticker := time.NewTicker(100 * time.Millisecond)
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
	} else if _,ok := data["acks"]; ok {
		message_type = "acks"
	} else if _,ok := data["grep"]; ok {
		message_type = "grep"
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
		rainstorm.InitiateJob(params)
	} else if message_type == "source" {
		var source_task global.SourceTask
		err = json.Unmarshal(json_data, &source_task)
		rainstorm.CompleteSourceTask(source_task.Src_file, source_task.Start, source_task.End)
	} else if message_type == "tuples" {
		var tuples map[string][]global.Tuple
		_ = json.Unmarshal([]byte(json_data), &tuples)
		rainstorm.CompleteTask(tuples["tuples"])
	} else if message_type == "acks" {
		var acks map[string][]global.Ack
		_ = json.Unmarshal([]byte(json_data), &acks)
		rainstorm.ProcessAcks(acks["acks"])
	} else if message_type == "grep" {
		var params map[string]string
		_ = json.Unmarshal(json_data, &params)
		command := params["grep"]
		fmt.Println("running this grep command: ", command)
		cmd := exec.Command("sh", "-c", command)
        output, err := cmd.CombinedOutput()
        if err != nil {
            fmt.Println(err)
            return
        }
        encoder := json.NewEncoder(conn)
        err = encoder.Encode(string(output))
		fmt.Println("sending this output back: " + string(output))
	}
}