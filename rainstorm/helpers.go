package rainstorm

import (
	"bufio"
	"fmt"
	"os"
	"distributed_system/hydfs"
	"distributed_system/global"
	"encoding/json"
	"distributed_system/util"
	"net"
	"strings"
	"strconv"
)

func CountLines(file_path string) int {
	// get the hydfs file and save it to a local file
	local_file_path := "rainstorm-countlines-file"
	hydfs.GetFile(file_path, local_file_path)
	file, err := os.Open(local_file_path)
	if err != nil {
		return 0
	}
	defer file.Close()

	// iterate through the file and count the number of lines
	scanner := bufio.NewScanner(file)
	line_count := 0

	for scanner.Scan() {
		line_count++
	}

	// remove the local file
	_ = os.Remove(local_file_path)

	return line_count
}


// get the tcp version of the node_id (value in the ring map)
func GetRainstormVersion(id string) string {
    bytes := []byte(id)
	bytes[32] = '7'
	id = string(bytes)
    
    return id
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

func SendBatches() {
	global.BatchesMutex.Lock()
	for destination, tuples := range global.Batches {
		if len(tuples) == 0 {
			continue
		}
		// send tuples to the destination
		conn, err := util.DialTCPClient(destination)
		if err != nil {
			fmt.Println("Error dialing tcp server", err)
		}
		message := make(map[string][]global.Tuple)

		// Add a dummy key with a list of tuples as the value
		message["tuples"] = tuples
		encoder  := json.NewEncoder(conn)
		err = encoder.Encode(message)

		// Clear the list for the current destination
		global.Batches[destination] = nil
	}
	global.BatchesMutex.Unlock()
}

func SendAckBatches() {
	global.AckBatchesMutex.Lock()
	for destination, acks := range global.AckBatches {
		if len(acks) == 0 {
			continue
		}
		// send tuples to the destination
		conn, err := util.DialTCPClient(destination)
		if err != nil {
			fmt.Println("Error dialing tcp server", err)
		}
		message := make(map[string][]global.Ack)

		// Add a dummy keytupleIDSa list of tuples as the value
		message["acks"] = acks
		encoder := json.NewEncoder(conn)
		err = encoder.Encode(message)
		
		global.AckBatches[destination] = nil
	}
	global.AckBatchesMutex.Unlock()
}

func GetMatchingLines(filename string, pattern string) int {
	file_hash := util.GetHash(filename)
	ports := hydfs.GetFileServers(file_hash)

	dest_port := GetRainstormVersion(ports[0][:36])

	// connect to the port
    conn, err := net.Dial("tcp", dest_port)
    if err != nil {
        fmt.Println(err)
        return 0
    }
    defer conn.Close()

	message := make(map[string]string)
	message["grep"] = "grep -c " + pattern + " file-store/" + dest_port[13:15] + "-" + filename
	fmt.Println("sending this grep message: " + message["grep"] + " to port: " + dest_port)

	// Encode the structure into JSON
	encoder := json.NewEncoder(conn)
	err = encoder.Encode(message)
	if err != nil {
		fmt.Println("Error encoding structure in get to json", err)
	}

    var response string
    decoder := json.NewDecoder(conn)
    err = decoder.Decode(&response)
    if err != nil {
        fmt.Println("error sending grep command")
    }

	fmt.Println("received this response from machine: " + response)
    line_count_str := strings.TrimSpace(response)
    line_count, err := strconv.Atoi(line_count_str)
    if err != nil {
        fmt.Printf("Error converting line count from %s to int: %v\n", dest_port, err)
        return 0
    }
    return line_count
}

func GetAppendLog(stage int) string {
	for _, task := range global.Schedule[stage] {
		// Check if the "port" matches the RainstormAddress
		fmt.Println("currently on this task: ", task)
		if task["Port"] == global.Rainstorm_address {
			fmt.Println("returning this filename: " + task["Log_filename"])
			return task["Log_filename"]
		}
	}
	return ""
}

func GetOperation(stage int) string {
	for _, task := range global.Schedule[stage] {
		if task["Port"] == global.Rainstorm_address {
			return task["Op"]
		}
	}
	return ""
}

func ProcessAcks(acks []global.Ack) {
	stage_to_file := make(map[int]string)
	file_to_content := make(map[string]string)
	for _, ack := range acks {
		id := ack.ID
		stage := ack.Stage

		append_file := ""
		if _, ok := stage_to_file[stage]; ok {
			append_file = stage_to_file[stage]
		} else {
			append_file := GetAppendLog(stage)
			stage_to_file[stage] = append_file
			file_to_content[append_file] = ""
		}
		file_to_content[append_file] += id + "\n"
	}

	for file, content := range file_to_content {
		global.AppendMutex.Lock()
		hydfs.AppendStringToFile(content, file)
		global.AppendMutex.Unlock()
	}
}