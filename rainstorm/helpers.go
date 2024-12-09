package rainstorm

import (
	"bufio"
	"fmt"
	"os"
	"distributed_system/hydfs"
	"distributed_system/global"
	"encoding/json"
	"distributed_system/util"
	"strings"
	"strconv"
	"time"
	"os/exec"
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
	if err != nil {
		return
	}
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
			continue
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
	for filename, contents := range global.AckBatches {
		if contents == "" {
			continue
		}
		hydfs.AppendStringToFile(contents,filename)
		global.AckBatches[filename] = ""
	}
	global.AckBatchesMutex.Unlock()
}

func GetMatchingLines(hydfs_filename string, pattern string) int {
	// get the hydfs file and place it in local file
	localfilename := "temp/temp-file-" + strconv.FormatInt(time.Now().UnixMilli(), 10)

	_, _ = os.Create(localfilename)

	hydfs.GetFile(hydfs_filename, localfilename)

	// grep the file
	pattern = strings.TrimSpace(pattern)
	command := "grep -c " + "-- " + pattern + " " + localfilename
	cmd := exec.Command("sh", "-c", command)
	output, err := cmd.CombinedOutput()

	// delete the local file
	_ = os.Remove(localfilename)
	
	if err != nil { // couldn't find pattern
		return 0
	}

	// return the line count
	line_count, _ := strconv.Atoi(strings.TrimSpace(string(output)))
	return line_count
}

func AckTask(curr_stage int) {
	conn, err := util.DialTCPClient(global.Leader_address) 
	for i, task := range global.Schedule[curr_stage] {
		if task["Port"] == global.Rainstorm_address {
			message := map[string]string{"message": fmt.Sprintf("%s task %d at %s Complete", task["Op"], i, task["Port"]),}
			encoder  := json.NewEncoder(conn)
			err = encoder.Encode(message)
			if err != nil {
				fmt.Println("Error encoding data in send schedule", err)
				continue
			}
		}
	}
}

func ResendTuples(hydfs_filename string) {
	// // get the hydfs file and place it in local file
	// file_hash := util.GetHash(hydfs_filename)
	// ports := hydfs.GetFileServers(file_hash)
	// dest_port := ports[0][:36]
	localfilename := "temp/temp-file-" + strconv.FormatInt(time.Now().UnixMilli(), 10)

	_, _ = os.Create(localfilename)

	hydfs.GetFile(hydfs_filename, localfilename)

	file, err := os.Open(localfilename)
	if err != nil {
		fmt.Printf("Error opening file: %v\n", err)
	}
	defer file.Close()

	word_counts := make(map[string][][]string) // unique_id maps to list of tuples with it

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()

		words := strings.Split(line, "|")
		if len(words) > 0 {
			unique_id := words[0]
			word_counts[unique_id] = append(word_counts[unique_id] , words)
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Printf("Error reading file: %v\n", err)
	}
	// Find words with less than 2 counts
	var incomplete [][]string
	for _, lines := range word_counts {
		if len(lines) == 1 {
			incomplete = append(incomplete, lines[0])
		}
	}

	for _, line := range incomplete {

		if len(line) <= 3{
			fmt.Println("ack that is seen twice", line)
			continue
		} 
		stage, _ := strconv.Atoi(line[3])
		// make the new tuple
		tuple := global.Tuple {
			ID: line[0],
			Key:   line[1],
			Value: line[2],
			Src:   global.Rainstorm_address,
			Stage: stage,
		}
		// get the destination address
		dest_address := ""
		if _, exists := global.Schedule[tuple.Stage]; exists {
			dest_address = global.Schedule[tuple.Stage][util.GetHash(tuple.Key) % len(global.Schedule[0])]["Port"]
		} else {
			dest_address = global.Leader_address
		}
		fmt.Println("tuple to resend: ", tuple)
		fmt.Println("to port: " + dest_address)

		// add to the batch
		global.BatchesMutex.Lock()
		if _, exists := global.Batches[dest_address]; exists {
			global.Batches[dest_address] = append(global.Batches[dest_address], tuple)
		} else {
			global.Batches[dest_address] = []global.Tuple{tuple}
		}
		global.BatchesMutex.Unlock()
	}
	// delete the local file
	_ = os.Remove(localfilename)
}

func GetAppendLog(stage int) string {
	for _, task := range global.Schedule[stage] {
		// Check if the "port" matches the RainstormAddress
		if task["Port"] == global.Rainstorm_address {
			return task["Log_filename"]
		}
	}
	return ""
}

func GetAppendLogAck(stage int, src string) string {
	for _, task := range global.Schedule[stage] {
		// Check if the "port" matches the RainstormAddress
		if task["Port"] == src {
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

func MergeLogs() {
	for i, _ := range global.Schedule {
		for j, _ := range global.Schedule[0] {
			hydfs.MergeLocally(global.Schedule[i][j]["Log_filename"])
		}
	}
}

func GetStateLog() string {
	for _, task := range global.Schedule[2] {
		// Check if the "port" matches the RainstormAddress
		if task["Port"] == global.Rainstorm_address {
			return task["State_filename"]
		}
	}
	return ""
}

