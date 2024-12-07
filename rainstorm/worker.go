package rainstorm

import (
	"distributed_system/util"
	"distributed_system/global"
	"distributed_system/hydfs"
	"strconv"
	"fmt"
	"os"
	"bufio"
	"sync"
	"strings"
	"os/exec"

	// "github.com/gofrs/flock"
)

var mu sync.Mutex 

func CompleteSourceTask(hydfs_file string, start_line int, end_line int) {
	file, err := os.Open("file-store/"+ hydfs_file)
	if err != nil {
		local_filename := "local_file-"+ strconv.Itoa(1)
		hydfs.GetFile(hydfs_file,local_filename)
		file_import, err2 := os.Open(local_filename)
		if err2 != nil {
			fmt.Println("Error in completing source task",err2)
		}
		file = file_import
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	line_num := 0

	for scanner.Scan() {
		line_num++
		if line_num >= start_line && line_num <= end_line {
			key := fmt.Sprintf("%s:%d", hydfs_file, line_num)
			value := scanner.Text()
			line_id := strconv.Itoa(line_num)
			record := global.Tuple{
				ID: line_id,
				Key: key,
				Value: value,
				Stage: 1,
				Src: global.Rainstorm_address,
			}
			partition := util.GetHash(record.Key) % len(global.Schedule[0]) // find the destination the tuple should go to 
			dest_address := global.Schedule[1][partition]["Port"] // add to the batch
			global.BatchesMutex.Lock()
			if _, exists := global.Batches[dest_address]; exists {
				global.Batches[dest_address] = append(global.Batches[dest_address], record)
			} else {
				global.Batches[dest_address] = []global.Tuple{record}
			}
			global.BatchesMutex.Unlock()
		}
		if line_num > end_line {
			break
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Println("error reading file: %w", err)
		return
	}
	return
}

func CompleteTask(tuples []global.Tuple) {
	task_to_log := make(map[string]string)
	append_to_send := make(map[int]string)
	dest_string := ""
	for _, tuple := range tuples {
		id := tuple.ID
		key := tuple.Key 
		value := tuple.Value 
		curr_stage := tuple.Stage 

		log_name := GetAppendLog(curr_stage)
		fmt.Println("curr log name: ", log_name)
		append_content := ""
		if _, ok := task_to_log[log_name]; ok {
			append_content = task_to_log[log_name]
			fmt.Println("file contents if in map: " + append_content)
		} else {
			append_content = hydfs.GetFileInVariable(log_name)
			task_to_log[log_name] = append_content
			fmt.Println("file contents if in else: " + append_content)
		}
		
		unique_id := strconv.Itoa(util.GetHash(key+value))
		// find the unique id in the append only file, check its state
		lines := GetMatchingLines(log_name, unique_id)

		if lines <= 1 { // if it isn't there 
			// process it with the executable
			// add the tuples to the batch global variable
			op := GetOperation(curr_stage)
			command := "./exe/" + op
			output := []byte{}
			if curr_stage == 1 {
				cmd := exec.Command(command, key, value, global.Schedule[curr_stage][0]["Pattern"])
				output, _ = cmd.CombinedOutput()
			} else {
				cmd := exec.Command(command, key, value)
				output, _ = cmd.CombinedOutput()
			}	
			
			ret_tuple := strings.Split(string(output), " ")
			fmt.Println("ret tuple: ", ret_tuple)
			if ret_tuple == nil || len(ret_tuple) != 2 {
				continue
			}
			if _, exists := global.Schedule[curr_stage + 1]; exists {
				new_tuple := global.Tuple{
					ID : unique_id,
					Key : ret_tuple[0],
					Value : ret_tuple[1],
					Src : global.Rainstorm_address,
					Stage : curr_stage + 1,
				}
				log := fmt.Sprintf("%s \n", id)
				append_to_send[curr_stage] += log
	
				dest_address := global.Schedule[new_tuple.Stage][util.GetHash(ret_tuple[0]) % 3]["Port"]
	
				//send batches to next stage
				global.BatchesMutex.Lock()
				if _, exists := global.Batches[dest_address]; exists {
					global.Batches[dest_address] = append(global.Batches[dest_address], new_tuple)
				} else {
					global.Batches[dest_address] = []global.Tuple{new_tuple}
				}
				global.BatchesMutex.Unlock()
	
				//send ack back to sender machine
				global.AckBatchesMutex.Lock()
				ack := global.Ack{
					ID : id,
					Stage : curr_stage - 1,
				}
				if _, exists := global.AckBatches[dest_address]; exists {
					global.AckBatches[dest_address] = append(global.AckBatches[dest_address], ack)
				} else {
					global.AckBatches[dest_address] = []global.Ack{ack}
				}
				global.AckBatchesMutex.Unlock()
			} else {
				output := fmt.Sprintf("%s, %s\n", ret_tuple[0], ret_tuple[1])
				dest_string += output
			}
		}
	}
	if len(dest_string) >= 0 {
		global.DestMutex.Lock()
		hydfs.AppendStringToFile(dest_string, global.Schedule[0][0]["Dest_filename"])
		global.DestMutex.Unlock()
	}
	for stage,log := range append_to_send {
		global.AppendMutex.Lock()
		hydfs.AppendStringToFile(log, GetAppendLog(stage))
		global.AppendMutex.Unlock()
	}
}


// func SendSinkBatch() {
// 	// Create a file lock
// 	file_lock := flock.New("counts.txt")

// 	// Acquire the lock
// 	err := file_lock.Lock()
// 	if err != nil {
// 		fmt.Println("error acquiring lock: %w", err)
// 	}
// 	defer file_lock.Unlock() // Ensure the lock is released

// 	// open the counts file
// 	src, err := os.Open("counts.txt")
// 	if err != nil {
// 		fmt.Println("Error opening source file:", err)
// 		return
// 	}
// 	defer src.Close()
// 	src.Close()

// 	// create the batch file to send
// 	dest, err := os.Create("temp.txt")
// 	if err != nil {
// 		fmt.Println("Error creating destination file:", err)
// 		return
// 	}
// 	defer dest.Close()

// 	scanner := bufio.NewScanner(src)
// 	writer := bufio.NewWriter(dest)

// 	curr_line := 0
// 	last_line := -1

// 	// get the contents to write to the file
// 	for scanner.Scan() {
// 		// Skip lines until the starting line number
// 		if curr_line >= global.LastSentLine {
// 			fmt.Println("curr line: ", curr_line)
// 			fmt.Println("last sent line: ", global.LastSentLine)
// 			// Write the current line to the temp file
// 			_, err := writer.WriteString(scanner.Text() + "\n")
// 			if err != nil {
// 				fmt.Println("Error writing to destination file:", err)
// 				return
// 			}
// 			last_line = curr_line
// 		}
// 		curr_line++
// 	}

// 	// write to the file
// 	if err := writer.Flush(); err != nil {
// 		fmt.Println("Error flushing writer:", err)
// 		return
// 	}

// 	// update the last line sent
// 	if last_line != -1 {
// 		global.LastSentLine = last_line + 1
// 	}

// 	file_info, _ := os.Stat("temp.txt")
// 	if file_info.Size() != 0 { // only send if there was an update
// 		// Send an append request to the destination file of the current contents
// 		hydfs.AppendFile("temp.txt", global.Schedule["dest_file"][0])
// 	}

// 	//  Delete the temp file
// 	err = os.Remove("temp.txt")
// 	if err != nil {
// 		fmt.Println("Error deleting file:", err)
// 		return
// 	}
// }