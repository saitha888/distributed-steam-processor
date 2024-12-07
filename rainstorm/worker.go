package rainstorm

import (
	"distributed_system/util"
	"distributed_system/global"
	"distributed_system/hydfs"
	"encoding/json"
	"strconv"
	"fmt"
	"os"
	"bufio"
	"sync"
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
	var wg sync.WaitGroup
	scanner := bufio.NewScanner(file)
	line_num := 0

	for scanner.Scan() {
		line_num++
		if line_num >= start_line && line_num <= end_line {
			key := fmt.Sprintf("%s:%d", hydfs_file, line_num)
			value := scanner.Text()
			record := global.Tuple{
				Key: key,
				Value: value,
				Stage: 1,
				Src: global.Rainstorm_address,
			}
			wg.Add(1)
			// Start a goroutine for sending the tuple
			go func(rec global.Tuple) {
				defer wg.Done() // Decrement the counter when goroutine completes
				partition := util.GetHash(rec.Key) % len(global.Schedule[0]) // find the destination the tuple should go to 
				dest_address := global.Schedule[1][partition]["Port"] // add to the batch
				global.BatchesMutex.Lock()
				if _, exists := global.Batches[dest_address]; exists {
					global.Batches[dest_address] = append(global.Batches[dest_address], record)
				} else {
					global.Batches[dest_address] = []global.Tuple{record}
				}
				global.BatchesMutex.Unlock()
				// next_stage_conn, err_s := util.DialTCPClient(global.Schedule[1][partition]["Port"])
				// res := fmt.Sprintf("tuple %s,%s is being sent for next stage to: %s",rec.Key, rec.Value, global.Schedule[1][partition])
				// fmt.Println(res)
				// if err_s != nil {
				// 	fmt.Println("Error dialing tcp server", err_s)
				// }
				// encoder  := json.NewEncoder(next_stage_conn)
				// errc := encoder.Encode(rec)
				// if errc != nil {
				// 	fmt.Println("Error encoding data in create", errc)
				// }
			}(record)
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
	fmt.Println("got to task 1: ", tuples)
	// curr_stage := tuples[0]["stage"]
	// next_stage := curr_stage + 1
	// fmt.Println("Reached stage ", next_stage)
	// op := global.Schedule[curr_stage][0]["Op"]
	
	// executable := "./exe/" + op
	// tuples := ""
	// if stage == 1 {
	// 	output, _ := exec.Command(executable, key, value, global.Schedule[0][0]["pattern"]).Output()
	// 	tuples = string(output)
	// } else {
	// 	output, _  := exec.Command(executable, key, value).Output()
	// 	tuples = string(output)
	// }
	// new_tuple := strings.Split(strings.TrimSpace(tuples), " ")
	// record := Global.Tuple {
	// 	Key : new_tuple[0],
	// 	Value: new_tuple[1],
	// 	Stage: next_stage,
	// 	Src: global.Rainstorm_address,
	// }
}

func SendBatches() {
	global.BatchesMutex.Lock()
	for destination, tuples := range global.Batches {
		if len(tuples) == 0 {
			continue
		}
		// Simulate sending tuples to the destination
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