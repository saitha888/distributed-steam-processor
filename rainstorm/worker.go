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
	"io/ioutil"
	"time"

	// "github.com/gofrs/flock"
)

var mu sync.Mutex 

var (
	taskTimer       *time.Timer
	timeoutDuration = 10 * time.Second
	cancelChan      chan struct{} // Channel to signal cancellation
)

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
			unique_id := strconv.Itoa(util.GetUniqueNodeID(key+value))
			record := global.Tuple{
				ID: unique_id,
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
			global.AppendMutex.Lock()
			log := fmt.Sprintf("%s|%s|%s|%s|processed\n", record.ID, record.Key, record.Value, strconv.Itoa(record.Stage))
			hydfs.AppendStringToFile(log, GetAppendLog(0))
			global.AppendMutex.Unlock()
		}
		if line_num > end_line {
			break
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Println("error reading file: %w", err)
		return
	}
	AckTask(0)
}

func CompleteTask(tuples []global.Tuple) {
	task_to_log := make(map[string]string)
	append_to_send := make(map[int]string)
	state_to_send := ""
	for _, tuple := range tuples {
		id := tuple.ID
		key := tuple.Key 
		value := tuple.Value 
		src := tuple.Src
		curr_stage := tuple.Stage 
		resetTaskTimer(curr_stage)
		log_name := GetAppendLog(curr_stage)
		append_content := ""
		if _, ok := task_to_log[log_name]; ok {
			append_content = task_to_log[log_name]
		} else {
			append_content = hydfs.GetFileInVariable(log_name)
			task_to_log[log_name] = append_content
		}
		
		unique_id := strconv.Itoa(util.GetUniqueNodeID(key+value))
		// find the unique id in the append only file, check its state
		lines := GetMatchingLines(log_name, unique_id)

		if lines < 1 { // if it isn't there 
			// process it with the executable
			// add the tuples to the batch global variable
			op := GetOperation(curr_stage)
			command := "./exe/" + op
			output := []byte{}
			if curr_stage == 1 {
				cmd := exec.Command(command, key, value, global.Schedule[curr_stage][0]["Pattern"])
				output, _ = cmd.CombinedOutput()
			} else {
				state_log := GetStateLog()
				if state_log == "" { // not a stateful operation
					cmd := exec.Command(command, key, value)
					output, _ = cmd.CombinedOutput()
				} else {
					global.StateMutex.Lock()
					// Read the file
					data, err := ioutil.ReadFile("counts.txt")
					if err != nil {
						fmt.Println("Error reading file:", err)
					}
					cmd := exec.Command(command, key, string(data))
					output, _ = cmd.CombinedOutput()

					// Open the file in append mode, creating it if it doesn't exist
					file, err := os.OpenFile("counts.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
					if err != nil {
						fmt.Println("Error opening file:", err)
					}
					defer file.Close()

					// Append the new word and count to the file
					result := string(output)
					_, err = file.WriteString(result)
					if err != nil {
						fmt.Println("Error writing to file:", err)
					}
					state_to_send += result + "\n"
					global.StateMutex.Unlock()
				}
			}	
			
			ret_tuple := strings.SplitN(strings.TrimSpace(string(output)), " ", 2)
			if ret_tuple == nil || len(ret_tuple) != 2 {
				global.AckBatchesMutex.Lock()
				filename := GetAppendLogAck(curr_stage - 1, src)
				if _, exists := global.AckBatches[filename]; exists {
					global.AckBatches[filename] += id + "|ack\n"
		
				} else {
					global.AckBatches[filename] = id + "|ack\n"
		
				}
				global.AckBatchesMutex.Unlock()
				continue
			}

			new_tuple := global.Tuple{
				ID : unique_id,
				Key : ret_tuple[0],
				Value : ret_tuple[1],
				Src : global.Rainstorm_address,
				Stage : curr_stage + 1,
			}

			log := fmt.Sprintf("%s|%s|%s|%s|processed\n", new_tuple.ID, new_tuple.Key, new_tuple.Value, strconv.Itoa(new_tuple.Stage))
			if _, exists := append_to_send[curr_stage]; exists {
				append_to_send[curr_stage] += log
			} else {
				append_to_send[curr_stage] = log
			}

			dest_address := ""
			if _, exists := global.Schedule[curr_stage+1]; exists {
				dest_address = global.Schedule[new_tuple.Stage][util.GetHash(ret_tuple[0]) % len(global.Schedule[0])]["Port"]
			} else {
				dest_address = global.Leader_address
			}

			//send batches to next stage
			global.BatchesMutex.Lock()
			if _, exists := global.Batches[dest_address]; exists {
				global.Batches[dest_address] = append(global.Batches[dest_address], new_tuple)
			} else {
				global.Batches[dest_address] = []global.Tuple{new_tuple}
			}
			global.BatchesMutex.Unlock()
		} else {
			fmt.Println("already in append only log", log_name, unique_id)
		}
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
	for stage,log := range append_to_send {
		global.AppendMutex.Lock()
		hydfs.AppendStringToFile(log, GetAppendLog(stage))
		global.AppendMutex.Unlock()
	}
	state_log := GetStateLog()
	if state_log != "" {
		hydfs.AppendStringToFile(state_to_send, state_log)
	}

}

func resetTaskTimer(stage int) {
	if cancelChan != nil {
		close(cancelChan) 
	}

	cancelChan = make(chan struct{})

	go func(localCancelChan chan struct{}) {
		select {
		case <-time.After(timeoutDuration):
				AckTask(stage)
		case <-localCancelChan:
			return
		}
	}(cancelChan)
}
