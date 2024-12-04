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
	"net"
	"strings"
	"sync"
	"github.com/gofrs/flock"
	"os/exec"
)

func CompleteSourceTask(hydfs_file string, destination string, start_line int, end_line int, conn net.Conn) {
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
			record := global.Stream{
				Src_file: hydfs_file,
				Dest_file: destination,
				Tuple: []string{key, value},
				Stage: 1,
			}
			msg := fmt.Sprintf("%d\n", line_num) // Add a newline for easier parsing
			_, err := conn.Write([]byte(msg))   // Send the line number as plain text
			if err != nil {
				fmt.Printf("Error sending line number for line %d: %v\n", line_num, err)
				continue
			}
			wg.Add(1)
			// Start a goroutine for sending the tuple
			go func(rec global.Stream) {
				defer wg.Done() // Decrement the counter when goroutine completes
				partition := util.GetHash(rec.Tuple[0]) % len(global.Schedule["0-source"])
				var keyToUse string
				for key := range global.Schedule {
					if strings.HasPrefix(key, "1-") {
						keyToUse = key
						break
					}
				}
				next_stage_conn, err_s := util.DialTCPClient(global.Schedule[keyToUse][partition])
				res := fmt.Sprintf("tuple %s,%s is being sent for next stage to: %s",rec.Tuple[0], rec.Tuple[1], global.Schedule[keyToUse][partition])
				fmt.Println(res)
				if err_s != nil {
					fmt.Println("Error dialing tcp server", err_s)
				}
				encoder  := json.NewEncoder(next_stage_conn)
				errc := encoder.Encode(rec)
				if errc != nil {
					fmt.Println("Error encoding data in create", errc)
				}
				buffer := make([]byte, 1024) // Create a buffer to hold the acknowledgment
				n, errr := next_stage_conn.Read(buffer)
				if errr != nil {
					fmt.Println("failed to receive acknowledgment: %w", errr)
					return 
				}
				if string(buffer[:n]) != "ack" {
					return
				}
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

func CompleteTask(hydfs_file string, destination string, tuple []string, stage int, conn net.Conn) {
	msg := fmt.Sprintf("ack") 
	_, err := conn.Write([]byte(msg)) 
	if err != nil {
		fmt.Printf("Error sending ack", err)
	}
	if len(tuple) > 1 {
		stage_key := FindStageKey(stage)
		// Run the executable on the tuple
		next_stage := FindStageKey(stage+1)
		executable := "./exe/" + stage_key[2:]
		if stage == 2 {
			cmd := exec.Command("sh", "-c", executable+" "+tuple[0]+" "+tuple[1])
			output, _ := cmd.Output()
			fmt.Println("acknowledgement: ", string(output))
			return
		}
		cmd := exec.Command(executable, tuple[0], tuple[1])
		output, err := cmd.Output()
		if err != nil {
			fmt.Printf("Error running executable: %v\n", err)
			return
		}
		lines := strings.Split(strings.TrimSpace(string(output)), "\n")
		var wg sync.WaitGroup
		for _, line := range lines {
			wg.Add(1)
			// Start a goroutine for sending the tuple
			go func(line string) {
				// fmt.Println("next stage: ", next_stage)
				tuple_parts := strings.Split(strings.Trim(strings.TrimSpace(line), "()"), ",")
				record := global.Stream{
					Src_file: hydfs_file,
					Dest_file: destination,
					Tuple: tuple_parts,
					Stage: stage+1,
				}
				key := strings.TrimSpace(tuple_parts[0])
			
				// Calculate the partition using the hash of the word
				partition := util.GetHash(key) % len(global.Schedule[next_stage])
				next_stage_conn, err := util.DialTCPClient(global.Schedule[next_stage][partition])
				res := fmt.Sprintf("Tuple %s is being sent for next stage to: %s", line, global.Schedule[next_stage][partition])
				fmt.Println(res)
				if err != nil {
					fmt.Printf("Error dialing TCP server for tuple %s: %v\n", line, err)
				}
				encoder  := json.NewEncoder(next_stage_conn)
				errc := encoder.Encode(record)
				if errc != nil {
					fmt.Println("Error encoding data in create", errc)
				}
			}(line)
		// ret := fmt.Sprintf("tuples returned for op_1 (%s): %s", stage_key[2:], output)
		// fmt.Println(ret)
		}
	} else if len(tuple) >= 1 {
		fmt.Println("received tuple from split stage", tuple[0])
	}
}

func FindStageKey(stage int) string {
	prefix := strconv.Itoa(stage) + "-"
	stage_key := ""
	for key := range global.Schedule {
		if strings.HasPrefix(key, prefix) {
			stage_key = key
			break
		}
	}
	return stage_key
}

func SendSinkBatch() {
	// Create a file lock
	file_lock := flock.New("counts.txt")

	// Acquire the lock
	err := file_lock.Lock()
	if err != nil {
		fmt.Println("error acquiring lock: %w", err)
	}
	defer file_lock.Unlock() // Ensure the lock is released

	// open the counts file
	src, err := os.Open("counts.txt")
	if err != nil {
		fmt.Println("Error opening source file:", err)
		return
	}
	defer src.Close()

	// create the batch file to send
	dest, err := os.Create("temp.txt")
	if err != nil {
		fmt.Println("Error creating destination file:", err)
		return
	}
	defer dest.Close()

	scanner := bufio.NewScanner(src)
	writer := bufio.NewWriter(dest)

	curr_line := 0
	last_line := 0

	// get the contents to write to the file
	for scanner.Scan() {
		// Skip lines until the starting line number
		if curr_line >= global.LastSentLine {
			// Write the current line to the destination file
			_, err := writer.WriteString(scanner.Text() + "\n")
			if err != nil {
				fmt.Println("Error writing to destination file:", err)
				return
			}
			last_line = curr_line
		}
		curr_line++
	}

	// write to the file
	if err := writer.Flush(); err != nil {
		fmt.Println("Error flushing writer:", err)
		return
	}

	// update the last line sent
	global.LastSentLine = last_line + 1

	// Send an append request to the destination file of the current contents
	hydfs.AppendFile("temp.txt", global.Schedule["dest_file"][0])

	//  Delete the temp file
	err = os.Remove("temp.txt")
	if err != nil {
		fmt.Println("Error deleting file:", err)
		return
	}
}