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
)
var i = 1
func CompleteSourceTask(hydfs_file string, destination string, start_line int, end_line int, conn net.Conn) {
	file, err := os.Open("file-store/"+ hydfs_file)
	if err != nil {
		local_filename := "local_file-"+ strconv.Itoa(i)
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
				conn, err := util.DialTCPClient(global.Schedule[keyToUse][partition])
				res := fmt.Sprintf("tuple %s,%s is being sent for next stage to: %s",rec.Tuple[0], rec.Tuple[1], global.Schedule[keyToUse][partition])
				fmt.Println(res)
				if err != nil {
					fmt.Println("Error dialing tcp server", err)
				}
				encoder  := json.NewEncoder(conn)
				errc := encoder.Encode(rec)
				if errc != nil {
					fmt.Println("Error encoding data in create", err)
				}
				buffer := make([]byte, 1024) // Create a buffer to hold the acknowledgment
				n, err := conn.Read(buffer)
				if err != nil {
					fmt.Println("failed to receive acknowledgment: %w", err)
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


