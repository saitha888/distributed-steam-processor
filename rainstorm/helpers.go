package rainstorm

import (
	"bufio"
	"fmt"
	"os"
	"distributed_system/hydfs"
	"encoding/json"
	"distributed_system/util"
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
