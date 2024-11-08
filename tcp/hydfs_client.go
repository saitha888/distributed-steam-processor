package tcp 

import (
	"distributed_system/membership"
	"fmt"
    "net"
    "os"
	"strconv"
	"time"
	"io"
)

func GetFile(hydfs_file string, local_file string) {
	file_hash := membership.GetHash(hydfs_file)
	node_ids := membership.GetFileServers(file_hash)

	machine_num, _ := strconv.Atoi(machine_number)
	replica_num := machine_num % 3

	file_server := node_ids[replica_num][:36]
	fmt.Println(file_server)

	server_num := node_ids[0][13:15]

	conn, err := net.Dial("tcp", file_server)
    if err != nil {
        fmt.Println(err)
        return
    }
    defer conn.Close() 

    message := fmt.Sprintf("get %s-%s", server_num, hydfs_file)

    conn.Write([]byte(message))

    // write the command to an output file
	buf := make([]byte, 1024) // Buffer to hold chunks of data
	var response string        // Variable to hold the full response

	for {
		n, err := conn.Read(buf)
		if err != nil {
			// If we've reached the end of the data, break out of the loop
			if err == io.EOF {
				break
			}
			fmt.Println("Error reading from connection:", err)
			return
		}
		response += string(buf[:n])
	}
	err = WriteToFile(local_file, response)
	if err != nil {
		return
	}
	output := fmt.Sprintf("file %s retrieved from server %s, wrote to %s", hydfs_file, file_server, local_file)
	fmt.Println(output)
}

// Function to write content to a local file
func WriteToFile(filename string, content string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteString(content)
	if err != nil {
		return err
	}
	return nil
}

func CreateFile(localfilename string, HyDFSfilename string) {
	// find which machine to create the file on
	file_hash := membership.GetHash(HyDFSfilename)
	node_ids := membership.GetFileServers(file_hash)

	// get the contents of the local filename
	file_contents, err := os.ReadFile(localfilename)
	if err != nil {  // local filename is invalid
		fmt.Println("File doesn't exist locally:", err)
		return
	}

	content := string(file_contents)
	replica_num := "0"
	// connect to the machine 
	for i,node_id := range node_ids {
		fmt.Println(node_id)
		if i == 0 {
			replica_num = node_id[13:15]
		}
		node_port := node_id[:36]

		conn, err := net.Dial("tcp", node_port)
		if err != nil {
			fmt.Println(err)
			return
		}
		defer conn.Close()
	
		// send the file message to the machine
		message := "create " + HyDFSfilename + " " + replica_num + " " + content
		conn.Write([]byte(message))
		
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			fmt.Println(err)
			return
		}
		response := string(buf[:n])
		fmt.Println(response)
	}
}

func AppendFile(local_file string, hydfs_file string) {

	replicas := membership.GetFileServers(membership.GetHash(hydfs_file))
	machine_num, err := strconv.Atoi(os.Getenv("MACHINE_NUMBER"))
	if err != nil {
		return
	}
	replica := replicas[machine_num % 3]
	replica_num := replicas[0][13:15]


	// get the contents of the local filename
	file_contents, err := os.ReadFile(local_file)
	if err != nil {  // local filename is invalid
		fmt.Println("File doesn't exist locally:", err)
		return
	}

	content := string(file_contents)

	// connect to port to write file contents into replica

	port := replica[:36]
	timestamp := time.Now().Format("15:04:05.000")
	conn, err := net.Dial("tcp", port)
	if err != nil {
		fmt.Println(err)
		return
	}
	filename := hydfs_file + "-" + timestamp

	message := "create" + " " + filename + " " + replica_num + " " + content
	conn.Write([]byte(message))
		
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		fmt.Println(err)
		return
	}
	response := string(buf[:n])
	fmt.Println(response)

}