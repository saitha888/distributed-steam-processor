package tcp 

import (
	"distributed_system/udp"
	"fmt"
    "net"
    "os"
)

func GetFile(hydfs_file string, local_file string) {
	file_hash := udp.GetHash(hydfs_file)
	ring_map := udp.GetRing()
	file_server := ""
	iterator := ring_map.Iterator()
	server_num := ""
	for iterator.Next() {
		if iterator.Key().(int) > file_hash {
			file_server = iterator.Value().(string)[:36]
			server_num = iterator.Value().(string)[13:15]
		}
		break
	} 
	if file_server == "" {
		iterator.First()
		file_server = iterator.Value().(string)[:36]
	}

	conn, err := net.Dial("tcp", file_server)
    if err != nil {
        fmt.Println(err)
        return
    }
    defer conn.Close()

    message := fmt.Sprintf("get %s-%s", server_num, hydfs_file)

    conn.Write([]byte(message))

    // write the command to an output file
	buf := make([]byte, 1024)
    n, err := conn.Read(buf)
	if err != nil {
        fmt.Println(err)
        return
    }
	response := string(buf[:n])
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
	file_hash := udp.GetHash(HyDFSfilename)
	node_ids := udp.GetFileServers(file_hash)

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