package tcp 

import (
	"distributed_system/udp"
	"github.com/emirpasic/gods/utils"
	"fmt"
    "net"
    "os"
)

func GetFile(hydfs_file string, local_file string) {
	file_hash := udp.GetHash(hydfs_file)
	ring_map := udp.GetRing()
	file_server := ""
	iterator := ring_map.Iterator()
	for iterator.Next() {
		if utils.StringComparator(iterator.Key(), file_hash) == 1 {
			file_server = iterator.Value().(string)[:36]
		}
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

    message := fmt.Sprintf("get %s", hydfs_file)

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
	// get the current ring map
	ring_map := udp.GetRing()

	// find which machine to create the file on
	file_hash := udp.GetHash(HyDFSfilename)

	// Find the smallest key that is greater than or equal to the hash
	node_ids := []string{}
	iterator := ring_map.Iterator()
	for iterator.Next() {
		if utils.StringComparator(iterator.Key(), file_hash) == 1 {
			node_ids = append(node_ids, iterator.Value().(string))
			for i := 0; i < 2; i++ {
				if (iterator.Next()) {
					node_ids = append(node_ids, iterator.Value().(string))
				} else {
					iterator.First()
					node_ids = append(node_ids, iterator.Value().(string))
				}
			}
			break
		}
	} 
	if len(node_ids) == 0 {
		iterator.First()
		for i := 0; i < 3; i++ {
			node_ids = append(node_ids, iterator.Value().(string))
			iterator.Next()
		}
	}


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
			fmt.Println(replica_num)
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
	}
}
