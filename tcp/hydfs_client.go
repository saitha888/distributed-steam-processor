package tcp 

import (
	"distributed_system/udp"
	"github.com/emirpasic/gods/utils"
	"os"
	"fmt"
	"net"
)

func CreateFile(localfilename string, HyDFSfilename string) {
	// get the current ring map
	ring_map := udp.GetRing()

	// find which machine to create the file on
	file_hash := udp.GetHash(HyDFSfilename)

	// Find the smallest key that is greater than or equal to the hash
	node_id := ""
	iterator := ring_map.Iterator()
	for iterator.Next() {
		if utils.StringComparator(iterator.Key(), file_hash) == 1 {
			node_id = iterator.Value().(string)
		}
	} 
	if node_id == "" {
		iterator.First()
		node_id = iterator.Value().(string)
	}


	// get the contents of the local filename
	file_contents, err := os.ReadFile(localfilename)
	if err != nil {  // local filename is invalid
		fmt.Println("File doesn't exist locally:", err)
		return
	}

	content := string(file_contents)

	// connect to the machine 
	node_port := node_id[:36]

	bytes := []byte(node_port)
	
	// Change the second character (index 1) from 'e' to 'a'
	bytes[32] = '8'
	
	// Convert back to string
	node_port = string(bytes)

    conn, err := net.Dial("tcp", node_port)
    if err != nil {
        fmt.Println(err)
        return
    }
    defer conn.Close()

    // send the file message to the machine
	message := "create " + HyDFSfilename + " " + content
    conn.Write([]byte(message))
}