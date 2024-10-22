package tcp 

import (
	"distributed_system/udp"
	"github.com/emirpasic/gods/utils"
	"fmt"
    "net"
    "os"
    // "os/exec"
    // "strings"
    // "io"
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

