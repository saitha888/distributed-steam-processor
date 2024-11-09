package tcp 

import (
	"distributed_system/udp"
	"fmt"
    "net"
    "os"
	"strconv"
	"time"
	"io"
	"strings"
	"github.com/emirpasic/gods/maps/treemap"
    "github.com/emirpasic/gods/utils"
)

func GetFile(hydfs_file string, local_file string) {
	file_hash := udp.GetHash(hydfs_file)
	node_ids := udp.GetFileServers(file_hash)

	machine_num, _ := strconv.Atoi(machine_number)
	replica_num := machine_num % 3

	file_server := node_ids[replica_num][:36]

	server_num := node_ids[0][13:15]

	conn, err := net.Dial("tcp", file_server)
    if err != nil {
        fmt.Println(err)
        return
    }
    defer conn.Close() 

    message := fmt.Sprintf("get %s-%s", server_num, hydfs_file)

    conn.Write([]byte(message))

	buf := make([]byte, 1000000)
    n, _ := conn.Read(buf)
    response := string(buf[:n])

	err = WriteToFile(local_file, response)
	log := "Got file " + hydfs_file + " from " + file_server + " and saved in " + local_file + " at " + time.Now().Format("15:04:05.000")
	fmt.Println(log)
	udp.AppendToFile(log, os.Getenv("HDYFS_FILENAME"))
	if err != nil {
		return
	}
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
		
		buf := make([]byte, 1000000)
		n, err := conn.Read(buf)
		if err != nil {
			fmt.Println(err)
			return
		}
		response := string(buf[:n])
		fmt.Println(response)
		udp.AppendToFile(response, os.Getenv("HDYFS_FILENAME"))
	}
}

func AppendFile(local_file string, hydfs_file string) {

	replicas := udp.GetFileServers(udp.GetHash(hydfs_file))
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
		
	buf := make([]byte, 1000000)
	n, err := conn.Read(buf)
	if err != nil {
		fmt.Println(err)
		return
	}
	response := string(buf[:n])
	fmt.Println(response)
	udp.AppendToFile(response, os.Getenv("HDYFS_FILENAME"))
}

func GetFromReplica(VMaddress string, HyDFSfilename string, localfilename string){
    file_hash := udp.GetHash(HyDFSfilename)
	node_ids := udp.GetFileServers(file_hash)

	server_num := node_ids[0][13:15]

	conn, err := net.Dial("tcp", VMaddress)
    if err != nil {
        fmt.Println(err)
        return
    }
    defer conn.Close() 

    message := fmt.Sprintf("get %s-%s", server_num, HyDFSfilename)

    conn.Write([]byte(message))

    // write the command to an output file
	buf := make([]byte, 1000000) // Buffer to hold chunks of data
	var response string        // Variable to hold the full response

	for {
		n, err := conn.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Println("Error reading from connection:", err)
			return
		}
		response += string(buf[:n])
	}
	err = WriteToFile(localfilename, response)
	if err != nil {
		return
	}
}	


//get every chunk of file from each replica "chunks"
//order chunks to create one merged file
//send merged file to each replica "merge"

func Merge(hydfs_file string) {
	replicas := udp.GetFileServers(udp.GetHash(hydfs_file))
	tot_response := ""
	for _, replica := range replicas {
		port := replica[:36]
		conn, err := net.Dial("tcp", port)
		if err != nil {
			fmt.Println(err)
			return
		}

		//request chunks of file from replica
		message := "chunks" + " " + hydfs_file
		conn.Write([]byte(message))
			
		buf := make([]byte, 1000000)
		n, err := conn.Read(buf)
		if err != nil {
			fmt.Println(err)
			return
		}
		response := string(buf[:n])
		tot_response += response
	}
	files_dict := treemap.NewWith(func(a, b interface{}) int {
        layout := "15:04:05.000"
        timeA, _ := time.Parse(layout, a.(string))
        timeB, _ := time.Parse(layout, b.(string))
        return utils.TimeComparator(timeA, timeB)
    })

	fmt.Println(tot_response)
	chunks := strings.Split(tot_response, "---BREAK---")
	chunks = chunks[:len(chunks)-1]
	for _,chunk := range chunks {
		filename := strings.Split(chunk, " ")[0]
		content := chunk[len(filename):]
		timestamp := filename[len(filename)-12:]
		fmt.Println(timestamp)
		files_dict.Put(timestamp,content)
	}
	iterator := files_dict.Iterator()
	iterator.First()
	merged_content := iterator.Value().(string)
	for iterator.Next() {
		merged_content += iterator.Value().(string)
	}
	fmt.Println(merged_content)
	for _, replica := range replicas {
		port := replica[:36]
		conn, err := net.Dial("tcp", port)
		if err != nil {
			fmt.Println(err)
			return
		}

		//request chunks of file from replica
		message := "merge" + " " + hydfs_file + " " + merged_content

		conn.Write([]byte(message))
		fmt.Println("merge req sent")
		buf := make([]byte, 1000000)
		n, err := conn.Read(buf)
		if err != nil {
			fmt.Println(err)
			return
		}
		response := string(buf[:n])
		fmt.Println(response)
	}
}

func MultiAppend(hydfs_file string, vms []string, local_files []string) {
	if len(vms) != len(local_files) {
		fmt.Println("Must have equal number of vms and filenames")
		return
	}
	for i:= range vms {
		go func(vm, localFile string) {
			port := vm[:36]
			conn, err := net.Dial("tcp", port)
			if err != nil {
				fmt.Println("Connection error:", err)
				return
			}
			defer conn.Close()

			message := "append " + localFile + " " + hydfs_file
			_, writeErr := conn.Write([]byte(message))
			if writeErr != nil {
				fmt.Println("Write error:", writeErr)
				return
			}

			buf := make([]byte, 1000000)
			n, readErr := conn.Read(buf)
			if readErr != nil {
				fmt.Println("Read error:", readErr)
				return
			}
			response := string(buf[:n])
			fmt.Println("Response from", vm, ":", response)
		}(vms[i], local_files[i]) // Pass i-th VM and local file as arguments to avoid closure issues
	}
}