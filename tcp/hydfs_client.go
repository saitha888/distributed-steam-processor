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
    "io/ioutil"
    "sync"
    "encoding/json"
)

var cache_set = make(map[string]bool)

func GetFile(hydfs_file string, local_file string) {

	file_hash := udp.GetHash(hydfs_file)
	node_ids := udp.GetFileServers(file_hash)

	machine_num, _ := strconv.Atoi(machine_number)
	replica_num := machine_num % 3

	file_server := node_ids[replica_num][:36]

	server_num := node_ids[0][13:15]
	dir := "./cache"

	_, exists := cache_set[hydfs_file]
	
	if exists {
		content, _ := ioutil.ReadFile(dir + "/" + hydfs_file)
		err = WriteToFile(local_file, string(content))
		return
	}

	conn, err := net.Dial("tcp", file_server)
    if err != nil {
        fmt.Println(err)
        return
    }
    defer conn.Close() 

    data := Message{
		Action:    "get",
		Filename:  server_num + hydfs_file,
		FileContents: "",
	}

	// Encode the structure into JSON
	encoder := json.NewEncoder(conn)
	err = encoder.Encode(data)
	if err != nil {
		panic(err)
	}

    localfile, err := os.Create(local_file) 
	if err != nil {
		panic(err)
	}
	defer localfile.Close()

	_, err = io.Copy(localfile, conn)
	if err != nil {
		panic(err)
	}

	// add to cache
    localfile_cache, err := os.Create("./cache/" + hydfs_file) 
    _, err = io.Copy(localfile_cache, conn)
	if err != nil {
		panic(err)
	}
	cache_set[hydfs_file] = true
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
        data := Message{
            Action:    "create",
            Filename:  replica_num + HyDFSfilename,
            FileContents: content,
        }
        encoder := json.NewEncoder(conn)
        err = encoder.Encode(data)
        if err != nil {
            panic(err)
        }
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
    conn, err := net.Dial("tcp", port)
    if err != nil {
        fmt.Println(err)
        return
    }
    message := "append" + " " + hydfs_file + " " + replica_num + " " + content
    conn.Write([]byte(message))
        
    buf := make([]byte, 1000000)
    n, err := conn.Read(buf)
    if err != nil {
        fmt.Println(err)
        return
    }
    response := string(buf[:n])
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
        } else {
            response := string(buf[:n])
            tot_response += response
        }

    }
    files_dict := treemap.NewWith(func(a, b interface{}) int {
        layout := "15:04:05.000"
        timeA, _ := time.Parse(layout, a.(string))
        timeB, _ := time.Parse(layout, b.(string))
        return utils.TimeComparator(timeA, timeB)
    })

    chunks := strings.Split(tot_response, "---BREAK---")
    chunks = chunks[:len(chunks)-1]
    chunks_set := make(map[string]bool)
	for _,chunk := range chunks {
		filename := strings.Split(chunk, " ")[0]
		content := chunk[len(filename):]
		timestamp := filename[len(filename)-12:]
		_, exists := chunks_set[timestamp]
		if exists {
			val, _ := files_dict.Get(timestamp)
			res, _ := val.(string)
			files_dict.Put(timestamp,  res + "\n" + content)
		} else {
			files_dict.Put(timestamp,content)
			chunks_set[timestamp] = true
		}
	}
    iterator := files_dict.Iterator()
    iterator.First()
    merged_content := iterator.Value().(string)
    for iterator.Next() {
        merged_content += iterator.Value().(string)
    }
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
        buf := make([]byte, 1000000)
        _, er2r := conn.Read(buf)
        if er2r != nil {
            fmt.Println(err)
            return
        }
    }
}

func MultiAppend(hydfs_file string, vms []string, local_files []string) {
    if len(vms) != len(local_files) {
        fmt.Println("Must have equal number of vms and filenames")
        return
    }
    var wg sync.WaitGroup
    for i := range vms {
        wg.Add(1)
        go func(vm, localFile string) {
            defer wg.Done()

            port := vm[:36]
            conn, err := net.Dial("tcp", port)
            if err != nil {
                fmt.Println("Connection error:", err)
                return
            }
            defer conn.Close()

            message := "append-req " + localFile + " " + hydfs_file
            _, writeErr := conn.Write([]byte(message))
            if writeErr != nil {
                fmt.Println("Write error:", writeErr)
                return
            }

            buf := make([]byte, 1000000)
            _, readErr := conn.Read(buf)
            if readErr != nil {
                fmt.Println("Read error:", readErr)
                return
            }
        }(vms[i], local_files[i]) // Pass i-th VM and local file as arguments to avoid closure issues
    }

    wg.Wait()
}