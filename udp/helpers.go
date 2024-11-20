package udp

import (
    "fmt"
	"net"
    "os"
    "strings"
    "time"
    "strconv"
    "math/rand"
    "log"
    "io/ioutil"
    "regexp"
	"github.com/emirpasic/gods/maps/treemap"
    "encoding/json"
    "io"
)

func ConnectToMachine(port string) (*net.UDPConn, error){
    addr, err := net.ResolveUDPAddr("udp", ":" + port)
    if err != nil {
        fmt.Println("Error resolving address:", err)
    }

    conn, err := net.ListenUDP("udp", addr)
    if err != nil {
        fmt.Println("Error starting UDP server:", err)
    }
    return conn, nil
}

func GetTcpID() string {
    bytes := []byte(node_id)
	bytes[32] = '8'
	
	node_id = string(bytes)
    return node_id
}

// Function to randomly select an alive node in the system
func SelectRandomNode() Node {
    rand.Seed(time.Now().UnixNano())
    var target_node Node
    for {
        random_index := rand.Intn(len(membership_list))
        selected_node := membership_list[random_index]
        if selected_node.NodeID != node_id && selected_node.Status != "leave" { 
            target_node = selected_node
            break
        }
    }
    return target_node
}

// Run the function for exactly 4 seconds
func susTimeout(duration time.Duration, sus_id string, inc_num int) {
	// Create a channel that will send a signal after the specified duration
	timeout := time.After(duration)
	// Run the work in a loop
	for {
		select {
		case <-timeout:
            RemoveNode(sus_id)
            return
		default:
			// Continue doing the work
            index := FindNode(sus_id)
            if index >= 0 && membership_list[index].Inc > inc_num {
                message := "Node suspect removed for: " + sus_id + "\n"
                AppendToFile(message, logfile)
                return
            }
		}
	}
}

// Function to write content to a local file
func WriteToFile(filename string, content string) error {
	// Create or truncate the file
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// Write the content to the file
	_, err = file.WriteString(content)
	if err != nil {
		return err
	}

	return nil
}

// renameFilesWithPrefix renames files in the "filestore" directory that start with oldPrefix to start with newPrefix
func RenameFilesWithPrefix(oldPrefix string, newPrefix string) {
	dir := "file-store"

	// Read the directory contents
	files, err := ioutil.ReadDir(dir)
	if err != nil {
        fmt.Println("cannot get to directory")
	}

	// Regular expression to match filenames starting with the oldPrefix followed by a dash
	re := regexp.MustCompile(fmt.Sprintf(`^(%s)-(.*)`, oldPrefix))

	// Iterate through all the files
	for _, file := range files {
		// Get the file name
		oldName := file.Name()

		// Use regex to check if the filename starts with oldPrefix and a dash
		matches := re.FindStringSubmatch(oldName)
		if matches == nil {
			// If there's no match, skip the file
			continue
		}

		// Create the new filename with newPrefix instead of oldPrefix
		newName := fmt.Sprintf("%s-%s", newPrefix, matches[2])

		// Construct full paths for renaming
		oldPath := fmt.Sprintf("%s/%s", dir, oldName)
		newPath := fmt.Sprintf("%s/%s", dir, newName)

		// Rename the file
		err = os.Rename(oldPath, newPath)
		if err != nil {
			log.Printf("Error renaming file %s to %s: %v", oldPath, newPath, err)
		} else {
			fmt.Printf("Renamed %s to %s\n", oldName, newName)
		}
	}
}

// Function to append a string to a file
func AppendToFile(content string, filename string) error {
	// Open the file or create it 
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	// Write the content to the file
	_, err = file.WriteString(content + "\n")
	if err != nil {
		return err
	}

	return nil
}

func FindSusMachines() []Node {
	var susList []Node
	for _, node := range membership_list {
        if node.Status == " sus "{
			susList = append(susList, node)
		}
    }
	return susList
}

// get the tcp version of the node_id (value in the ring map)
func GetTCPVersion(id string) string {
    bytes := []byte(id)
	bytes[32] = '8'
	id = string(bytes)
    
    return id
}

func ListServers(HyDFSfilename string) {
    file_hash := GetHash(HyDFSfilename)
    fmt.Println("File ID: ", strconv.Itoa(file_hash))

    node_ids := GetFileServers(file_hash)
    for _,node := range node_ids {
        fmt.Println(node)
    }
}


func GetFileServers(file_hash int) []string {
    node_ids := []string{}
	iterator := ring_map.Iterator()
	for iterator.Next() {
		if iterator.Key().(int)> file_hash {
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

    return node_ids
}

func ListStore() map[string]bool {
    timestamp_pattern := `^[\d]{2}:[\d]{2}:[\d]{2}\.[\d]{3}$`
    fmt.Println("STORE FOR: " + node_id[:31] + " - " + "HASH: " + strconv.Itoa(GetHash(ring_id)))
    dir := "./file-store"

    files, err := ioutil.ReadDir(dir)
    if err != nil {
        fmt.Println("Error reading directory:", err)
    }

    file_set := make(map[string]bool)

    for _, file := range files {
        filename := file.Name()
        last_dash := strings.LastIndex(filename, "-")
        if last_dash != -1 {
            prefix := filename[:last_dash]
	        timestamp := filename[last_dash+1:]
            matched, _ := regexp.MatchString(timestamp_pattern, timestamp)
            if matched {
                filename = prefix[3:]
            }
        }
        if file_set[filename] {
            continue
        }
        file_set[filename] = true
        if !file.IsDir() {
            fmt.Println("File: " + filename + " \tFile Hash: " + strconv.Itoa(GetHash(filename)))
        }
    }
    return file_set
}

func GetFileContents(filename string) string {
    dir := "./file-store"

    files, err := ioutil.ReadDir(dir)
    if err != nil {
        fmt.Println("Error reading directory:", err)
    }

    var combined strings.Builder 

    for _, file := range files {
        if !file.IsDir() {
            curr_file := file.Name()
            if strings.HasPrefix(curr_file, filename) {
                content, err := ioutil.ReadFile(dir + "/" + curr_file)
                if err != nil {
                    fmt.Println("Error reading file:", curr_file, err)
                    continue
                }
                combined.WriteString(string(content))
            } 
        }
    }

    result := combined.String()
    return result
}

func GetNodeID() string {
    return node_id
}

func GetRing() *treemap.Map {
    return ring_map
}

func GetMembershipList() []Node {
	return membership_list
}

func GetFilePrefix() string {
    return file_prefix
}

// Sends a message with contents to_send to target_node
func SendMessage(target_node string, to_send string, node_to_send string) {
    target_addr := target_node[:36]
    conn, err := DialUDPClient(target_addr)
    defer conn.Close()

    message := to_send + " " + node_to_send
    _, err = conn.Write([]byte(message))
    if err != nil {
        fmt.Println("Error sending fail message:", err)
        return
    }
}

// Sends a message that to_clear node is alive to node_id
func SendAlive(node_id string, to_clear string, inc_num string) {

    target_addr := node_id[:36]
    conn, err := DialUDPClient(target_addr)
    defer conn.Close()

    message := "alive " + to_clear + " " + inc_num
    _, err = conn.Write([]byte(message))
    if err != nil {
        fmt.Println("Error sending alive message:", err)
        return
    }
}

// Function to resolve and dial a UDP connection to a given address
func DialUDPClient(target_addr string) (*net.UDPConn, error) {

    // Resolve the UDP address
    addr, err := net.ResolveUDPAddr("udp", target_addr)
    if err != nil {
        fmt.Println("Error resolving target address:", err)
        return nil, err
    }

    // Dial UDP to the target node
    conn, err := net.DialUDP("udp", nil, addr)
    if err != nil {
        fmt.Println("Error connecting to target node:", err)
        return nil, err
    }
    return conn, nil
}

func ListCache() map[string]bool {
    dir := "./cache"

    files, err := ioutil.ReadDir(dir)
    if err != nil {
        fmt.Println("Error reading directory:", err)
    }

    file_set := make(map[string]bool)

    for _, file := range files {
        filename := file.Name()
        file_set[filename] = true
    }
    return file_set
}

func RemoveFromCache(filename string) {
    dir := "./cache"

    files, err := ioutil.ReadDir(dir)
    if err != nil {
        fmt.Println("Error reading directory:", err)
    }

    for _, file := range files {
        curr_file := file.Name()
        if curr_file == filename {
            os.Remove(dir + "/" + filename)
        }
    }
}

func PullFiles(conn net.Conn) {
    data := Message{
        Action: "pull",
        Filename:  "",
        FileContents: "",
    }
    encoder := json.NewEncoder(conn)
    err = encoder.Encode(data)
    if err != nil {
        fmt.Println("Error encoding data in pull-files in self join", err)
    } 

    decoder := json.NewDecoder(conn)
    for {
        var response Message
        err = decoder.Decode(&response)
        if err != nil {
            if err == io.EOF {
                // End of the response from the server
                fmt.Println("All messages received.")
                break
            }
            fmt.Println("Error decoding message from server:", err)
            return
        }
        file, err := os.OpenFile("file-store/"+response.Filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
        if err != nil {
            fmt.Println(err)
        }
        defer file.Close()

        _, err = file.WriteString(response.FileContents)
        if err != nil {
            fmt.Println(err)
        }
    }
}