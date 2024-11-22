package util

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
    "encoding/json"
    "io"
    "distributed_system/global"
    "github.com/emirpasic/gods/maps/treemap"
    "crypto/sha256"
    "encoding/binary"
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
    bytes := []byte(global.Node_id)
	bytes[32] = '8'
	
	tcp_node_id := string(bytes)
    return tcp_node_id
}

// Function to randomly select an alive node in the system
func SelectRandomNode() global.Node {
    rand.Seed(time.Now().UnixNano())
    var target_node global.Node
    for {
        random_index := rand.Intn(len(global.Membership_list))
        selected_node := global.Membership_list[random_index]
        if selected_node.NodeID != global.Node_id && selected_node.Status != "leave" { 
            target_node = selected_node
            break
        }
    }
    return target_node
}

// Get the index of a machine in the list
func FindNode(node_id string) int {
    for index,node := range global.Membership_list { 
        if node_id == node.NodeID {
            return index
        }
    }
    return -1
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

func FindSusMachines() []global.Node {
	var susList []global.Node
	for _, node := range global.Membership_list {
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
	iterator := global.Ring_map.Iterator()
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
    fmt.Println("STORE FOR: " + global.Node_id[:31] + " - " + "HASH: " + strconv.Itoa(GetHash(global.Ring_id)))
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
    delete(global.Cache_set, filename)
}

func GetFiles(conn net.Conn, data global.Message) {
    encoder := json.NewEncoder(conn)
    err := encoder.Encode(data)
    if err != nil {
        fmt.Println("Error encoding data in pull-files in self join", err)
    } 

    decoder := json.NewDecoder(conn)
    for {
        var response global.Message
        err := decoder.Decode(&response)
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

// Get the deterministic hash of a string
func GetHash(data string) int {
	hash := sha256.Sum256([]byte(data))
    truncated_hash := binary.BigEndian.Uint64(hash[:8])
    ring_hash := truncated_hash % 2048
	return (int)(ring_hash)
}

// Change the status of a machine in the list
func ChangeStatus(index int, message string){
    global.Membership_list[index].Status = message
}

func CheckStatus(node string) string {
    index := FindNode(node)
    if index >= 0 {
        return global.Membership_list[index].Status
    }
    return "none"
}

// Get the 3 nodes before a node in the ring
func GetPredecessors(self_id string) [3]string{
    var prev1, prev2, prev3 string

	// Create an iterator to go through the ring map
	it := global.Ring_map.Iterator()

	for it.Next() { // get the three predecessors
		if it.Value().(string) == self_id {
			break
		}
		prev3 = prev2
		prev2 = prev1
		prev1 = it.Value().(string)
	}

	if prev1 == "" { // if the first predecessor wasn't set (current node is at the start of map)
		_, v1 := global.Ring_map.Max()
		prev1 = v1.(string)
	}
	if prev2 == "" { // if the second predecessor wasn't set
		_, max_value := global.Ring_map.Max()
		if prev1 == max_value.(string) { // if the first predecessor is already the last map value
			it = global.Ring_map.Iterator()
			for it.Next() {
				if it.Value().(string) == prev1 { // find the value before the first predecessor
					break
				}
				prev2 = it.Value().(string)
			}
		} else { // set to last value in map
			prev2 = max_value.(string)
		}
	}
	if prev3 == "" { // if the third predecessor wasn't set
		_, max_value := global.Ring_map.Max()
		if prev1 == max_value.(string) || prev2 == max_value.(string) { // if the first or second predecessor is already the last map value
			it = global.Ring_map.Iterator()
			for it.Next() {
				if it.Value().(string) == prev2 { // find the value before the second predecessor
					break
				}
				prev3 = it.Value().(string)
			}
		} else { // set to last value in map
			prev3 = max_value.(string)
		}
	}

	predecessors := [3]string{prev1, prev2, prev3}
    return predecessors
}

// Get the 1 node after a node in the ring
func GetSuccessor(ring_id string) string{
    successor := ""

    //iterate through the ring map
    it := global.Ring_map.Iterator()
    for it.Next() {
		if it.Value().(string) == ring_id { // if we found the current value
            if it.Next() { // set the succesor to the next value if it's valid
				successor = it.Value().(string)
			} else { // set to first value in map (wrap around)
				_, successor_val := global.Ring_map.Min()
                successor = successor_val.(string)
			}
		}
	}
    return successor
}

// iteratorAt finds the iterator positioned at the given key
func IteratorAt(ringMap *treemap.Map, start_val string) *treemap.Iterator {
	iterator := ringMap.Iterator()
	for iterator.Next() {
		if iterator.Value().(string) == start_val {
			// Return the iterator at the position of startKey
			return &iterator
		}
	}
	// Return nil if the key is not found
    iterator.First()
	return &iterator
}

// IteratorAtNMinusSteps moves backward by `steps` from the position of `start_val`, wrapping around if necessary.
func IteratorAtNMinusSteps(ringMap *treemap.Map, start_val string, steps int) string {
	iterator := ringMap.Iterator()
	found := false

	// Locate the starting position of `start_val`
	for iterator.Next() {
		if iterator.Value().(string) == start_val {
			found = true
			break
		}
	}

	// If `start_val` is not found, return an empty string
	if !found {
		return ""
	}

	// Move backward by `steps`, wrapping around as necessary
	for i := 0; i < steps; i++ {
		// Attempt to move backward
		if !iterator.Prev() {
			// If at the beginning, wrap around to the last element
            iterator.First()
            temp := iterator
			for iterator.Next() {
                temp = iterator
            } // Move to the last element
            iterator = temp
		}
        fmt.Printf("%s is at n-%d\n", iterator.Value().(string), i+1)
	}

	// Return the value at the final position
	return iterator.Value().(string)
}

// Turn the membership list global variable into a string
func MembershiplistToString() string{
    nodes := make([]string, 0)
    for _,node := range global.Membership_list {
        current_node := node.NodeID + " " + node.Status + " " + strconv.Itoa(node.Inc)
        nodes = append(nodes, current_node)
    }
    result := strings.Join(nodes, ", ")
    return result
}

// Change the status of a machine in the list
func ChangeInc(index int, message int){
    global.Membership_list[index].Inc = message
}
