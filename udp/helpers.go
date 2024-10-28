package udp

import (
    "fmt"
    "net"
    "os"
    "strings"
    "time"
    "strconv"
    "math/rand"
    "crypto/sha256"
    "github.com/emirpasic/gods/maps/treemap"
    "log"
    "io/ioutil"
    "regexp"
    "bufio"
    "encoding/binary"
)

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

// Function to send a failure message
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

//Function to leave the system
func LeaveList() {
    // Change own status to left, inform other machines to change status to left
    for i,node :=  range membership_list {
        if node.NodeID == node_id { // check if at self
            changeStatus(i, "leave")
        } else { 
            node_address := node.NodeID[:36]
            conn, err := DialUDPClient(node_address)
            defer conn.Close()

            // Send leave message
            message := fmt.Sprintf("leave " + node_id)
            _, err = conn.Write([]byte(message))
            if err != nil {
                fmt.Println("Error sending leave message:", err)
                return
            }
        }
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

func GetSelfID() string {
    return node_id
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
                appendToFile(message, logfile)
                return
            }
		}
	}
}

func checkStatus(node string) string {
    index := FindNode(node)
    if index >= 0 {
        return membership_list[index].Status
    }
    return "none"
}

// Function to connect to another machine
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

// Turn the membership list global variable into a string
func MembershiplistToString() string{
    nodes := make([]string, 0)
    for _,node := range membership_list {
        current_node := node.NodeID + " " + node.Status + " " + strconv.Itoa(node.Inc)
        nodes = append(nodes, current_node)
    }
    result := strings.Join(nodes, ", ")
    return result
}

// Remove a machine from the membership list
func RemoveNode(id_to_remove string) {
    for index,node := range membership_list {
        if id_to_remove == node.NodeID { // remove the node if it's found
            membership_list = append(membership_list[:index], membership_list[index+1:]...)
        }
    }
    ring_map := GetRing()
    // node_key := GetKeyByValue(ring_map, id_to_remove)
    iterator := IteratorAt(ring_map, id_to_remove)
    id := ""
    if (!iterator.Next()) {
        iterator.First()
        id = iterator.Value().(string)
    } else {
        id = iterator.Value().(string)
    }
    if (id == node_id) {
        fmt.Println("immediate predecessor failed")
        //if removed node is right before this node
        //this node becomes new origin for failed node, rename files
        RenameFilesWithPrefix(id_to_remove[13:15], node_id[13:15])

        //pull files of origin n-3
        nod := IteratorAtNMinusSteps(ring_map, node_id, 3)
        port := nod[:36]
        // pull for files
        conn_pred, err := net.Dial("tcp", port )
        if err != nil {
            fmt.Println(err)
            return
        }
        defer conn_pred.Close()
        message := fmt.Sprintf("pull")
        conn_pred.Write([]byte(message))
        reader := bufio.NewReader(conn_pred)
        buffer := ""

        for {
            // Read up to the next newline in chunks
            part, err := reader.ReadString('\n')
            if err != nil {
                fmt.Println("Error reading from server:", err)
                break
            }

            // Append the read part to the buffer
            buffer += part

            // Check if buffer contains the custom delimiter
            if strings.Contains(buffer, "\n---END_OF_MESSAGE---\n") {
                // Split buffer by the custom delimiter
                parts := strings.Split(buffer, "\n---END_OF_MESSAGE---\n")

                // Process all complete messages in parts
                for i := 0; i < len(parts)-1; i++ {
                    if strings.TrimSpace(parts[i]) != "" { // Ignore empty messages
                        fmt.Println("Received message:", parts[i])
                        filename := strings.Split(parts[i], " ")[0]
                        argument_length := 1 + len(filename)
                        contents := parts[i][argument_length:]
                        new_filename := "./file-store/" + filename
        
                        file, err := os.OpenFile(new_filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
                        if err != nil {
                            fmt.Println(err)
                        }
                        defer file.Close()
        
                        _, err = file.WriteString(contents)
                        if err != nil {
                            fmt.Println(err)
                        }
                    }
                }
                // Retain the last part in the buffer (incomplete message)
                buffer = parts[len(parts)-1]
            }
        }
    }
    id2 := ""
    if (!iterator.Next()) {
        iterator.First()
        id2 = iterator.Value().(string)
    } else {
        id2 = iterator.Value().(string)
    }
    if (id2 == node_id) {
        //if removed node is 2 nodes before this node
        //rename files of origin n-2 to n-1 
        RenameFilesWithPrefix(IteratorAtNMinusSteps(ring_map, node_id, 2)[13:15], node_id[13:15])

        //pull files of origin n-3
        nod := IteratorAtNMinusSteps(ring_map, node_id, 3)
        port := nod[:36]
        // pull for files
        conn_pred, err := net.Dial("tcp", port )
        if err != nil {
            fmt.Println(err)
            return
        }
        defer conn_pred.Close()
        message := fmt.Sprintf("pull")
        conn_pred.Write([]byte(message))
        reader := bufio.NewReader(conn_pred)
        buffer := ""

        for {
            // Read up to the next newline in chunks
            part, err := reader.ReadString('\n')
            if err != nil {
                fmt.Println("Error reading from server:", err)
                break
            }

            // Append the read part to the buffer
            buffer += part

            // Check if buffer contains the custom delimiter
            if strings.Contains(buffer, "\n---END_OF_MESSAGE---\n") {
                // Split buffer by the custom delimiter
                parts := strings.Split(buffer, "\n---END_OF_MESSAGE---\n")

                // Process all complete messages in parts
                for i := 0; i < len(parts)-1; i++ {
                    if strings.TrimSpace(parts[i]) != "" { // Ignore empty messages
                        fmt.Println("Received message:", parts[i])
                        filename := strings.Split(parts[i], " ")[0]
                        argument_length := 1 + len(filename)
                        contents := parts[i][argument_length:]
                        new_filename := "./file-store/" + filename
        
                        file, err := os.OpenFile(new_filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
                        if err != nil {
                            fmt.Println(err)
                        }
                        defer file.Close()
        
                        _, err = file.WriteString(contents)
                        if err != nil {
                            fmt.Println(err)
                        }
                    }
                }
                // Retain the last part in the buffer (incomplete message)
                buffer = parts[len(parts)-1]
            }
        }
    }
    
    bytes := []byte(id_to_remove)
	
	bytes[32] = '8'
	
	id_to_remove = string(bytes)


    ring_map.Remove(GetHash(id_to_remove))

    // say node that fails is n
    // files with n origin, n-1 origin, n-2 origin
    // give files of origin n to nodes n+3
    // give files of origin n-1 to n+2
    // give files of origin n-2 to n+1
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

// // Function to find the key by its value in a TreeMap
// func GetKeyByValue(ringMap *treemap.Map, value string) interface{} {
// 	iterator := ringMap.Iterator()
// 	for iterator.Next() {
// 		if iterator.Value() == value {
// 			return iterator.Key() // Return the corresponding key
// 		}
// 	}
//     iterator.First()
// 	return iterator.Key()
// }

// Function to find the iterator positioned at the nth key and move backwards by steps
func IteratorAtNMinusSteps(ringMap *treemap.Map, start_val string, steps int) string {
	// Get an iterator at the beginning of the TreeMap
	iterator := ringMap.Iterator()
	found := false

	// First, find the position of startKey (n)
	for iterator.Next() {
		if iterator.Value().(string) == start_val {
			found = true
			break
		}
	}

	// If startKey is found, move backwards by the specified steps
	if found {
		// Move backwards by `steps`
		for i := 0; i < steps; i++ {
			if iterator.Prev() {
				// Move backwards
			} else {
				// If there's no previous element (we hit the beginning of the map), return nil
				return ""
			}
		}
		return iterator.Value().(string)
	}
	return ""
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

//function to add node
func AddNode(node_id string, node_inc int, status string, i string){
    new_node := Node{
        NodeID:    node_id,  
        Status:    status,           
        Inc: node_inc,
        Index: i,
    }
    membership_list = append(membership_list, new_node)
    bytes := []byte(node_id)
	bytes[32] = '8'
	
	node_id = string(bytes)

    ring_map.Put(GetHash(node_id), node_id)
}


// Get the index of a machine in the list
func FindNode(node_id string) int {
    for index,node := range membership_list { 
        if node_id == node.NodeID {
            return index
        }
    }
    return -1
}

func GetHash(data string) int {
	hash := sha256.Sum256([]byte(data))
    truncated_hash := binary.BigEndian.Uint64(hash[:8])
    ring_hash := truncated_hash % 2048
	return (int)(ring_hash)
}

// Change the status of a machine in the list
func changeStatus(index int, message string){
    membership_list[index].Status = message
}

// Change the status of a machine in the list
func changeInc(index int, message int){
    membership_list[index].Inc = message
}


// Function to append a string to a file
func appendToFile(content string, filename string) error {
	// Open the file or create it 
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
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


func ListMem(list_to_print []Node) {
    if len(list_to_print) == 0 {
        fmt.Println("List is empty.")
        return
    }

    nodeIDWidth := 54
    statusWidth := 4

    fmt.Printf("%-*s | %-*s | %s\n", nodeIDWidth, "NodeID", statusWidth, "Status", "Incarnation #")
    fmt.Println(strings.Repeat("-", nodeIDWidth+statusWidth+25))

    // Go through membership list and print each entry
    for _, node := range list_to_print {
        fmt.Printf("%s | %s  | %s\n",node.NodeID, node.Status, strconv.Itoa(node.Inc))
    }
    fmt.Println()
    fmt.Print("> ")
}

func GetRing() *treemap.Map {
    return ring_map
}

func ListRing(treeMap *treemap.Map) {
    keys := treeMap.Keys()
    for _, hash := range keys {
        id, _ := treeMap.Get(hash)  // Get the value associated with the key
		fmt.Printf("Hash: %d, Node: %s\n", hash, id)
    }
}

func PrintFiles(dirName string) {
	if _, err := os.Stat(dirName); os.IsNotExist(err) {
		log.Fatalf("Directory %s does not exist\n", dirName)
	}

	files, err := ioutil.ReadDir(dirName)
	if err != nil {
		log.Fatalf("Failed to read directory: %v", err)
	}

	for _, file := range files {
		fmt.Println(file.Name())
	}
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

func GetMembershipList() []Node {
	return membership_list
}

func FindNodeWithPort(port string) int {
    for index,node := range(membership_list) {
        if port == node.NodeID[:36] {
            return index
        }
    }
    return -1
}


func IntroducerJoin() {
    // create unique node_id and add to list
    membership_list = nil
    inc_num += 1
    if node_id == ""{
        node_id  = os.Getenv("MACHINE_ADDRESS") + "_" + time.Now().Format("2006-01-02_15:04:05")
        AddNode(node_id, 1, "alive", machine_number)
    } 

    // go through ports, get first alive membership list
    for i,port := range ports {
        machine, _ := strconv.Atoi(machine_number)
        if i == machine - 1 {
            continue
        }
        // connect to the port

        conn, _ := DialUDPClient(port)
        defer conn.Close()

        // request membership list
        message := fmt.Sprintf("mem_list") 
        _, err = conn.Write([]byte(message))
        if err != nil {
            fmt.Println("Error getting result from :" + port, err)
            continue
        }
        // Read the response from the port
        buf := make([]byte, 1024)

        conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))

        n, _, err := conn.ReadFromUDP(buf)
        if err != nil {
            fmt.Println("Error reading from :" + port, err)
            continue
        }
        memb_list_string := string(buf[:n])
        memb_list := strings.Split(memb_list_string,", ")

        // if none keep membership list empty
        if memb_list_string == "" {
            continue
        } else { // else set membership list to recieved membership list and break
            for _,node :=  range memb_list {
                node_vars := strings.Split(node, " ")
                inc, _ := strconv.Atoi(node_vars[2])
                fmt.Println("adding node again: " + node_vars[0])
                AddNode(node_vars[0], inc, node_vars[1], node_vars[2])
            }
            // change own node status to alive
            index := FindNode(node_id)
            if index >= 0 {
                changeStatus(index, "alive")
            }        
            break
        }
    }

    // send to all other machines it joined 
    for _,node := range membership_list {
        if node.Status == "alive" {
            node_address := node.NodeID[:36]
            if node_address != os.Getenv("MACHINE_ADDRESS") { // check that it's not self
                // connect to node
                addr, err := net.ResolveUDPAddr("udp", node_address)
                if err != nil {
                    fmt.Println("Error resolving target address:", err)
                }

                // Dial UDP to the target node
                conn, err := net.DialUDP("udp", nil, addr)
                if err != nil {
                    fmt.Println("Error connecting to target node:", err)
                }
                defer conn.Close()

                result := "join " + node_id
                // send join message
                conn.Write([]byte(result))
            }
        }
    }
}


func ProcessJoin(address string) {
    // increment the incarnation number
    inc_num += 1
    // Connect to introducer
    conn, err := DialUDPClient(introducer_address)
    defer conn.Close()

    // Initialize node id for machine.
    if node_id == "" {
        node_id = address + "_" + time.Now().Format("2006-01-02_15:04:05")
    }


    // Send join message to introducer
    message := fmt.Sprintf("join %s", node_id)
    _, err = conn.Write([]byte(message))
    if err != nil {
        fmt.Println("Error sending message to introducer:", err)
        return
    }
    buf := make([]byte, 1024)

    // Read the response from the introducer (membership list to copy)
    n, _, err := conn.ReadFromUDP(buf)
    if err != nil {
        fmt.Println("Error reading from introducer:", err)
        return
    }
    memb_list_string := string(buf[:n])
    memb_list := strings.Split(memb_list_string,", ")

    // Clear existing membership list if dealing with a node that left
    membership_list = nil

    // Update machine's membership list
    for _,node :=  range memb_list {
        node_vars := strings.Split(node, " ")
        inc, _ := strconv.Atoi(node_vars[2])
        index := node_vars[0][13:15]
        AddNode(node_vars[0], inc, node_vars[1], index)
    }

    // Print the response from the introducer (e.g., acknowledgment or membership list)
    fmt.Printf("Received mem_list from introducer\n")

}


func ProcessJoinMessage(message string) {
    joined_node := message[5:]
    index := FindNode(joined_node)
    if index >= 0 { // machine was found
        changeStatus(index, "alive")
    } else { // machine was not found
        i := joined_node[13:15]
        AddNode(joined_node, 1, "alive", i)
    }
    send := "Node join detected for: " + joined_node + " at " + time.Now().Format("15:04:05") + "\n"
    appendToFile(send, logfile)
}