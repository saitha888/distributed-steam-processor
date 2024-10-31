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
    "bufio"
    "encoding/binary"
    "sort"
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
func RemoveNode(node_id string) {
    for index,node := range membership_list {
        if node_id == node.NodeID { // remove the node if it's found
            membership_list = append(membership_list[:index], membership_list[index+1:]...)
            ring_map.Remove(node.RingID)
            break
        }
    }
}

//function to add node
func AddNode(node_id string, node_inc int, status string, i string){
    ring_id := GetTCPVersion(node_id)
    ring_hash := GetHash(ring_id)

    new_node := Node{
        NodeID:    node_id,  
        Status:    status,           
        Inc: node_inc,
        Index: i,
        RingID: ring_hash,
    }
    membership_list = append(membership_list, new_node)

    ring_map.Put(ring_hash, ring_id)
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

    nodeIDWidth := 56
    ringIDWidth := 4
    statusWidth := 4

    sort.Slice(list_to_print, func(i, j int) bool {
        return list_to_print[i].RingID < list_to_print[j].RingID
    })

    fmt.Printf("%-*s | %-*s | %-*s | %s | \n", ringIDWidth, "RingID", nodeIDWidth, "NodeID", statusWidth, "Status", "Incarnation #")
    fmt.Println(strings.Repeat("-", nodeIDWidth+statusWidth+ringIDWidth+30))

    // Go through membership list and print each entry
    for _, node := range list_to_print {
        fmt.Printf("%-*s | %s | %s  | %s\n",8,strconv.Itoa(node.RingID),node.NodeID, node.Status, strconv.Itoa(node.Inc))
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
        node_id  = os.Getenv("MACHINE_UDP_ADDRESS") + "_" + time.Now().Format("2006-01-02_15:04:05")
        AddNode(node_id, 1, "alive", machine_number)
    } 

    // go through ports, get first alive membership list
    for _,port := range ports {
        if port[13:15] == machine_address[13:15] {
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
            if node_address != os.Getenv("MACHINE_UDP_ADDRESS") { // check that it's not self
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
    ring_id := GetTCPVersion(node_id)
    SelfJoin(ring_id)
}


func ProcessJoin(address string) {
    // increment the incarnation number
    inc_num += 1
    // Connect to introducer
    conn_introducer, err := DialUDPClient(introducer_address)
    defer conn_introducer.Close()

    // Initialize node id for machine.
    if node_id == "" {
        node_id = machine_address + "_" + time.Now().Format("2006-01-02_15:04:05")
    }
    ring_id := GetTCPVersion(node_id)

    // Send join message to introducer
    message := fmt.Sprintf("join %s", node_id)
    _, err = conn_introducer.Write([]byte(message))
    if err != nil {
        fmt.Println("Error sending message to introducer:", err)
        return
    }
    buf := make([]byte, 1024)

    // Read the response from the introducer (membership list to copy)
    n, _, err := conn_introducer.ReadFromUDP(buf)
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
    SelfJoin(ring_id)
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
    // check if a predecessor got added
    NewJoin(joined_node)
}

func SelfJoin(ring_id string) {
    // find successor and connect
    successor := GetSuccessor(ring_id)
    if len(successor) == 0 {
        return
    }
    successor_port := successor[:36]

    if successor_port != os.Getenv("MACHINE_TCP_ADDRESS") {
        conn_successor, err := net.Dial("tcp", successor_port)
        if err != nil {
            fmt.Println("Error connecting to server:", err)
        }
        defer conn_successor.Close()

        // Send a split message to the successor
        fmt.Fprintln(conn_successor, "split " + ring_id)

        reader := bufio.NewReader(conn_successor)
        buffer := ""

        for {
            // Keep reading till the next new line until we reach the delimiter
            part, err := reader.ReadString('\n')
            if err != nil {
                fmt.Println("Error reading from server:", err)
                break
            }

            // Append the read part to the buffer
            buffer += part

            // Check if buffer contains the  delimiter
            if strings.Contains(buffer, "\n---END_OF_MESSAGE---\n") {
                parts := strings.Split(buffer, "\n---END_OF_MESSAGE---\n")

                for i := 0; i < len(parts)-1; i++ {
                    if strings.TrimSpace(parts[i]) != "" {
                        filename := strings.Split(parts[i], " ")[0] // get the filename
                        argument_length := 1 + len(filename)
                        contents := parts[i][argument_length:] // get the contents
                        new_filename := "./file-store/" + machine_address[13:15] + "-" + filename // rename the file
                        file, err := os.OpenFile(new_filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644) // write to file
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
                buffer = parts[len(parts)-1] // reset the buffer
            }
        }
    }

    // find the predecessors
    predecessors := GetPredecessors(ring_id)
    // go through each predecessor
    for i,p :=  range predecessors {
        if i == 2 || len(p) == 0 { // if it's the third predecessor or empty continue
            continue
        }
        // connect to the predecesorr
        pred_port := p[:36]
        if pred_port != os.Getenv("MACHINE_TCP_ADDRESS"){
            conn_pred, err := net.Dial("tcp", pred_port)
            if err != nil {
                fmt.Println("Error connecting to server:", err)
            }
            defer conn_pred.Close()

            // Send a message to the server
            fmt.Fprintln(conn_pred, "pull")

            reader := bufio.NewReader(conn_pred)
            buffer := ""

            for {
                // Keep reading till the next new line until we reach the delimiter
                part, err := reader.ReadString('\n')
                if err != nil {
                    fmt.Println("Error reading from server:", err)
                    break
                }

                // Append the read part to the buffer
                buffer += part

                // Check if buffer contains the delimiter
                if strings.Contains(buffer, "\n---END_OF_MESSAGE---\n") {
                    parts := strings.Split(buffer, "\n---END_OF_MESSAGE---\n")

                    for i := 0; i < len(parts)-1; i++ {
                        if strings.TrimSpace(parts[i]) != "" { 
                            filename := strings.Split(parts[i], " ")[0] // get filename
                            argument_length := 1 + len(filename)
                            contents := parts[i][argument_length:] // get contents
                            new_filename := "./file-store/" + filename // add directory to file
            
                            file, err := os.OpenFile(new_filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644) // write to file
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
                    buffer = parts[len(parts)-1] // reset the buffer
                }
            }
        }
    }
}

func NewJoin(joined_node string) {
    // get ring ids for both nodes
    self_id := GetTCPVersion(node_id)
    joined_node = GetTCPVersion(joined_node)

    // get predecessors
    predecessors := GetPredecessors(self_id)

    dir := "./file-store" 

    // find all the prefixes for what files may need to be removed
    curr_prefix := os.Getenv("MACHINE_UDP_ADDRESS")[13:15]
    first_pred_prefix, second_pred_prefix, third_pred_prefix := "","",""
    if len(predecessors[0]) > 0 {
        first_pred_prefix = predecessors[0][13:15]
    }
    if len(predecessors[1]) > 0 {
        second_pred_prefix = predecessors[1][13:15]
    }
    if len(predecessors[2]) > 0 {
        third_pred_prefix = predecessors[2][13:15]
    }

    // get all the files in the directory
    files, err := ioutil.ReadDir(dir)
    if err != nil {
        log.Fatal(err)
    }
    
    for i,p :=  range predecessors {
        if p == joined_node && i == 0 { // if it's immediate predecessor
            pred_hash := GetHash(p) // get the hash of the predecessor
            for _, file := range files {
                filename := file.Name()
                file_hash := GetHash(filename[3:])
                // find files with prefix of current server
                if !file.IsDir() && strings.HasPrefix(filename, curr_prefix) {
                    // if the hash now routes to predecessor change the prefix
                    if pred_hash >= file_hash && file_hash < GetHash(self_id) {
                        old_filename := "file-store/" + filename
                        new_filename := "file-store/" + p[13:15] + "-" + filename[3:]
                        os.Rename(old_filename, new_filename)
                    }
                }
                // find files with prefix of third predecessor and remove them
                if !file.IsDir() && strings.HasPrefix(filename, third_pred_prefix) {
                    err := os.Remove(dir + "/" + filename)
                    if err != nil {
                        fmt.Println("Error removing file:", err)
                    }
                }
            }
        } else if p == joined_node && i == 1{ // if it's second predecessor
            second_pred_hash := GetHash(p)
            pred_hash := GetHash(predecessors[0])
            for _, file := range files {
                filename := file.Name()
                file_hash := GetHash(filename[3:])
                // find files with prefix of first predecessor
                if !file.IsDir() && strings.HasPrefix(filename, first_pred_prefix) {
                    // if the hash now routes to second predecessor change the prefix
                    if second_pred_hash >= file_hash && file_hash < pred_hash {
                        old_filename := "file-store/" + filename
                        new_filename := "file-store/" + second_pred_prefix + "-" + filename[3:]
                        os.Rename(old_filename, new_filename)
                    }
                }
                // find files with prefix of third predecessor and remove
                if !file.IsDir() && strings.HasPrefix(filename, third_pred_prefix) {
                    err := os.Remove(dir + "/" + filename)
                    if err != nil {
                        fmt.Println("Error removing file:", err)
                    }
                }
            }
        } else if p == joined_node && i == 2 { // if it's third predecessor
            third_pred_hash := GetHash(p)
            second_pred_hash := GetHash(predecessors[1])
            for _, file := range files {
                filename := file.Name()
                file_hash := GetHash(filename[3:])
                // find files with prefix of second predecessor
                if !file.IsDir() && strings.HasPrefix(filename, second_pred_prefix) {
                    // if the hash now routes to third predecessor remove
                    if third_pred_hash >= file_hash && file_hash < second_pred_hash {
                        err := os.Remove(dir + "/" + filename)
                        if err != nil {
                            fmt.Println("Error removing file:", err)
                        }
                    }
                }
            }
        }
    }
}

func GetPredecessors(self_id string) [3]string{
    var prev1, prev2, prev3 string

	// Create an iterator to go through the ring map
	it := ring_map.Iterator()

	for it.Next() { // get the three predecessors
		if it.Value().(string) == self_id {
			break
		}
		prev3 = prev2
		prev2 = prev1
		prev1 = it.Value().(string)
	}

	if prev1 == "" { // if the first predecessor wasn't set (current node is at the start of map)
		_, v1 := ring_map.Max()
		prev1 = v1.(string)
	}
	if prev2 == "" { // if the second predecessor wasn't set
		_, max_value := ring_map.Max()
		if prev1 == max_value.(string) { // if the first predecessor is already the last map value
			it = ring_map.Iterator()
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
		_, max_value := ring_map.Max()
		if prev1 == max_value.(string) || prev2 == max_value.(string) { // if the first or second predecessor is already the last map value
			it = ring_map.Iterator()
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

func GetSuccessor(ring_id string) string{
    successor := ""

    //iterate through the ring map
    it := ring_map.Iterator()
    for it.Next() {
		if it.Value().(string) == ring_id { // if we found the current value
            if it.Next() { // set the succesor to the next value if it's valid
				successor = it.Value().(string)
			} else { // set to first value in map (wrap around)
				_, successor_val := ring_map.Min()
                successor = successor_val.(string)
			}
		}
	}
    return successor
}

// get the tcp version of the node_id (value in the ring map)
func GetTCPVersion(id string) string {
    bytes := []byte(id)
	bytes[32] = '8'
	id = string(bytes)
    
    return id
}

func GetNodeID() string {
    return node_id
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

func ListStore() {
    dir := "./file-store"
    own_files := []string{}
    replica_files := []string{}

    files, err := ioutil.ReadDir(dir)
    if err != nil {
        fmt.Println("Error reading directory:", err)
    }

    for _, file := range files {
        if !file.IsDir() {
            filename := file.Name()
            if strings.HasPrefix(filename, os.Getenv("MACHINE_UDP_ADDRESS")[13:15]) {
                own_files = append(own_files, filename)
            } else {
                replica_files = append(replica_files, filename)
            }
        }
    }

    fmt.Println("Origin Server Files:")
    for _,filename := range own_files {
        fmt.Println(filename[3:])
    }
    fmt.Println("Replicated Files:")
    for _,filename := range own_files {
        fmt.Println(filename[3:])
    }
}