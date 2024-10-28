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
        }
    }
    
    bytes := []byte(node_id)
	
	bytes[32] = '8'
	
	node_id_ring := string(bytes)

    ring_map.Remove(GetHash(node_id_ring))
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
	
	node_id_ring := string(bytes)

    ring_map.Put(GetHash(node_id), node_id_ring)
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

func GetHash(data string) string {
	hash := sha256.Sum256([]byte(data))
	return fmt.Sprintf("%x", hash) 
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
		fmt.Printf("Hash: %s, Node: %s\n", hash, id)
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
        node_id  = os.Getenv("MACHINE_UDP_ADDRESS") + "_" + time.Now().Format("2006-01-02_15:04:05")
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
    target_value := os.Getenv("MACHINE_TCP_ADDRESS")

    bytes := []byte(node_id)
	
	bytes[32] = '8'
	
	ring_id := string(bytes)

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

    successor := ""
    // find successor and get files
    it := ring_map.Iterator()
    for it.Next() {
		if it.Value().(string)[:36] == target_value {
            if it.Next() {
				successor = it.Value().(string)
			} else {
				_, successor_val := ring_map.Min()
                successor = successor_val.(string)
			}
		}
	}
    successor_port := successor[:36]
    fmt.Println("successor found as: ", successor_port)
    if successor_port != os.Getenv("MACHINE_TCP_ADDRESS") {
        conn_successor, err := net.Dial("tcp", successor_port)
        if err != nil {
            fmt.Println("Error connecting to server:", err)
        }
        defer conn_successor.Close()

        // Send a message to the server
        fmt.Fprintln(conn_successor, "split " + ring_id)

        // Read multiple responses from the server
        scanner := bufio.NewScanner(conn_successor)
        for scanner.Scan() {
            server_response := scanner.Text()
            filename := strings.Split(server_response, " ")[0]
            argument_length := 1 + len(filename)
            contents := server_response[argument_length:]
            new_filename := machine_address[13:15] + "-" + filename

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
        if err := scanner.Err(); err != nil {
            fmt.Println("Error reading from server:", err)
        }
    }

    // find the predecessors and get files
	var prev1, prev2 string
	var prev_key1 string

	it = ring_map.Iterator()

	for it.Next() {
		if it.Value().(string) == target_value {
			break
		}

		prev2 = prev1
		prev1 = it.Value().(string)
		prev_key1 = it.Key().(string)
	}

    if prev1 == "" {
        k1, v1 := ring_map.Max()
        prev_key1 = k1.(string)
        prev1 = v1.(string)
    }
    if prev2 == "" { 
        max_key, max_value := ring_map.Max()
        
        if prev_key1 == max_key.(string) {
            it := ring_map.Iterator()
            for it.Next() {
                if it.Key() == prev_key1 {
                    break
                }
                _, prev2 = it.Key(), it.Value().(string)
            }
        } else {
            prev2 = max_value.(string)
        }
    }
    predecessors := [2]string{prev1, prev2}
    fmt.Println("predecessors found as: ", predecessors)
    // get files from predecessors
    for _,p :=  range predecessors {
        pred_port := p[:36]
        if pred_port != os.Getenv("MACHINE_TCP_ADDRESS"){
            conn_pred, err := net.Dial("tcp", pred_port)
            if err != nil {
                fmt.Println("Error connecting to server:", err)
            }
            defer conn_pred.Close()

            // Send a message to the server
            fmt.Fprintln(conn_pred, "pull")

            // Read multiple responses from the server
            scanner := bufio.NewScanner(conn_pred)
            for scanner.Scan() {
                server_response := scanner.Text()
                filename := strings.Split(server_response, " ")[0]
                argument_length := 1 + len(filename)
                contents := server_response[argument_length:]
                new_filename := machine_address[13:15] + "-" + filename

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
            if err := scanner.Err(); err != nil {
                fmt.Println("Error reading from server:", err)
            }
        }
        
    }
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
    target_value := os.Getenv("MACHINE_TCP_ADDRESS")

    var prev1, prev2, prev3 string
	var prev_key1, prev_key2 string

	// Create an iterator to go through the TreeMap
	it := ring_map.Iterator()

	for it.Next() {
		if it.Value().(string) == target_value {
			break
		}

		prev3 = prev2
		prev2 = prev1
		prev_key2 = prev_key1
		prev1 = it.Value().(string)
		prev_key1 = it.Key().(string)
	}

	if prev1 == "" {
		k1, v1 := ring_map.Max()
		prev_key1 = k1.(string)
		prev1 = v1.(string)
	}
	if prev2 == "" {
		max_key, max_value := ring_map.Max()
		if prev_key1 == max_key.(string) {
			it = ring_map.Iterator()
			for it.Next() {
				if it.Key() == prev_key1 {
					break
				}
				prev_key2, prev2 = it.Key().(string), it.Value().(string)
			}
		} else {
			prev_key2, prev2 = max_key.(string), max_value.(string)
		}
	}
	if prev3 == "" {
		max_key, max_value := ring_map.Max()
		if prev_key1 == max_key.(string) || prev_key2 == max_key.(string) {
			it = ring_map.Iterator()
			for it.Next() {
				if it.Key() == prev_key1 || it.Key() == prev_key2 {
					break
				}
				prev3 = it.Value().(string)
			}
		} else {
			prev3 = max_value.(string)
		}
	}

	// Collect all three predecessors in a slice
	predecessors := [3]string{prev1, prev2, prev3}

    bytes := []byte(joined_node)
	
	bytes[32] = '8'
	
	joined_node = string(bytes)

    dir := "./file-store" 
    curr_prefix := target_value[13:15]
    first_pred_prefix, second_pred_prefix, third_pred_prefix := "","",""
    if len(predecessors[0]) > 0 {
        first_pred_prefix = predecessors[0][13:15]
    }
    if len(predecessors[1]) > 0 {
        second_pred_prefix = predecessors[0][13:15]
    }
    if len(predecessors[2]) > 0 {
        third_pred_prefix = predecessors[0][13:15]
    }

    files, err := ioutil.ReadDir(dir)
    if err != nil {
        log.Fatal(err)
    }
    
    for i,p :=  range predecessors {
        if p == joined_node && i == 0 { // if it's immediate predecessor
            fmt.Println("immediate predecessor joined")
            for _, file := range files {
                filename := file.Name()
                file_hash := GetHash(filename[4:])
                // find files with prefix of current server
                if !file.IsDir() && strings.HasPrefix(filename, curr_prefix) {
                    // if the hash now routes to predecessor change the prefix
                    pred_hash := GetHash(p)
                    if pred_hash >= file_hash {
                        old_filename := "file-store/" + filename
                        new_filename := "file-store/" + p[13:15] + "-" + filename[4:]
                        os.Rename(old_filename, new_filename)
                    }
                }
                // find files with prefix of second predecessor and remove
                if !file.IsDir() && strings.HasPrefix(filename, third_pred_prefix) {
                    err := os.Remove(filename)
                    if err != nil {
                        fmt.Println("Error removing file:", err)
                    }
                }
            }
        } else if p == joined_node && i == 1{ // if it's second predecessor
            fmt.Println("second predecessor joined")
            for _, file := range files {
                filename := file.Name()
                file_hash := GetHash(filename[4:])
                // find files with prefix of first predecessor
                if !file.IsDir() && strings.HasPrefix(filename, first_pred_prefix) {
                    // if the hash now routes to second predecessor change the prefix
                    pred_hash := GetHash(predecessors[1])
                    if pred_hash >= file_hash {
                        old_filename := "file-store/" + filename
                        new_filename := "file-store/" + second_pred_prefix + "-" + filename[4:]
                        os.Rename(old_filename, new_filename)
                    }
                }
                // find files with prefix of second predecessor and remove
                if !file.IsDir() && strings.HasPrefix(filename, third_pred_prefix) {
                    err := os.Remove(filename)
                    if err != nil {
                        fmt.Println("Error removing file:", err)
                    }
                }
            }
        }
    }
}