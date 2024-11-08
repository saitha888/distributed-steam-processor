package udp

import (
    "fmt"
    "net"
    "strings"
    "time"
    "strconv"
    "bufio"
    "os"
    "log"
    "io/ioutil"
)

// Function to join system
func JoinSystem(address string) {
    if udp_address == introducer_address {
        IntroducerJoin()
    } else {
        ProcessJoin(address)
    }
}

// Handles the introducer joining/rejoining the system
func IntroducerJoin() {
    membership_list = nil // reset membership list if rejoining
    inc_num += 1 // increment incarnation number (starts at 1)

    if node_id == ""{ // if it's joining and not rejoining
        node_id  = udp_address + "_" + time.Now().Format("2006-01-02_15:04:05") // create unique node id
        AddNode(node_id, 1, "alive") // add to membership list
    } 

    // go through ports, get first alive membership list
    for _,port := range ports {
        if port[13:15] == udp_address[13:15] { // don't connect to port if its at self
            continue
        }
        // connect to the port
        conn, _ := DialUDPClient(port)
        defer conn.Close()

        // request membership list
        message := fmt.Sprintf("mem_list") 
        _, err = conn.Write([]byte(message))
        if err != nil {
            fmt.Println("Error writing to: " + port + " when introducer requesting membership list.", err)
            continue
        }

        // Read the response from the port
        buf := make([]byte, 1024)

        conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))

        n, _, err := conn.ReadFromUDP(buf)
        if err != nil {
            fmt.Println("Error reading from: " + port + " when introducer requesting membership list.", err)
            continue
        }

        memb_list_string := string(buf[:n])
        memb_list := strings.Split(memb_list_string,", ")

        if memb_list_string == "" { // if none keep membership list empty
            continue
        } else { // else set membership list to recieved membership list and break
            for _,node :=  range memb_list {
                node_vars := strings.Split(node, " ")
                inc, _ := strconv.Atoi(node_vars[2])
                AddNode(node_vars[0], inc, node_vars[1])
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
            if node_address != udp_address { // check that it's not self
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

    // handle fixing the ring
    ring_id := GetTCPVersion(node_id)
    SelfJoin(ring_id)
}

// Handles a process joining/rejoining the system
func ProcessJoin(address string) {
    membership_list = nil // reset membership list if rejoining
    inc_num += 1 // increment incarnation number (starts at 1)

    // Initialize node id for machine.
    if node_id == "" { // if it's joining and not rejoining
        node_id = udp_address + "_" + time.Now().Format("2006-01-02_15:04:05") // create unique node id
    }

    // Connect to introducer
    conn_introducer, err := DialUDPClient(introducer_address)
    defer conn_introducer.Close()


    // Send join message to introducer
    message := fmt.Sprintf("join %s", node_id)
    _, err = conn_introducer.Write([]byte(message))
    if err != nil {
        fmt.Println("Error sending message to introducer when initially joining: ", err)
        return
    }

    // Read the response from the introducer (membership list to copy)
    buf := make([]byte, 1024)
    n, _, err := conn_introducer.ReadFromUDP(buf)
    if err != nil {
        fmt.Println("Error reading membership list from introducer when initially joining: ", err)
        return
    }

    memb_list_string := string(buf[:n])
    memb_list := strings.Split(memb_list_string,", ")

    // Update machine's membership list
    for _,node :=  range memb_list {
        node_vars := strings.Split(node, " ")
        inc, _ := strconv.Atoi(node_vars[2])
        AddNode(node_vars[0], inc, node_vars[1])
    }

    fmt.Printf("Received mem_list from introducer\n")

    // handle fixing the ring id
    ring_id := GetTCPVersion(node_id)
    SelfJoin(ring_id)
}

// Handles a process pulling files from successors and predecessors when it joins the system
func SelfJoin(ring_id string) {
    // find successor and connect
    successor := GetSuccessor(ring_id)
    if len(successor) == 0 {
        return
    }
    successor_port := successor[:36]

    if successor_port != tcp_address {
        conn_successor, err := net.Dial("tcp", successor_port)
        if err != nil {
            fmt.Println("Error connecting to successor when joining system: ", err)
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
                        new_filename := "./file-store/" + file_prefix + "-" + filename // rename the file
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
        if pred_port != tcp_address {
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

// Handles anoher process joining the system
func ProcessJoinMessage(message string) {
    joined_node := message[5:]
    index := FindNode(joined_node)
    if index >= 0 { // machine was found
        changeStatus(index, "alive")
    } else { // machine was not found
        AddNode(joined_node, 1, "alive")
    }
    send := "Node join detected for: " + joined_node + " at " + time.Now().Format("15:04:05") + "\n"
    appendToFile(send, logfile)
    // check if a predecessor got added
    NewJoin(joined_node)
}

// Checks if a new predecessor got added and files need to be updated
func NewJoin(joined_node string) {
    // get ring ids for both nodes
    self_id := GetTCPVersion(node_id)
    joined_node = GetTCPVersion(joined_node)

    // get predecessors
    predecessors := GetPredecessors(self_id)

    dir := "./file-store" 

    // find all the prefixes for what files may need to be removed
    curr_prefix := udp_address[13:15]
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

// Handles adding node to system
func AddNode(node_id string, node_inc int, status string){
    ring_id := GetTCPVersion(node_id)
    ring_hash := GetHash(ring_id)

    new_node := Node{
        NodeID:    node_id,  
        Status:    status,           
        Inc: node_inc,
        RingID: ring_hash,
    }
    membership_list = append(membership_list, new_node)

    ring_map.Put(ring_hash, ring_id)
}