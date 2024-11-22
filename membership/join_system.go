package membership

import (
    "fmt"
    "net"
    "strings"
    "time"
    "strconv"
    "os"
    "log"
    "io/ioutil"
    "distributed_system/global"
    "distributed_system/util"
)

// Function to join system
func JoinSystem(address string) {
    if global.Udp_address == global.Introducer_address {
        IntroducerJoin()
    } else {
        ProcessJoin(address)
    }
}

// Handles the introducer joining/rejoining the system
func IntroducerJoin() {
    global.Membership_list = nil // reset membership list if rejoining
    global.Inc_num += 1 // increment incarnation number (starts at 1)

    if global.Node_id == ""{ // if it's joining and not rejoining
        curr_time := time.Now().Format("2006-01-02_15:04:05")
        global.Node_id  = global.Udp_address + "_" + curr_time // create unique node id
        global.Ring_id = global.Tcp_address + "_" + curr_time
        AddNode(global.Node_id, 1, "alive") // add to membership list
    } 

    // go through global.Udp_ports, get first alive membership list
    for _,port := range global.Udp_ports {
        if port[13:15] == global.Udp_address[13:15] { // don't connect to port if its at self
            continue
        }
        // connect to the port
        conn, _ := util.DialUDPClient(port)
        defer conn.Close()

        // request membership list
        message := fmt.Sprintf("mem_list") 
        _, err := conn.Write([]byte(message))
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
            index := FindNode(global.Node_id)
            if index >= 0 {
                util.ChangeStatus(index, "alive")
            }      

            break
        }
    }

    // send to all other machines it joined 
    for _,node := range global.Membership_list {
        if node.Status == "alive" {
            node_address := node.NodeID[:36]
            if node_address != global.Udp_address { // check that it's not self
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

                result := "join " + global.Node_id
                // send join message
                conn.Write([]byte(result))
            }
        }
    }

    // handle fixing the ring
    SelfJoin(global.Ring_id)
}

// Handles a process joining/rejoining the system
func ProcessJoin(address string) {
    global.Membership_list = nil // reset membership list if rejoining
    global.Inc_num += 1 // increment incarnation number (starts at 1)

    // Initialize node id for machine.
    if global.Node_id == "" { // if it's joining and not rejoining
        global.Node_id = global.Udp_address + "_" + time.Now().Format("2006-01-02_15:04:05") // create unique node id
    }

    // Connect to introducer
    conn_introducer, err := util.DialUDPClient(global.Introducer_address)
    defer conn_introducer.Close()


    // Send join message to introducer
    message := fmt.Sprintf("join %s", global.Node_id)
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
    SelfJoin(global.Ring_id)
}

// Handles a process pulling files from successors and predecessors when it joins the system
func SelfJoin(ring_id string) {
    // find successor and connect
    successor := util.GetSuccessor(ring_id)
    if len(successor) == 0 {
        return
    }
    successor_port := successor[:36]

    if successor_port != global.Tcp_address {
        conn_successor, err := net.Dial("tcp", successor_port)
        if err != nil {
            fmt.Println("Error connecting to successor when joining system: ", err)
        }
        defer conn_successor.Close()

        // Send a split message to the successor
        fmt.Fprintln(conn_successor, "split " + ring_id)
        data := global.Message{
            Action: "split" + ring_id,
            Filename:  "",
            FileContents: "",
        }
        util.GetFiles(conn_successor, data)
    }

    // find the predecessors
    predecessors := util.GetPredecessors(ring_id)
    // go through each predecessor
    for i,p :=  range predecessors {
        if i == 2 || len(p) == 0 { // if it's the third predecessor or empty continue
            continue
        }
        // connect to the predecesorr
        pred_port := p[:36]
        if pred_port != global.Tcp_address {
            conn_pred, err := net.Dial("tcp", pred_port)
            if err != nil {
                fmt.Println("Error connecting to server:", err)
            }
            defer conn_pred.Close()

            // Send a global.Message to the server
            data := global.Message{
                Action: "pull",
                Filename:  "",
                FileContents: "",
            }
            util.GetFiles(conn_pred,data)
        }
    }
}

// Handles anoher process joining the system
func ProcessJoinMessage(message string) {
    joined_node := message[5:]
    index := FindNode(joined_node)
    if index >= 0 { // machine was found
        util.ChangeStatus(index, "alive")
    } else { // machine was not found
        AddNode(joined_node, 1, "alive")
    }
    send := "Node join detected for: " + joined_node + " at " + time.Now().Format("15:04:05") + "\n"
    util.AppendToFile(send, global.Membership_log)
    // check if a predecessor got added
    NewJoin(joined_node)
}

// Checks if a new predecessor got added and files need to be updated
func NewJoin(joined_node string) {
    // get ring ids for both nodes
    self_id := util.GetTCPVersion(global.Node_id)
    joined_node = util.GetTCPVersion(joined_node)

    // get predecessors
    predecessors := util.GetPredecessors(self_id)

    dir := "./file-store" 

    // find all the prefixes for what files may need to be removed
    curr_prefix := global.Udp_address[13:15]
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
            pred_hash := util.GetHash(p) // get the hash of the predecessor
            for _, file := range files {
                filename := file.Name()
                file_hash := util.GetHash(filename[3:])
                // find files with prefix of current server
                if !file.IsDir() && strings.HasPrefix(filename, curr_prefix) {
                    // if the hash now routes to predecessor change the prefix
                    if pred_hash >= file_hash && file_hash < util.GetHash(self_id) {
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
            second_pred_hash := util.GetHash(p)
            pred_hash := util.GetHash(predecessors[0])
            for _, file := range files {
                filename := file.Name()
                file_hash := util.GetHash(filename[3:])
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
            third_pred_hash := util.GetHash(p)
            second_pred_hash := util.GetHash(predecessors[1])
            for _, file := range files {
                filename := file.Name()
                file_hash := util.GetHash(filename[3:])
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
    ring_id := util.GetTCPVersion(node_id)
    ring_hash := util.GetHash(ring_id)

    new_node := global.Node{
        NodeID:    node_id,  
        Status:    status,           
        Inc: node_inc,
        RingID: ring_hash,
    }
    global.Membership_list = append(global.Membership_list, new_node)

    global.Ring_map.Put(ring_hash, ring_id)
}
