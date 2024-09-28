package udp

import (
    "fmt"
    "net"
    "time"
    "strings"
    "math/rand"
    "strconv"
)

// Global variable to save unique node ID
var node_id string = ""

// Function to join system through the introducer
func JoinSystem(address string) {
    // increment the incarnation number
    inc_num += 1
    // Connect to introducer
    conn, err := DialUDPClient("fa24-cs425-1210.cs.illinois.edu:9080")
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
        AddNode(node_vars[0], inc, node_vars[1])
    }

    // Print the response from the introducer (e.g., acknowledgment or membership list)
    fmt.Printf("Received mem_list from introducer\n")

}

// Function to randomly select a node from the system and ping it
func PingClient(plus_s bool) {
    target_node := SelectRandomNode()
    target_addr := target_node.NodeID[:36]

    // Connect to the randomly selected node
    conn, err := DialUDPClient(target_addr)
    defer conn.Close()

    // Send a ping message
    message := fmt.Sprintf("ping")
    _, err = conn.Write([]byte(message))
    if err != nil {
        fmt.Println("Error sending ping message:", err)
        return
    }
    buf := make([]byte, 1024)


    n, _, err2 := conn.ReadFromUDP(buf)
    if err2 != nil {
        fmt.Println("Error reading from introducer:", err)
    } else {
        ack := string(buf[:n])
        recieved_node_id := ack[:36]
        recieved_inc, _ := strconv.Atoi(ack[38:])
        index := FindNode(recieved_node_id)
        if index >= 0 {
            membership_list[index].Status = "alive"
            membership_list[index].Inc = recieved_inc
        }

    }
    if err2 != nil {
        if plus_s == false {
            message := "Node failure detected for: " + target_node.NodeID + " from machine " + udp_port + " at " + time.Now().Format("15:04:05") + "\n"
            appendToFile(message, logfile)
            RemoveNode(target_node.NodeID)
            for _,node := range membership_list {
                SendFailure(node.NodeID, target_node.NodeID)
            }
        }
        if plus_s {
            message := "Node suspect detected for: " + target_node.NodeID + " from machine " + udp_port + " at " + time.Now().Format("15:04:05") + "\n"
            appendToFile(message, logfile)
            for _,node := range membership_list {
                SendSuspected(node.NodeID, target_node.NodeID)
            }
            susTimeout(4*time.Second, target_node.NodeID, target_node.Inc)
            index := FindNode(target_node.NodeID)
            if index < 0 {
                for _, node := range(membership_list) {
                    SendFailure(node.NodeID, target_node.NodeID)
                }
            }

        }
    }

    // ack_message := string(buf[:n])
    // fmt.Printf("Received ack from %s: %s\n", target_node.NodeID, ack_message)
}

// Function to send a failure message
func SendFailure(node_id string, to_delete string) {

    target_addr := node_id[:36]
    conn, err := DialUDPClient(target_addr)
    defer conn.Close()

    message := fmt.Sprintf("fail %s", to_delete)
    _, err = conn.Write([]byte(message))
    if err != nil {
        fmt.Println("Error sending fail message:", err)
        return
    }
}

// Function to send that a node is suspected
func SendSuspected(node_id string, sus_node string) {

    target_addr := node_id[:36]
    conn, err := DialUDPClient(target_addr)
    defer conn.Close()

    message := fmt.Sprintf("suspected %s", sus_node)
    _, err = conn.Write([]byte(message))
    if err != nil {
        fmt.Println("Error sending fail message:", err)
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
                fmt.Println("Error sending ping message:", err)
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
func SelectRandomNode() *Node {
    rand.Seed(time.Now().UnixNano())
    var target_node *Node
    for {
        random_index := rand.Intn(len(membership_list))
        selected_node := membership_list[random_index]
        if selected_node.NodeID != node_id && selected_node.Status == "alive" { 
            target_node = &selected_node
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
                break
            }
		}
	}
}
