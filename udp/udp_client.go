package udp

import (
    "fmt"
    "net"
    "time"
    "strings"
    "math/rand"
)

// Global variable to save unique node ID
var node_id string = ""

// Function to join system through the introducer
func JoinSystem(address string) {

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
    fmt.Println(memb_list)

    // Clear existing membership list if dealing with a node that left
    membership_list = nil

    // Update machine's membership list
    for _,node :=  range memb_list {
        node_vars := strings.Split(node, " ")
        AddNode(node_vars[0], node_vars[2], node_vars[1])
    }

    // Print the response from the introducer (e.g., acknowledgment or membership list)
    fmt.Printf("Received mem_list from introducer")

}

// Function to randomly select a node from the system and ping it
func PingClient() {

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

    // Give machine 2 seconds to receive the acknowledgement
    conn.SetReadDeadline(time.Now().Add(2 * time.Second))

    _, _, err2 := conn.ReadFromUDP(buf)
    if err2 != nil {

        // If machine does not receive ack, mark it as failed and send fail message to the system
        fmt.Println("Failure detected: " + target_node.NodeID + " " + time.Now().Format("15:04:05"))
        RemoveNode(target_node.NodeID)
        for _,node := range membership_list {
            SendFailure(node.NodeID, target_node.NodeID)
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

//Function to leave the system
func LeaveList() {

    // Change own status to left, inform other machines to change status to left
    for _,node :=  range membership_list {
        if node.NodeID == node_id { // check if at self
            node.Status = "left"
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