package udp

import (
    "fmt"
    "net"
    "time"
    "strings"
    "math/rand"
    "os"
)

var node_id string = ""

func JoinSystem(address string) {
    addr, err := net.ResolveUDPAddr("udp", "fa24-cs425-1210.cs.illinois.edu:9080")
    if err != nil {
        fmt.Println("Error resolving introducer address:", err)
        return
    }

    conn, err := net.DialUDP("udp", nil, addr)
    if err != nil {
        fmt.Println("Error connecting to introducer:", err)
        return
    }
    defer conn.Close()

    // Send the address to the introducer
    if node_id == "" {
        node_id = address + "_" + time.Now().Format("2006-01-02_15:04:05")
    }
    message := fmt.Sprintf("join %s", node_id) // Format the message as "join <address>"
    _, err = conn.Write([]byte(message))
    if err != nil {
        fmt.Println("Error sending message to introducer:", err)
        return
    }

    // Buffer to store the response from the introducer (optional)
    buf := make([]byte, 1024)

    // Read the response from the introducer (acknowledgment or membership list)
    n, _, err := conn.ReadFromUDP(buf)
    if err != nil {
        fmt.Println("Error reading from introducer:", err)
        return
    }
    memb_list_string := string(buf[:n])
    memb_list := strings.Split(memb_list_string,", ")
    fmt.Println(memb_list)

    membership_list = nil

    for _,node :=  range memb_list {
        node_vars := strings.Split(node, " ")
        new_node := Node{
            NodeID:    node_vars[0],  
            Status:    node_vars[1],           
            Timestamp: node_vars[2],
        }
        if new_node.NodeID[:36] == os.Getenv("MACHINE_ADDRESS") {
            node_id = new_node.NodeID
        }
        membership_list = append(membership_list, new_node) 
    }
    // Print the response from the introducer (e.g., acknowledgment or membership list)
    fmt.Printf("Received response from introducer: %s\n", string(buf[:n]))

    // Optional: Sleep for a while before ending the client (to show acknowledgment)
    time.Sleep(3 * time.Second)

}

// Function to randomly select one node and ping it
func PingClient() {

    rand.Seed(time.Now().UnixNano())

    // Select a random node that is not the current machine
    var target_node *Node
    for {
        random_index := rand.Intn(len(membership_list))
        selected_node := membership_list[random_index]

        if selected_node.NodeID != node_id && selected_node.Status == "alive" { 
            target_node = &selected_node
            break
        }
    }

    target_addr := target_node.NodeID[:36]
    // target_addr := "fa24-cs425-1202.cs.illinois.edu:9082"


    addr, err := net.ResolveUDPAddr("udp", target_addr)
    if err != nil {
        fmt.Println("Error resolving target address:", err)
        return
    }

    // Dial UDP to the target node
    conn, err := net.DialUDP("udp", nil, addr)
    if err != nil {
        fmt.Println("Error connecting to target node:", err)
        return
    }
    defer conn.Close()

    // Send a ping message
    message := fmt.Sprintf("ping")
    _, err = conn.Write([]byte(message))
    if err != nil {
        fmt.Println("Error sending ping message:", err)
        return
    }

    buf := make([]byte, 1024)

    conn.SetReadDeadline(time.Now().Add(2 * time.Second))

    n, _, err := conn.ReadFromUDP(buf)
    if err != nil {
        fmt.Println("Error reading ack from target node:", err)
        
        for _,node := range membership_list {
            sendFailure(node.NodeID, target_node.NodeID)
        }
    }

    ack_message := string(buf[:n])
    fmt.Printf("Received ack from %s: %s\n", target_node.NodeID, ack_message)
}

func sendFailure(node_id string, to_delete string) {

    target_addr := node_id[:36]
    // target_addr := "fa24-cs425-1202.cs.illinois.edu:9082"


    addr, err := net.ResolveUDPAddr("udp", target_addr)
    if err != nil {
        fmt.Println("Error resolving target address:", err)
        return
    }

    // Dial UDP to the target node
    conn, err := net.DialUDP("udp", nil, addr)
    if err != nil {
        fmt.Println("Error connecting to target node:", err)
        return
    }

    defer conn.Close()
    message := fmt.Sprintf("fail %s", to_delete)
    _, err = conn.Write([]byte(message))
    if err != nil {
        fmt.Println("Error sending fail message:", err)
        return
    }
}

func LeaveList() {
    // change own status to left, inform other machines to change status to left
    for _,node :=  range membership_list {
        fmt.Println("global variable node id " + node_id)
        if node.NodeID == node_id { // check if at self
            node.Status = "left"
        } else { // other machine
            node_address := node.NodeID[:36]
            // connect to machine 
            addr, err := net.ResolveUDPAddr("udp", node_address)
            if err != nil {
                fmt.Println("Error resolving target address:", err)
                return
            }
            conn, err := net.DialUDP("udp", nil, addr)
            if err != nil {
                fmt.Println("Error connecting to target node:", err)
                return
            }
            defer conn.Close()
            // send leave message
            message := fmt.Sprintf("leave " + node_id)
            _, err = conn.Write([]byte(message))
            if err != nil {
                fmt.Println("Error sending ping message:", err)
                return
            }
        }
    }
}