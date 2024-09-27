package udp

import (
    "fmt"
    "net"
    "time"
    "strings"
    "math/rand"
)

var node_id string = ""

func JoinSystem(address string) {

    conn, err := DialUDPClient("fa24-cs425-1210.cs.illinois.edu:9080")
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
        AddNode(node_vars[0], node_vars[1], node_vars[2])
    }

    // Print the response from the introducer (e.g., acknowledgment or membership list)
    fmt.Printf("Received response from introducer: %s\n", string(buf[:n]))

    // Optional: Sleep for a while before ending the client (to show acknowledgment)
    time.Sleep(3 * time.Second)

}

// Function to randomly select one node and ping it
func PingClient() {

    target_node := SelectRandomNode()

    target_addr := target_node.NodeID[:36]
    // target_addr := "fa24-cs425-1202.cs.illinois.edu:9082"


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

    conn.SetReadDeadline(time.Now().Add(2 * time.Second))

    _, _, err2 := conn.ReadFromUDP(buf)
    if err2 != nil {
        fmt.Println("Error reading ack from target node:", err2)
        RemoveNode(target_node.NodeID)
        for _,node := range membership_list {
            SendFailure(node.NodeID, target_node.NodeID)
        }
    }

    // ack_message := string(buf[:n])
    // fmt.Printf("Received ack from %s: %s\n", target_node.NodeID, ack_message)
}

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

func LeaveList() {
    // change own status to left, inform other machines to change status to left
    for _,node :=  range membership_list {
        if node.NodeID == node_id { // check if at self
            node.Status = "left"
        } else { 

            // other machine
            node_address := node.NodeID[:36]
            
            // connect to machine 
            conn, err := DialUDPClient(node_address)
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

// helper function to resolve and dial a UDP connection.
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

//function that selects random machine to pig
func SelectRandomNode() *Node {
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
    return target_node
}



