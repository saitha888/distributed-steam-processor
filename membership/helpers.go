package membership

import (
    "time"
    "math/rand"
    "distributed_system/global"
    "fmt"
    "net"
)

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

func CheckStatus(node string) string {
    index := FindNode(node)
    if index >= 0 {
        return global.Membership_list[index].Status
    }
    return "none"
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
