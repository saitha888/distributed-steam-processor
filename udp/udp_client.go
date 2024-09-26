package udp

import (
    "fmt"
    "net"
    "time"
    "strings"
    "math/rand"
    "os"
)

var node_id = ""

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
    message := fmt.Sprintf("join %s", address) // Format the message as "join <address>"
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

        if selected_node.NodeID != node_id { 
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

    // buf := make([]byte, 1024)

    // conn.SetReadDeadline(time.Now().Add(2 * time.Second))

    // n, _, err := conn.ReadFromUDP(buf)
    // if err != nil {
    //     fmt.Println("Error reading ack from target node:", err)
        
    //     // for _,node := range membership_list {
    //     //     sendFailure()
    //     // }
    // }

    // ack_message := string(buf[:n])
    // fmt.Printf("Received ack from %s: %s\n", target_node.NodeID, ack_message)
}





// Function to act as a client and send ping messages to a target server
// func PingClient(targetAddr string, type string) {
//     addr, err := net.ResolveUDPAddr("udp", targetAddr)
//     if err != nil {
//         fmt.Println("Error resolving server address:", err)
//         return
//     }

//     conn, err := net.DialUDP("udp", nil, addr)
//     if err != nil {
//         fmt.Println("Error connecting to server:", err)
//         return
//     }
//     defer conn.Close()

//     // Periodically send ping messages
//     for {
//         message := "Ping from client"
//         _, err = conn.Write([]byte(message))
//         if err != nil {
//             fmt.Println("Error sending message:", err)
//             return
//         }

//         // Buffer to store the response from the server
//         buf := make([]byte, 1024)

//         // Read the response from the server (acknowledgment)
//         n, _, err := conn.ReadFromUDP(buf)
//         if err != nil {
//             fmt.Println("Error reading from server:", err)
//             return
//         }

//         // Print the response from the server
//         fmt.Printf("Received response from server: %s\n", string(buf[:n]))

//         // Sleep for a while before sending the next ping
//         time.Sleep(6 * time.Second) // adjust the interval as needed
//     }
// }