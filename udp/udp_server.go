package udp

import (
    "fmt"
    "net"
    "os"
    "github.com/joho/godotenv"
    "strings"
)

var err = godotenv.Load(".env")
var udp_port string = os.Getenv("UDP_PORT")


type Node struct {
    NodeID    string    // Unique node ID (e.g., "IP:Port-Version")
    Status    string    // Status of the node: "alive", "failed", "left"
    Timestamp string // Timestamp for the most recent status update
}

var membership_list []Node

//starts udp server that listens for pings
func UdpServer() {
    addr, err := net.ResolveUDPAddr("udp", ":"+udp_port)
    if err != nil {
        fmt.Println("Error resolving address:", err)
        return
    }

    conn, err := net.ListenUDP("udp", addr)
    if err != nil {
        fmt.Println("Error starting UDP server:", err)
        return
    }
    defer conn.Close()

    buf := make([]byte, 1024)

    for {
        n, addr, err := conn.ReadFromUDP(buf)
        if err != nil {
            fmt.Println("Error reading from UDP:", err)
            continue
        }

        message := string(buf[:n])
        fmt.Println(len(message))
        if message == "mem_list" {
            nodes := make([]string, 0)
            for _,node := range membership_list {
                current_node := node.NodeID + " " + node.Status + " " + node.Timestamp
                nodes = append(nodes, current_node)
            }
            result := strings.Join(nodes, ", ")
            fmt.Println(result)
            conn.WriteToUDP([]byte(result), addr)
        } else if message == "ping" {
            // fmt.Printf("Received Ping from %v: %s\n", addr, message)

            // Respond with Ack
            ack := "Ack"
            conn.WriteToUDP([]byte(ack), addr)
        } else if message[:4] == "fail" {
            node_id := message[5:]
            for index,node := range membership_list {
                if node_id == node.NodeID { // remove the node if it's found
                    membership_list = append(membership_list[:index], membership_list[index+1:]...)
                }
            }
            message := fmt.Sprintf("%s deleted", node_id)
            conn.WriteToUDP([]byte(message), addr)
        } else if message[:4] == "join" {
            node_id := message[5:]
            node_timestamp := message[len(message)-19:]
            found := false
            for _,node := range membership_list {
                if node_id == node.NodeID {
                    found = true
                    break
                }
            }
            if found == false { // add the node if it's not already in membership list
                new_node := Node{
                    NodeID:    node_id,  
                    Status:    "alive",           
                    Timestamp: node_timestamp,
                }
                membership_list = append(membership_list, new_node)
            }
        }
    }
}

// func HandleIntroducerRejoin() {
//     nodes := make([]string, 0)
//     for _,node := range membership_list {
//         conn.WriteToUDP([]byte(result), addr+ " " + node.Status + " " + node.Timestamp)
//         nodes = append(nodes, current_node)
//     }
//     result := strings.Join(nodes, ", ")
//     fmt.Println(result)
//     conn.WriteToUDP([]byte(result), "fa24-cs425-1210.cs.illinois.edu:9080")
// }


func ListMem() {
    // Check if the membership list is empty
    if len(membership_list) == 0 {
        fmt.Println("Membership list is empty.")
        return
    }

    // Set column widths for alignment
    nodeIDWidth := 54
    statusWidth := 4

    // Print header with formatted columns
    fmt.Printf("%-*s | %-*s | %s\n", nodeIDWidth, "NodeID", statusWidth, "Status", "Last Updated")
    fmt.Println(strings.Repeat("-", nodeIDWidth+statusWidth+25))

    // Iterate over the membership list and print each entry
    for _, node := range membership_list {
        fmt.Printf("%s | %s  | %s\n",node.NodeID, node.Status, node.Timestamp)
    }
}

