package udp

import (
    "fmt"
    "net"
    "os"
    "github.com/joho/godotenv"
    "strings"
)

// global variables
var err = godotenv.Load(".env")
var udp_port string = os.Getenv("UDP_PORT")
var membership_list []Node


// struct for each process
type Node struct {
    NodeID    string  
    Status    string    
    Timestamp string 
}

//starts udp server that listens for pings
func UdpServer() {
    conn, err = ConnectToMachine(udp_port)

    buf := make([]byte, 1024)

    for {
        // read response from machine
        n, addr, err := conn.ReadFromUDP(buf)
        if err != nil {
            fmt.Println("Error reading from UDP:", err)
            continue
        }

        message := string(buf[:n])
        if message == "mem_list" { // introducer asking for membership list
            result := MembershiplistToString()
            conn.WriteToUDP([]byte(result), addr)
        } else if message == "ping" { // machine checking health
            ack := "Ack"
            conn.WriteToUDP([]byte(ack), addr)
        } else if message[:4] == "fail" { // machine failure detected
            failed_node := message[5:]
            RemoveNode(failed_node)
        } else if message[:4] == "join" { // new machine joined
            joined_node := message[5:]
            node_timestamp := message[len(message)-19:]
            index := FindNode(joined_node)
            if index >= 0 { // machine was found
                changeStatus(index, "alive")
            } else { // machine was not found
                AddNode(joined_node, node_timestamp)
            }
        } else if message[:5] == "leave" { // machine left
            left_node := message[6:]
            index := FindNode(joined_node)
            changeStatus(index, "left")
        }
    }
}

func ListMem() {
    if len(membership_list) == 0 {
        fmt.Println("Membership list is empty.")
        return
    }

    nodeIDWidth := 54
    statusWidth := 4

    fmt.Printf("%-*s | %-*s | %s\n", nodeIDWidth, "NodeID", statusWidth, "Status", "Last Updated")
    fmt.Println(strings.Repeat("-", nodeIDWidth+statusWidth+25))

    // Go through membership list and print each entry
    for _, node := range membership_list {
        fmt.Printf("%s | %s  | %s\n",node.NodeID, node.Status, node.Timestamp)
    }
}


// Function to connect to another machine
func ConnectToMachine(port string) (*net.UDPConn, error){
    addr, err := net.ResolveUDPAddr("udp", ":" + port)
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
    return conn, nil
}

// Turn the membership list global variable into a string
func MembershiplistToString() string{
    nodes := make([]string, 0)
    for _,node := range membership_list {
        current_node := node.NodeID + " " + node.Status + " " + node.Timestamp
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
}

// Add a machine to the membership list
func AddNode(node_id string, node_timestamp string){
    new_node := Node{
        NodeID:    node_id,  
        Status:    "alive",           
        Timestamp: node_timestamp,
    }
    membership_list = append(membership_list, new_node)
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

// Change the status of a machine in the list
func changeStatus(index int, message string){
    membership_list[index].Status = message
}
