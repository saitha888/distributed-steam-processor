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
        fmt.Printf("Received Ping from %v: %s\n", addr, message)

        // Respond with Ack
        ack := "Ack"
        conn.WriteToUDP([]byte(ack), addr)
    }
}

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

