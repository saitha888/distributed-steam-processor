package udp

import (
    "fmt"
    "net"
    "os"
    "github.com/joho/godotenv"
    "time"
)

var err = godotenv.Load(".env")
var udp_port string = os.Getenv("UDP_PORT")
var addr string = os.Getenv("MACHINE_ADDRESS")

var type Node struct {
    NodeID    string    // Unique node ID (e.g., "IP:Port-Version")
    Status    string    // Status of the node: "alive", "failed", "left"
    Timestamp time.Time // Timestamp for the most recent status update
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

