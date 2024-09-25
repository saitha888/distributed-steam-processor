package udp

import (
    "fmt"
    "net"
    "time"
)

func JoinSystem(nodeID string) {
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

    // Send the unique node ID to the introducer
    message := fmt.Sprintf("join %s", nodeID) // Format the message as "join <nodeID>"
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

    // Print the response from the introducer (e.g., acknowledgment or membership list)
    fmt.Printf("Received response from introducer: %s\n", string(buf[:n]))

    // Optional: Sleep for a while before ending the client (to show acknowledgment)
    time.Sleep(3 * time.Second)
}


// Function to act as a client and send ping messages to a target server
func PingClient(targetAddr string, type string) {
    addr, err := net.ResolveUDPAddr("udp", targetAddr)
    if err != nil {
        fmt.Println("Error resolving server address:", err)
        return
    }

    conn, err := net.DialUDP("udp", nil, addr)
    if err != nil {
        fmt.Println("Error connecting to server:", err)
        return
    }
    defer conn.Close()

    // Periodically send ping messages
    for {
        message := "Ping from client"
        _, err = conn.Write([]byte(message))
        if err != nil {
            fmt.Println("Error sending message:", err)
            return
        }

        // Buffer to store the response from the server
        buf := make([]byte, 1024)

        // Read the response from the server (acknowledgment)
        n, _, err := conn.ReadFromUDP(buf)
        if err != nil {
            fmt.Println("Error reading from server:", err)
            return
        }

        // Print the response from the server
        fmt.Printf("Received response from server: %s\n", string(buf[:n]))

        // Sleep for a while before sending the next ping
        time.Sleep(6 * time.Second) // adjust the interval as needed
    }
}