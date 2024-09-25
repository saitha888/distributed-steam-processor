package udp

import (
    "fmt"
    "net"
    "os"
    "os/exec"
    "strings"
    "io"
    "strconv"
    "time"
)

// Function to act as a client and send ping messages to a target server
func UdpClient(targetAddr string) {
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