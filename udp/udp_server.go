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

//starts udp server that listens for pings
func UdpServer(port string) {
    addr, err := net.ResolveUDPAddr("udp", ":"+port)
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

