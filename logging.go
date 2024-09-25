package main

import (
    "fmt"
    "net"
    "os"
    "os/exec"
    "strings"
    "io"
    "strconv"
    "time"
    "distributed_system/udp"
    "distributed_system/tcp"
)

//global variable for port. machine number, log file name (different on each machine)
var tcp_port string = "8081"
var udp_port string = "9091"
var machineNumber int = 1
var filename string = "machine.1.log"

// have a list of addresses for other machines
var ports = []string{
    "fa24-cs425-1201.cs.illinois.edu:8081", 
    "fa24-cs425-1202.cs.illinois.edu:8082", 
    // "fa24-cs425-1203.cs.illinois.edu:8083", 
    // "fa24-cs425-1204.cs.illinois.edu:8084", 
    // "fa24-cs425-1205.cs.illinois.edu:8085", 
    // "fa24-cs425-1206.cs.illinois.edu:8086", 
    // "fa24-cs425-1207.cs.illinois.edu:8087", 
    // "fa24-cs425-1208.cs.illinois.edu:8088", 
    // "fa24-cs425-1209.cs.illinois.edu:8089",
    // "fa24-cs425-1210.cs.illinois.edu:8080",
}

func main() {

    // clear the output file 
    file, err := os.OpenFile("output.txt", os.O_WRONLY|os.O_TRUNC, 0644)
    if err != nil {
        fmt.Println("Error opening file:", err)
        return
    }
    defer file.Close()

    // check whether it's a server (receiver) or client (sender)
    if len(os.Args) > 1 && os.Args[1] == "client" { // run client
        grep := os.Args[2]
        TcpClient(grep)
    } else { 

        //run server
        go tcpServer()
        go UdpServer(udp_port)
        go UdpClient("fa24-cs425-1202.cs.illinois.edu:9092")

        fmt.Println("\n")
        select {}
        // for {
        //     displayMembershipList()
        //     time.Sleep(2 * time.Second) // Update every 5 seconds
        //     clearPrintedLines(3)
        // }
    }
}

// Display the membership list
func displayMembershipList() {
    fmt.Println("Membership List for Machine")
    fmt.Println(" IP                                     | Status")
    for i := 0; i < len(ports); i++ {
        if tcp_port != ports[i][len(ports[i])-4:]{
            fmt.Printf("%s    | alive \n", ports[i])
        }
    }
}


func clearPrintedLines(lines int) {
    for i := 0; i < lines; i++ {
        fmt.Print("\033[A\r\033[K") // Move up, go to start of line, and clear line
    }
}


