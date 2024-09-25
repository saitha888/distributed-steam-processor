package main

import (
    "fmt"
    "os"
    "distributed_system/tcp"
    "distributed_system/udp"
)

//global variable for port. machine number, log file name (different on each machine)

var udp_port string = "9092"
var machineNumber int = 2
var filename string = "machine.1.log"



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
        tcp.TcpClient(grep)
    } else { 

        //run server
        go tcp.TcpServer()
        go udp.UdpServer(udp_port)
        go udp.UdpClient("fa24-cs425-1206.cs.illinois.edu:9091")

        fmt.Println("\n")
        select {}
        // for {
        //     displayMembershipList()
        //     time.Sleep(2 * time.Second) // Update every 5 seconds
        //     clearPrintedLines(3)
        // }
    }
}

// // Display the membership list
// func displayMembershipList() {
//     fmt.Println("Membership List for Machine")
//     fmt.Println(" IP                                     | Status")
//     for i := 0; i < len(ports); i++ {
//         if tcp_port != ports[i][len(ports[i])-4:]{
//             fmt.Printf("%s    | alive \n", ports[i])
//         }
//     }
// }


// func clearPrintedLines(lines int) {
//     for i := 0; i < lines; i++ {
//         fmt.Print("\033[A\r\033[K") // Move up, go to start of line, and clear line
//     }
// }


