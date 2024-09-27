package main

import (
    "fmt"
    "os"
    "distributed_system/tcp"
    "distributed_system/udp"
    "github.com/joho/godotenv"
    "bufio"
    "time"
)

var addr string = os.Getenv("MACHINE_ADDRESS")
var stopPing chan bool

func main() {

    err := godotenv.Load(".env")

    // clear the output file 
    file, err := os.OpenFile("output.txt", os.O_WRONLY|os.O_TRUNC, 0644)
    if err != nil {
        fmt.Println("Error opening file: ", err)
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
        go udp.UdpServer()
        commandLoop()
        
        // go udp.PingClient()


        select {}

    }
}

func startPinging() {
	// Initialize or reset the stop channel for a new pinging session
	stopPing = make(chan bool)

	// Start the ping loop in a Goroutine
	go func() {
		for {
			select {
			case <-stopPing: // Check if a signal to stop the loop is received
				fmt.Println("Stopping PingClient...")
				return
			default:
				// Sleep and then ping a random node
				time.Sleep(2 * time.Second)
				udp.PingClient()
			}
		}
	}()
}

func commandLoop() {
    scanner := bufio.NewScanner(os.Stdin)
    for {
        fmt.Print("> ") // CLI prompt
        scanner.Scan()
        command := scanner.Text()

        switch command {

        case "join":
            go udp.JoinSystem(addr)
    
            // Start pinging if joining the system
            startPinging()
    
        case "list_mem":
            go udp.ListMem()
    
        case "leave":
            // Send a signal to stop the ping loop
            if stopPing != nil {
                close(stopPing) // Close the stopPing channel to stop the ping loop
            }
            go udp.LeaveList()

        default:
            fmt.Println("Unknown command. Available commands: list_mem, list_self, join, leave")
        }
    }
}