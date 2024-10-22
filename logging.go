package main

import (
    "fmt"
    "os"
    "distributed_system/tcp"
    "distributed_system/udp"
    "github.com/joho/godotenv"
    "bufio"
    "strings"
    "time"
    "log"
)

var addr string = os.Getenv("MACHINE_ADDRESS")
var stopPing chan bool
var suspicionEnabled bool = true

func main() {

    err := godotenv.Load(".env")

    // clear the output file 
    file, err := os.OpenFile("output.txt", os.O_WRONLY|os.O_TRUNC, 0644)
    if err != nil {
        fmt.Println("Error opening file: ", err)
        return
    }
    defer file.Close()

    // clear the logging file 
    file, err = os.OpenFile(os.Getenv("LOG_FILENAME"), os.O_WRONLY|os.O_TRUNC, 0644)
    if err != nil {
        fmt.Println("Error opening file: ", err)
        return
    }
    defer file.Close()


    if _, err := os.Stat("file-store"); os.IsNotExist(err) {
		err := os.Mkdir("file-store", 0755)
		if err != nil {
			log.Fatalf("Failed to create directory: %v", err)
		}
	} else {
		err := os.RemoveAll("file-store")
		if err != nil {
			log.Fatalf("Failed to remove directory contents: %v", err)
		}
		err = os.Mkdir("file-store", 0755)
		if err != nil {
			log.Fatalf("Failed to recreate directory: %v", err)
		}
	}




    // check whether it's a server (receiver) or client (sender)
    if len(os.Args) > 1 && os.Args[1] == "client" { // run client
        grep := os.Args[2]
        tcp.TcpClient(grep)
    } else { 

        //run server
        go tcp.TcpServer()
        go udp.UdpServer()
        commandLoop()

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
				time.Sleep(1 * time.Second)
				udp.PingClient(suspicionEnabled)
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
        args := strings.Fields(command)

        switch args[0] {
            case "grep":   
                if len(args) < 2 {
                    fmt.Println("Error: Missing pattern parameter. Usage: grep <pattern>")
                    continue
                }
                pattern := args[1]
                fmt.Println(pattern)
                tcp.TcpClient(pattern)

            case "get":
                hydfs_file := args[1]
                local_file := args[2]
                tcp.GetFile(hydfs_file, local_file)

            case "join":
                udp.JoinSystem(addr)
        
                // Start pinging if joining the system
                startPinging()
            case "list_ring":
                ring_map := udp.GetRing()
                go udp.ListRing(ring_map)

            case "list_mem":
                membership_list := udp.GetMembershipList()
                go udp.ListMem(membership_list)
            
            case "store":
                go udp.PrintFiles("file-store")
        
            case "leave":
                // Send a signal to stop the ping loop
                if stopPing != nil {
                    close(stopPing) // Close the stopPing channel to stop the ping loop
                }
                go udp.LeaveList()
            case "enable_sus":
                // Toggle suspicion flag
                suspicionEnabled = true
                fmt.Println(suspicionEnabled)
        
            case "disable_sus":
                // Disable suspicion mechanism
                suspicionEnabled = false
            
            case "status_sus":
                if suspicionEnabled {
                    fmt.Println("Suspicion enabled")
                } else {
                    fmt.Println("Suspicion disabled")
                }
            
            case "list_sus":
                sus_list := udp.FindSusMachines()
                go udp.ListMem(sus_list)

            default:
                fmt.Println("Unknown command. Available commands: list_mem, list_self, join, leave")
            }
    }
}