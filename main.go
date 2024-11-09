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
    "distributed_system/test_scripts"
)


var addr string = os.Getenv("MACHINE_UDP_ADDRESS")
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

    // clear the membership logging file 
    file, err = os.OpenFile(os.Getenv("MEMBERSHIP_FILENAME"), os.O_WRONLY|os.O_TRUNC, 0644)
    if err != nil {
        fmt.Println("Error opening file: ", err)
        return
    }
    defer file.Close()

    // clear the hydfs logging file 
    file, err = os.OpenFile(os.Getenv("HDYFS_FILENAME"), os.O_WRONLY|os.O_TRUNC, 0644)
    if err != nil {
        fmt.Println("Error opening file: ", err)
        return
    }
    defer file.Close()

    // create/clear file store
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

    // create/clear cache
    if _, err := os.Stat("cache"); os.IsNotExist(err) {
		err := os.Mkdir("cache", 0755)
		if err != nil {
			log.Fatalf("Failed to create directory: %v", err)
		}
	} else {
		err := os.RemoveAll("cache")
		if err != nil {
			log.Fatalf("Failed to remove directory contents: %v", err)
		}
		err = os.Mkdir("cache", 0755)
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

            case "list_mem_ids":
                membership_list := udp.GetMembershipList()
                go udp.ListMemRing(membership_list)
            
            case "list_mem":
                membership_list := udp.GetMembershipList()
                go udp.ListMem(membership_list)
        
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
            
            case "create":
                localfilename := args[1]
                HyDFSfilename := args[2]
                tcp.CreateFile(localfilename, HyDFSfilename)
            
            case "ls":
                HyDFSfilename := args[1]
                udp.ListServers(HyDFSfilename)
            
            case "store":
                udp.ListStore()

            case "append":
                local_file := args[1]
                hydfs_file := args[2]
                tcp.AppendFile(local_file, hydfs_file)
            
            case "multiappend":
                hydfs_file := args[1]
                vms := []string{}
                local_files := []string{}
                for _,param := range args[2:] {
                    if len(param) >=10 && param[:10] == "fa24-cs425" {
                        vms = append(vms, param)
                    } else {
                        local_files = append(local_files, param)
                    }
                }
                tcp.MultiAppend(hydfs_file, vms, local_files)
            
            case "getfromreplica":
                VMaddress := args[1]
                HyDFSfilename := args[2]
                localfilename := args[3]
                tcp.GetFromReplica(VMaddress, HyDFSfilename, localfilename)
            
            case "test1":
                file1 := args[1]
                file2 := args[2]
                file3 := args[3]
                file4 := args[4]
                file5 := args[5]
                filenames := [5]string{file1, file2, file3, file4, file5}
                test_scripts.Test1(filenames)

            case "test4":
                test_scripts.Test4()

            case "merge":
                hydfs_file := args[1]
                tcp.Merge(hydfs_file)

            default:
                fmt.Println("Unknown command. Available commands: list_mem, list_self, join,  leave")
            }
    }
}