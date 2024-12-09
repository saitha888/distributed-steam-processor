package main

import (
    "fmt"
    "os"
    "distributed_system/servers"
    "distributed_system/global"
    "distributed_system/util"
    "distributed_system/membership"
    "distributed_system/hydfs"
    "distributed_system/grep"
    "github.com/joho/godotenv"
    "bufio"
    "time"
    "log"
    "distributed_system/scripts"
    "distributed_system/rainstorm"
    "strconv"
)


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
    file, err = os.OpenFile(global.Membership_log, os.O_WRONLY|os.O_TRUNC, 0644)
    if err != nil {
        fmt.Println("Error opening file: ", err)
        return
    }
    defer file.Close()

    // clear the hydfs logging file 
    file, err = os.OpenFile(global.Hydfs_log, os.O_WRONLY|os.O_TRUNC, 0644)
    if err != nil {
        fmt.Println("Error opening file: ", err)
        return
    }
    defer file.Close()

    // clear the counts file 
    file, err = os.OpenFile("counts.txt", os.O_WRONLY|os.O_TRUNC, 0644)
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

    // create/clear temp
    if _, err := os.Stat("temp"); os.IsNotExist(err) {
        err := os.Mkdir("temp", 0755)
        if err != nil {
            log.Fatalf("Failed to create directory: %v", err)
        }
    } else {
        err := os.RemoveAll("temp")
        if err != nil {
            log.Fatalf("Failed to remove directory contents: %v", err)
        }
        err = os.Mkdir("temp", 0755)
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
        grep_command := os.Args[2]
        filename := os.Args[3]
        grep.GrepClient(grep_command, filename)
    } else { 
        //run server
        go servers.TcpServer()
        go servers.UdpServer()
        go servers.RainstormServer()
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
				membership.PingClient(suspicionEnabled)
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
        args := util.ParseArguments(command)

        switch args[0] {
            case "grep":   
                if len(args) < 2 {
                    fmt.Println("Error: Missing pattern parameter. Usage: grep <pattern> <filename>")
                    continue
                }
                pattern := args[1]
                filename := args[2]
                grep.GrepClient(pattern, filename)

            case "get":
                hydfs_file := args[1]
                local_file := args[2]
                hydfs.GetFile(hydfs_file, local_file)
                fmt.Println(hydfs_file+ " retrieved and written to " + local_file)

            case "join":
                membership.JoinSystem(global.Udp_address)
        
                // Start pinging if joining the system
                startPinging()
            case "list_ring":
                go hydfs.ListRing(global.Ring_map)

            case "list_mem_ids":
                go hydfs.ListMemRing(global.Membership_list)
            
            case "list_mem":
                go hydfs.ListMem(global.Membership_list)
        
            case "leave":
                // Send a signal to stop the ping loop
                if stopPing != nil {
                    close(stopPing) // Close the stopPing channel to stop the ping loop
                }
                go membership.LeaveList()
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
                sus_list := util.FindSusMachines()
                go hydfs.ListMem(sus_list)
            
            case "create":
                localfilename := args[1]
                localfilename = "local-files/" + localfilename
                HyDFSfilename := args[2]
                hydfs.CreateFile(localfilename, HyDFSfilename)
                fmt.Println(HyDFSfilename +" Created")
            
            case "ls":
                HyDFSfilename := args[1]
                hydfs.ListServers(HyDFSfilename)
            
            case "store":
                util.ListStore()

            case "append":
                local_file := args[1]
                local_file = "local-files/" + local_file
                hydfs_file := args[2]
                hydfs.AppendFile(local_file, hydfs_file)
                fmt.Println(local_file + "added to" + hydfs_file )
            
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
                hydfs.MultiAppend(hydfs_file, vms, local_files)
            
            case "getfromreplica":
                VMaddress := args[1]
                HyDFSfilename := args[2]
                localfilename := args[3]
                hydfs.GetFromReplica(VMaddress, HyDFSfilename, localfilename)
            
            case "test1":
                file1 := args[1]
                file2 := args[2]
                file3 := args[3]
                file4 := args[4]
                file5 := args[5]
                filenames := [5]string{file1, file2, file3, file4, file5}
                scripts.TestCreate(filenames)

            case "test4":
                scripts.TestAppend()

            case "merge":
                hydfs_file := args[1]
                hydfs.Merge(hydfs_file)
                fmt.Println("Merging of " + hydfs_file + " complete")
            
            case "rainstorm":
                if len(args) < 8 {
                    print("Not enough args")
                } else {
                    //RainStorm <op1 _exe> <pattern> <op2 _exe> <stateful> <hydfs_src_file> <hydfs_dest_filename> <num_tasks>
                    params := map[string]string{
                        "op_1":      args[1],
                        "pattern":    args[2],
                        "op_2":      args[3],
                        "stateful": args[4],
                        "src_file":  args[5],
                        "dest_file": args[6],
                        "num_tasks": args[7],
                    }
                    hydfs.CreateFile("empty.txt", params["dest_file"])
                    rainstorm.CallRainstorm(params) 
                }
                

            case "kill2":
                if len(args) < 11 {
                    print("Not enough args")
                    continue
                } else {
                    machine1, _ := strconv.Atoi(args[2])
                    machine2, _ := strconv.Atoi(args[3])
                    params := map[string]string{
                        "op_1":      args[4],
                        "pattern":    args[5],
                        "op_2":      args[6],
                        "stateful": args[7],
                        "src_file":  args[8],
                        "dest_file": args[9],
                        "num_tasks": args[10],
                    }
                    hydfs.CreateFile("empty.txt", params["dest_file"])
                    rainstorm.CallRainstorm(params) 
                    scripts.KillVMS(machine1, machine2)
                }
                
            case "schedule":
                util.DisplaySchedule()
            
            case "merge-logs":
                rainstorm.MergeLogs()


            default:
                fmt.Println("Unknown command. Available commands: list_mem, list_self, join,  leave")
            }
    }
}