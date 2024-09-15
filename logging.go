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
)

//global variable for port. machine number, log file name (different on each machine)
var port string = "8086"
var machineNumber int = 6
var filename string = "machine.6.log"

func main() {
    // check whether it's a server (receiver) or client (sender)
    if len(os.Args) > 1 && os.Args[1] == "client" { // run client
        grep := os.Args[2]
        print("patten: ", grep)
        client(grep)
    } else { // run server
        server()
    }
}

func server() {
    // listen for connection from other machine 
    ln, err := net.Listen("tcp", ":" + port)
    if err != nil {
        fmt.Println(err)
        return
    }

    // run subroutine to handle the connection
    for {
        conn, err := ln.Accept()
        if err != nil {
            fmt.Println(err)
            continue
        }

        // Handle the connection in a go routine
        go handleConnection(conn)
    }
}

func handleConnection(conn net.Conn) {
    // Close the connection when we're done
    defer conn.Close()

    // Get the grep command
    buf := make([]byte, 1024)
    n, _ := conn.Read(buf)
    message := string(buf[:n])

    if message[:4] == "grep" { // recieving a grep command
        grep := message + " " + filename
        // run the grep command on machine
        cmd := exec.Command("sh", "-c", grep)
        output, err := cmd.CombinedOutput()
        
        if err != nil {
            fmt.Println(err)
            return
        }
        
        // send the result back to the initial machine
        conn.Write(output)
    } else if message[:5] == "client" {
		totalLines := client("grep " + message[6:])
		conn.Write([]byte(strconv.Itoa(totalLines)))
	} else { // recieving a message to generate log file
        // Open the file to write the contents
        file, err := os.Create(filename)
        if err != nil {
            fmt.Println(err)
            return
        }
        defer file.Close()

        // Write initial  chunk to the file
        _, err = file.Write(buf[:n])
        if err != nil {
            fmt.Println(err)
            return
        }

        // Read from the connection in chunks and write to the file
        for {
            n, err := conn.Read(buf)
            if err != nil {
                if err == io.EOF {
                    break
                }
                fmt.Println(err)
                return 
            }

            // Write the chunk to the file
            _, err = file.Write(buf[:n])
            if err != nil {
                fmt.Println(err)
                return
            }
        }
    }
}

func client(pattern string) int {
    // Start the timer
    start := time.Now()

    // have a list of addresses for other machines
    ports := []string{
                        "fa24-cs425-1201.cs.illinois.edu:8081", 
                        "fa24-cs425-1202.cs.illinois.edu:8082", 
                        "fa24-cs425-1203.cs.illinois.edu:8083", 
                        "fa24-cs425-1204.cs.illinois.edu:8084", 
                        "fa24-cs425-1205.cs.illinois.edu:8085", 
                        "fa24-cs425-1206.cs.illinois.edu:8086", 
                        "fa24-cs425-1207.cs.illinois.edu:8087", 
                        "fa24-cs425-1208.cs.illinois.edu:8088", 
                        "fa24-cs425-1209.cs.illinois.edu:8089",
                        "fa24-cs425-1210.cs.illinois.edu:8080",
                    }
    totalLines := 0
    // loop through all other machines
    for i := 0; i < len(ports); i++ {
        // check if we're on initial machine
        if i == machineNumber - 1 {
            // fmt.Println("here")
            command := "grep -nH " + pattern + " " + filename
            cmd := exec.Command("sh", "-c", command)
            output, err := cmd.CombinedOutput()
            
            command2 := "grep -c " + pattern + " " + filename
            cmd2 := exec.Command("sh", "-c", command2)
            output2, err2 := cmd2.CombinedOutput()

            if err != nil {
                fmt.Println(err)
                return 0
            }
            if err2 != nil {
                fmt.Println(err2)
                return 0 
            }
            fmt.Print(string(output))
            fmt.Print(string(output2))
            //FIX LINE COUNT OF SELF MACHINE
            // c, e := strconv.Atoi(output2)

            // if e != nil {
            //     fmt.Println(e)
            //     return
            // }
        }
        grep_response := "grep -nH " + pattern
        grep_count := "grep -c " + pattern
        //lineGrep := "grep -c" + pattern + " " + filename


        // connect to another machine and send grep command
        sendCommand(ports[i], grep_response)
        //connect to another machine and send grep line command
        lineCount := sendLineCommand(ports[i], grep_count)

        totalLines += lineCount
    }
    //Print total lines
    fmt.Print("Total line count: " + strconv.Itoa(totalLines) + "\n\n\n")

    // Stop the timer
    elapsed := time.Since(start)

    // Output how long the command took
    fmt.Printf("Grep command took %s to complete.\n", elapsed)
    fmt.Println()
    return totalLines
}

func sendCommand(port string, message string) {
    // conect to the port
    conn, err := net.Dial("tcp", port)
    if err != nil {
        fmt.Println(err)
        return
    }
    defer conn.Close()

    // send the grep command to the machine
    conn.Write([]byte(message))

    // get the response from the machine
    buf := make([]byte, 1024)
    // loop through whole response and print it till it reaches the end
    for {
        n, err := conn.Read(buf)
        if err != nil {
            if err == io.EOF {

                break 
            }
            return
        }
        fmt.Print(string(buf[:n])) 
    }
}

func sendLineCommand(port string, message string) int {
    // conect to the port
    conn, err := net.Dial("tcp", port)
    if err != nil {
        fmt.Println(err)
        return 0
    }
    defer conn.Close()

    // send the grep command to the machine
    conn.Write([]byte(message))

    // get the response from the machine
    buf := make([]byte, 1024)
    n, err := conn.Read(buf)
    // loop through whole response and print it till it reaches the end
    // for {
    //     n, err := conn.Read(buf)
    //     if err != nil {
    //         if err == io.EOF {
    //             fmt.Println(err)
    //             break 
    //         }
    //         return
    //     }
    //     fmt.Print(string(buf[:n])) 
    // }
    fmt.Printf("Line count from machine: ")
    fmt.Printf(string(buf[:n]) + "\n\n")
    lineCountStr := string(buf[:n])
    lineCountStr = strings.TrimSpace(lineCountStr)
    lineCount, err := strconv.Atoi(lineCountStr)
    if err != nil {
        fmt.Printf("Error converting line count from %s to int: %v\n", port, err)
        return 0
    }
    return lineCount
}
