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
        client(grep)
    } else { 

        //run server
        go tcpServer()
        go udpServer()
        go udpClient("fa24-cs425-1202.cs.illinois.edu:9092")

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


//starts tcp server that listens for grep commands
func tcpServer() {

    // listen for connection from other machine 
    ln, err := net.Listen("tcp", ":" + tcp_port)
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

//starts ucp server that listens for pings
func udpServer() {
    addr, err := net.ResolveUDPAddr("udp", ":"+udp_port)
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

// Function to act as a client and send ping messages to a target server
func udpClient(targetAddr string) {
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


//handler of any incoming connection from other machines
func handleConnection(conn net.Conn) {

    // Close the connection when we're done
    defer conn.Close()

    // Get the connection message
    buf := make([]byte, 1024)
    n, _ := conn.Read(buf)
    message := string(buf[:n])

    // Check if message is a grep command
    if message[:4] == "grep" {
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

    // Check if message is a client call (for testing)
    } else if message[:6] == "client" {
        totalLines := client(message[7:])
        conn.Write([]byte(strconv.Itoa(totalLines)))

    //if not grep or command call, must be call to create a log file
    } else { 

        // Open the file to write the contents
        file, err := os.Create(filename)
        if err != nil {
            fmt.Println(err)
            return
        }
        defer file.Close()

        // Write initial chunk to the file
        _, err = file.Write(buf[:n])
        if err != nil {
            fmt.Println(err)
            return
        }

        // Read from the connection in chunks and write to the file
        for {
            n, err := conn.Read(buf)

            if err != nil {

                //break once at the end of the buffer
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

//goes through lists of ports, runs the grep command, prints aggregated results
func client(pattern string) int {

    // Start the timer
    start := time.Now()

    //Array for printing out machine line counts at the end
    linesArr := []string{}

    totalLines := 0

    // loop through all other machines
    for i := 0; i < len(ports); i++ {

        // check if we're on initial machine
        if i == machineNumber - 1 {
            
            //if on initial machine, run grep commands on its log files
            //first grep command for printing matching lines
            command := "grep -nH " + pattern + " " + filename
            cmd := exec.Command("sh", "-c", command)
            output, err := cmd.CombinedOutput()
            if err != nil {
                fmt.Println("Error converting output to int:", err)
                continue
            }

            //second grep command for printing matching line counts
            command2 := "grep -c " + pattern + " " + filename
            cmd2 := exec.Command("sh", "-c", command2)
            output2, err2 := cmd2.CombinedOutput()
            if err2 != nil {
                fmt.Println(err2)
                continue 
            }

            //converts line grep command into int
            lineStr := strings.TrimSpace(string(output2))
            selfLineCount, err3 := strconv.Atoi(lineStr)
            if err3 != nil {
                fmt.Println(err3)
                continue 
            }

            //append line counts for initial
            lineStr = fmt.Sprintf("Machine %s: %d", ports[i][13:15], selfLineCount) + "\n"
            linesArr = append(linesArr, lineStr)

            totalLines += selfLineCount
            
            // write the command to an output file
            file, err := os.OpenFile("output.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
            if err != nil {
                fmt.Println(err)
                continue
            }
            defer file.Close()

            _, err = file.Write(output)
            if err != nil {
                fmt.Println(err)
                continue
            }

        //case for connecting to other machines and running grep command
        } else {
        
        grep_response := "grep -nH " + pattern
        grep_count := "grep -c " + pattern

        // connect to machine and send grep command
        sendCommand(ports[i], grep_response)

        //connect to machine and send grep line command
        lineCount := sendLineCommand(ports[i], grep_count)
        
        //append line counts for connected machine
        lineStr := fmt.Sprintf("Machine %s: %d", ports[i][13:15], lineCount) + "\n"
        linesArr = append(linesArr, lineStr)

        totalLines += lineCount
        }
    }

    //at end of query results, print out line counts for each machine, total line count
    fmt.Print(linesArr)
    fmt.Print("\n\n")
    fmt.Print("Total line count: " + strconv.Itoa(totalLines) + "\n\n\n")

    // stop the timer
    elapsed := time.Since(start)

    // output how long the process took
    fmt.Printf("Grep command took %s to complete.\n", elapsed)
    fmt.Println()
    return totalLines
}

//handler of grep query
func sendCommand(port string, message string) {

    // connect to the port
    conn, err := net.Dial("tcp", port)
    if err != nil {
        fmt.Println(err)
        return
    }
    defer conn.Close()

    // send the grep command to the machine
    conn.Write([]byte(message))

    // write the command to an output file
    file, err := os.OpenFile("output.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
    if err != nil {
        fmt.Println(err)
        return
    }
    defer file.Close()

    _, err = io.Copy(file, conn)
    if err != nil {
        fmt.Println(err)
        return
    }
}

//handler of grep line query
func sendLineCommand(port string, message string) int {
    // connect to the port
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

    // convert the response into int and return it back to client
    lineCountStr := string(buf[:n])
    lineCountStr = strings.TrimSpace(lineCountStr)
    lineCount, err := strconv.Atoi(lineCountStr)
    if err != nil {
        fmt.Printf("Error converting line count from %s to int: %v\n", port, err)
        return 0
    }
    return lineCount
}

