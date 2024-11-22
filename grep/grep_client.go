package grep 

import (
    "fmt"
    "net"
    "os"
    "os/exec"
    "strings"
    "io"
    "strconv"
    "time"
    "distributed_system/global"

)

var filename string = "thullu"
//goes through lists of ports, runs the grep command, prints aggregated results
func TcpClient(pattern string) int {

    // Start the timer
    start := time.Now()

    //Array for printing out machine line counts at the end
    linesArr := []string{}

    totalLines := 0

    // loop through all other machines
    for i := 0; i < len(global.Tcp_ports); i++ {

        machine_num, err := strconv.Atoi(global.Machine_number)
        if err != nil {
            fmt.Println("Error converting APP_PORT:", err)
        } 

        // check if we're on initial machine
        if i == machine_num - 1 {
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
            lineStr = fmt.Sprintf("Machine %s: %d", global.Tcp_ports[i][13:15], selfLineCount) + "\n"
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
            sendCommand(global.Tcp_ports[i], grep_response)

            //connect to machine and send grep line command
            lineCount := sendLineCommand(global.Tcp_ports[i], grep_count)
            
            //append line counts for connected machine
            lineStr := fmt.Sprintf("Machine %s: %d", global.Tcp_ports[i][13:15], lineCount) + "\n"
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