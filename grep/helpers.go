package grep 

import (
    "fmt"
    "net"
    "os"
    "strings"
    "io"
    "strconv"
)

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