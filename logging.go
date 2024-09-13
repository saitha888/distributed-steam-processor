
package main

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"io"
	"lines"
	"bufio"
)

//global variable for port. machine number, log file name (different on each machine)
var port string = "8086"
var machineNumber int = 6
var filename string = "machine.6.log"

func main() {
	// check whether it's a server (receiver) or client (sender)
	if len(os.Args) > 1 && os.Args[1] == "client" { // run client
		grep := strings.Join(os.Args[2:], " ")
		client(grep)
	} else if len(os.Args) > 1 && os.Args[1] == "test" {
		machineName := os.Args[2]
		fileContents := os.Args[3:]
		sendFileContents(machineName,fileContents)
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
    command := string(buf[:n])

	command = command + " " + filename

	// run the grep command on machine
	cmd := exec.Command("sh", "-c", command)
	output, err := cmd.CombinedOutput()
	
	if err != nil {
		fmt.Println(err)
		return
	}
	
	// send the result back to the initial machine
    conn.Write(output)
}

func client(grep string) {
	// have a list of addresses for other machines
	ports := []string{"fa24-cs425-1201.cs.illinois.edu:8081", 
						"fa24-cs425-1202.cs.illinois.edu:8082", 
						"fa24-cs425-1203.cs.illinois.edu:8083", 
						"fa24-cs425-1204.cs.illinois.edu:8084", 
						"fa24-cs425-1205.cs.illinois.edu:8085", 
						"fa24-cs425-1206.cs.illinois.edu:8086", 
						"fa24-cs425-1207.cs.illinois.edu:8087", 
						"fa24-cs425-1208.cs.illinois.edu:8088", 
						"fa24-cs425-1209.cs.illinois.edu:8089",
						"fa24-cs425-1210.cs.illinois.edu:8080"}
	// loop through all other machines
	for i := 0; i < len(ports); i++ {
        // check if we're on initial machine
		if i == machineNumber - 1 {
			fmt.Println("here")
			command := grep + " " + filename
			cmd := exec.Command("sh", "-c", command)
			output, err := cmd.CombinedOutput()
			
			if err != nil {
				fmt.Println(err)
				return
			}
			fmt.Print(string(output))
			return
			
		}
		// connect to another machine and send grep command
		sendCommand(ports[i], grep)
    }
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
				fmt.Println(err)
                break 
            }
            return
        }
        fmt.Print(string(buf[:n])) 
    }
}

func writeToFile(fileContents string) {
	file, err := os.OpenFile(filename)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer file.Close()

	// Create a new writer
	writer := bufio.NewWriter(file)

	// Iterate through the array and write each line
	for _, line := range lines {
		_, err := writer.WriteString(line + "\n") // Write each string with a newline
		if err != nil {
			fmt.Println(err)
			return
		}
	}
}

func sendFileContents(machine string, contents string) {
	conn, err := net.Dial("tcp", port)
    if err != nil {
        fmt.Println(err)
        return
    }
    defer conn.Close()

	// send the file contents to the machine
    conn.Write([]byte(message))
}