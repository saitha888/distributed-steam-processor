
package main

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
)

//global variable for port (different on each machine)
var port string = "8080"

func main() {
	// check whether it's a server (receiver) or client (sender)
	if len(os.Args) > 1 && os.Args[1] == "client" { // run client
		grep := strings.Join(os.Args[2:], " ")
		client(grep)
	} else { // run server
		server(port)
	}
}

func server(port string) {
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
	command = command + " machine.1.log"

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
	// have a list of ports for other machines
	ports := []string{"8080", "8081", "8082", "8083", "8084", "8085", "8086", "8087", "8088", "8089"}
	// loop through all other machines
	for i := 0; i < len(ports); i++ {
        // connect to each machine and send grep command
		sendCommand(ports[i], grep)
    }
}

func sendCommand(port string, message string) {
	// conect to the port
    conn, err := net.Dial("tcp", "localhost:"+port)
    if err != nil {
        fmt.Println(err)
        return
    }
    defer conn.Close()

	// send the grep command to the machine
    conn.Write([]byte(message))

	// get the response from the machine
    buf := make([]byte, 1024)
    n, _ := conn.Read(buf)
	fmt.Println(string(buff[:n]))
}