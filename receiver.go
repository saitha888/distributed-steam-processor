package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
)

type MyStruct struct {
	ID    int
	Name  string
	Email string
}

func main() {
	// Start the server
	listener, err := net.Listen("tcp", ":"+"8088")
	if err != nil {
		panic(err)
	}
	defer listener.Close()

	println("Server is listening on port 8088...")

	for {
		conn, err := listener.Accept()
		if err != nil {
			panic(err)
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	// Decode the received JSON into the structure
	var receivedData MyStruct
	decoder := json.NewDecoder(conn)
	err := decoder.Decode(&receivedData)
	if err != nil {
		panic(err)
	}

	// Print the received structure
	fmt.Printf("Received: %+v\n", receivedData)

	// Specify the file to send back
	filePath := "10mb_file.txt" // Change this to the file you want to send
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	// Send the file contents back to the client
	_, err = io.Copy(conn, file)
	if err != nil {
		fmt.Println("Error sending file:", err)
		return
	}

	fmt.Println("File sent successfully.")
}
