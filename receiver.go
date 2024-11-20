package main

import (
	"encoding/json"
	"fmt"
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
		fmt.Println("Error decoding received data:", err)
		return
	}

	// Print the received structure
	fmt.Printf("Received: %+v\n", receivedData)

	// Read the contents of the file
	filePath := "10mb_file.txt"
	fileContent, err := os.ReadFile(filePath)
	if err != nil {
		fmt.Println("Error reading file:", err)
		return
	}

	// Create a new struct with the file content as the Email field
	responseStruct := MyStruct{
		ID:    receivedData.ID,
		Name:  "Server Response",
		Email: string(fileContent), // File content becomes the Email field
	}

	// Encode the new struct into JSON and send it back
	encoder := json.NewEncoder(conn)
	err = encoder.Encode(responseStruct)
	if err != nil {
		fmt.Println("Error encoding response struct:", err)
		return
	}

	fmt.Println("Response struct sent successfully.")
}
