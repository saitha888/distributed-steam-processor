package main

import (
	"encoding/json"
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
	// Read the contents of the file
	filePath := "10mb_file.txt"
	fileContent, err := os.ReadFile(filePath)
	if err != nil {
		panic(err)
	}

	// Convert the file content to a string and assign it to the Email field
	data := MyStruct{
		ID:    1,
		Name:  "John Doe",
		Email: string(fileContent),
	}

	// Connect to the server
	conn, err := net.Dial("tcp", "fa24-cs425-1208.cs.illinois.edu:8088")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	// Encode the structure into JSON
	encoder := json.NewEncoder(conn)
	err = encoder.Encode(data)
	if err != nil {
		panic(err)
	}

	println("Structure sent successfully.")

	// Open a file to save the response
	outFile, err := os.Create("response_file.txt") // Save the response in a file
	if err != nil {
		panic(err)
	}
	defer outFile.Close()

	// Receive and save the server's response
	_, err = io.Copy(outFile, conn)
	if err != nil {
		panic(err)
	}

	println("Response received and saved to 'response_file.txt'.")
}
