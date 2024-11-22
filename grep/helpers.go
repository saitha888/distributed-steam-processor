package grep 

import (
    "fmt"
    "net"
    "strings"
    "strconv"
    "distributed_system/global"
    "encoding/json"
    "distributed_system/util"
)

//handler of grep query
func sendCommand(port string, message string, filename string) {

    // connect to the port
    conn, err := net.Dial("tcp", port)
    if err != nil {
        fmt.Println(err)
        return
    }
    defer conn.Close()

	data := global.Message{
		Action: message,
		Filename: filename,
		FileContents: "",
	}

	// Encode the structure into JSON
	encoder := json.NewEncoder(conn)
	err = encoder.Encode(data)
	if err != nil {
		fmt.Println("Error encoding structure in get to json", err)
	}

    // write the command to an output file
    var response global.Message
    decoder := json.NewDecoder(conn)
    err = decoder.Decode(&response)
    if err != nil {
        fmt.Println("error sending grep command")
    }

    util.WriteToFile("output.txt", response.FileContents)
}

//handler of grep line query
func sendLineCommand(port string, message string, filename string) int {
    // connect to the port
    conn, err := net.Dial("tcp", port)
    if err != nil {
        fmt.Println(err)
        return 0
    }
    defer conn.Close()

    data := global.Message{
		Action: message,
		Filename: filename,
		FileContents: "",
	}

	// Encode the structure into JSON
	encoder := json.NewEncoder(conn)
	err = encoder.Encode(data)
	if err != nil {
		fmt.Println("Error encoding structure in get to json", err)
	}

    var response global.Message
    decoder := json.NewDecoder(conn)
    err = decoder.Decode(&response)
    if err != nil {
        fmt.Println("error sending grep command")
    }

    lineCountStr := strings.TrimSpace(response.FileContents)
    lineCount, err := strconv.Atoi(lineCountStr)
    if err != nil {
        fmt.Printf("Error converting line count from %s to int: %v\n", port, err)
        return 0
    }
    return lineCount
}