package main

import (
    "fmt"
    "net"
    "io"
    "math/rand"
    "time"
    "strings"
    "testing"
)

var ports = map[int]string{
    // // 1: "fa24-cs425-1201.cs.illinois.edu:8081",
    // 2: "fa24-cs425-1202.cs.illinois.edu:8082",
    // // 3: "fa24-cs425-1203.cs.illinois.edu:8083",
    // // 4: "fa24-cs425-1204.cs.illinois.edu:8084",
    // // 5: "fa24-cs425-1205.cs.illinois.edu:8085",
    6: "fa24-cs425-1206.cs.illinois.edu:8086",
    // 7: "fa24-cs425-1207.cs.illinois.edu:8087",
    // 8: "fa24-cs425-1208.cs.illinois.edu:8088",
    // 9: "fa24-cs425-1209.cs.illinois.edu:8089",
    // 10: "fa24-cs425-1210.cs.illinois.edu:8080",
}

func main() {
    sendLogFiles()
    output := sendCommand("fa24-cs425-1206.cs.illinois.edu:8086", "grep GET")
    fmt.Printf(output)
    // testAll()
}

func sendLogFiles() {

    // Loop through all machines in the ports map
    for machineNumber, address := range ports {
        // Connect to the machine's server
        conn, err := net.Dial("tcp", address)
        if err != nil {
            fmt.Printf("Error connecting to machine %d at %s: %v\n", machineNumber, address, err)
            continue
        }
        defer conn.Close()


        content:= generate_file_contents(100)
        // Send a message indicating a log file is being transferred
        conn.Write([]byte(content))
        fmt.Printf("Log file '%s' sent to machine %d\n", logFileName, machineNumber)
    }
}

func sendCommand(port string, message string) string {
    // conect to the port
    conn, err := net.Dial("tcp", port)
    if err != nil {
        fmt.Println(err)
        return ""
    }
    defer conn.Close()

    // send the grep command to the machine
    conn.Write([]byte(message))

    var result strings.Builder
    buf := make([]byte, 1024)

    // Read the response and append it to the result string
    for {
        n, err := conn.Read(buf)
        if err != nil {
            if err == io.EOF {
                break
            }
            return ""
        }
        result.Write(buf[:n]) // Append the received data to the result
    }

    return result.String()
}

func generate_file_contents(lines int) string {

    // random number generator
    rand.Seed(time.Now().UnixNano())

    // list of endpoint choices
    endpointChoices := []string{"GET", "PUT", "POST", "DELETE"}

    // list of filename choices
    filenames := []string{"/wp-admin","/wp-content", "/search/tag/list", "/app/main/posts", "/list", "/explore", "/posts/posts/explore"}

    output := ""

    // create line_amount number of random lines
    for i := 0; i <= lines; i++ {
        // random ip address
        ipAddress := fmt.Sprintf("%d.%d.%d.%d", rand.Intn(256), rand.Intn(256), rand.Intn(256), rand.Intn(256))

        // random date
        start := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC) // Start date: Jan 1, 2000
        end := time.Now()
        randomDuration := time.Duration(rand.Int63n(end.Unix() - start.Unix())) * time.Second
        randomDate := start.Add(randomDuration)
        date := randomDate.Format("2006-01-02")

        // endpoint type 
        randomIndex1 := rand.Intn(len(endpointChoices))
        endpoint := endpointChoices[randomIndex1]

        // random file name
        randomIndex2 := rand.Intn(len(filenames))
        filename := filenames[randomIndex2]

        contents := ipAddress +  " - - " + " [" + date + "] " + endpoint + " " + filename + " HTTP/1.0" + "\n"
        
        output += contents
    }
    return output
}

type Test struct {
    Name          string
    GrepCommand   string
    LogContents   string
    ExpectedOutput string
}

testSet := []Test{
    {
        Name:          "Test 1: Search for error",
        GrepCommand:   "grep 'ERROR'",
        LogContents:   "INFO: Starting service\nERROR: Something went wrong\nINFO: Service started",
        ExpectedOutput: "ERROR: Something went wrong",
    },
    {
        Name:          "Test 2: Search for info",
        GrepCommand:   "grep 'INFO'",
        LogContents:   "INFO: Starting service\nERROR: Something went wrong\nINFO: Service started",
        ExpectedOutput: "INFO: Starting service\nINFO: Service started",
    },
}


testAll() {
    for test := range testSet {
        output = sendCommand(test.GrepCommand)
        if output != test.ExpectedOutput {
            t.Errorf("Error with %s: Expected %s, but got %s", test.Name, expected, test.ExpectedOutput) // Fails the test with a formatted error message
        }
    }
}
