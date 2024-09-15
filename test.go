package main

import (
    "fmt"
    "net"
    "io"
    "math/rand"
    "time"
    "strings"
)
var ports = []string{
                        "fa24-cs425-1201.cs.illinois.edu:8081", 
						"fa24-cs425-1202.cs.illinois.edu:8082", 
						"fa24-cs425-1203.cs.illinois.edu:8083", 
						"fa24-cs425-1204.cs.illinois.edu:8084", 
						"fa24-cs425-1205.cs.illinois.edu:8085", 
						"fa24-cs425-1206.cs.illinois.edu:8086", 
						"fa24-cs425-1207.cs.illinois.edu:8087", 
						"fa24-cs425-1208.cs.illinois.edu:8088", 
						"fa24-cs425-1209.cs.illinois.edu:8089",
						"fa24-cs425-1210.cs.illinois.edu:8080"
                    }


type Test struct {
    testName string
    countCheck int
    patterns []string
}

func main() {
    fmt.Println("here")
    // testSet := []Test{
    //     {   
    //         testName: "Infrequent Pattern Test",
    //         countCheck: 500,
    //         patterns: []string{"/mango"},
    //     },
    //     {
    //         testName: "Frequent Pattern Test",
    //         countCheck: 1000000,
    //         patterns: []string{"/cherry"},
    //     },
    //     {
    //         testName: "RegEx Pattern Test",      
    //         countCheck: 1000000,
    //         patterns: []string{"/kiwi", "/cherry"},
    //     },
    //     {
    //         testName: "Occurs in One Machine Test",
    //         countCheck: 100000,
    //         patterns: []string{"/banana"},
    //     },
    //     {
    //         testName: "Occurs in Some Machine Test",
    //         countCheck: 50000,
    //         patterns: []string{"/apple"},
    //     },
    // }
    // testAll(testSet)
    testRare()
}

func sendLogFiles(content string, address string) {
    // Connect to the machine's server
    fmt.Println("in log files function")
    conn, err := net.Dial("tcp", address)
    if err != nil {
        fmt.Println(err)
    }
    defer conn.Close()
    fmt.Println("made connection")

    // write the file contents to the machine
    conn.Write([]byte(content))
}

func sendCommand(port string, message string) string {
    // conect to the port
    conn, err := net.Dial("tcp", port)
    if err != nil {
        fmt.Println(err)
        return ""
    }
    fmt.Println("no error connecting")
    defer conn.Close()

    // send the to the machine
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


func testRare() string{
    // file generation
    patterns := []string{"/mango"}
    pattern_count := 50
    total_count := 1500

    // grep command to send from machine
    grep := "client /mango"

    // generate files across all machines
    for i := 0; i < len(ports); i++ {
        fmt.Println("sending log files")
        sendLogFiles(generate_file_contents(patterns, pattern_count, total_count), ports[i])
    }
    // send grep command from machine
    totalLines := sendCommand("fa24-cs425-1207.cs.illinois.edu:8087", grep)
    fmt.Println(totalLines)
    return totalLines
}

func testFrequent(machine string) string{
    // file generation
    patterns := []string{"/cherry"}
    pattern_count := 100000
    total_count := 1500

    // grep command to send from machine
    grep := "/cherry"

    // generate files across all machines
    for i := 0; i < len(ports); i++ {
        sendLogFiles(generate_file_contents(patterns, pattern_count, total_count), ports[i])
    }
    // send grep command from machine
    return sendCommand(machine, grep)
}

func testRegex(machine string) string{
    // file generation
    patterns := []string{"/kiwi", "/cherry"}
    pattern_count := 100000
    total_count := 150000

    // grep command to send from machine
    grep := "/kiwi|/cherry"

    // generate files across all machines
    for i := 0; i < len(ports); i++ {
        sendLogFiles(generate_file_contents(patterns, pattern_count, total_count), ports[i])
    }
    // send grep command from machine
    return sendCommand(machine, grep)
}

func testOccursInOne(machine string) string{
    // file generation for one machine
    patterns := []string{"/banana"}
    pattern_count := 100000
    total_count := 150000

    // grep command to send from machine
    grep := "/banana"

    // generate files across all machines
    sendLogFiles(generate_file_contents(patterns, pattern_count, total_count), ports[0])
    for i := 1; i < len(ports); i++ {
        sendLogFiles(generate_file_contents(patterns, 0, total_count), ports[i])
    }

    // send grep command from machine
    return sendCommand(machine, grep)
}

func testOccursInSome(machine string) string{
    // file generation for one machine
    patterns := []string{"/apple"}
    pattern_count := 10000
    total_count := 150000

    // grep command to send from machine
    grep := "/apple"

    // generate files with pattern across half machines
    for i := 0; i < len(ports); i++ {
        if i % 2 == 0{
            sendLogFiles(generate_file_contents(patterns, pattern_count, total_count), ports[i])
        } else {
            sendLogFiles(generate_file_contents(patterns, 0, total_count), ports[i])
        }
        
    }

    // send grep command from machine
    return sendCommand(machine, grep)
}


// func testAll(testSet []Test) {

//     for i := 0; i < 5; i++ {
//         output := ""
//         if i == 0 {
//             output = testRare()
//         } else if i == 1 {
//             output = testFrequent(ports[m])
//         } else if i == 2 {
//             output = testRegex(ports[m])
//         } else if i == 3 {
//             output = testOccursInOne(ports[m])
//         } else {
//             output = testOccursInSome(ports[m])
//         }
        
//         if !checkCount(output, testSet[i].countCheck) || !checkPattern(output, testSet[i].patterns[0], 50){
//             fmt.Println("Test Failed: " + testSet[i].testName + " on machine " + ports[m])
//         }
//     }  
// }


// check functions for test cases
func checkCount(output string, countCheck int) bool {
    return strings.Count(output, "\n") == countCheck
}

// check that pattern is present in random lines
func checkPattern(output string, pattern string, lines_to_check int) bool {

    check_count := 0
    // array of lines
    lines_array := strings.Split(output, "\n")

    // random number generator
    rand.Seed(time.Now().UnixNano())

    for check_count < lines_to_check {
        // get random index
        ind := rand.Intn(len(lines_array))

        // check line
        if !strings.Contains(lines_array[ind], pattern) {
            return false
        }
        check_count += 1
    }

    return true
}



// helper functions for generating files 

func generate_date() string {
    start := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC) // Start date: Jan 1, 2000
    end := time.Now()
    randomDuration := time.Duration(rand.Int63n(end.Unix() - start.Unix())) * time.Second
    randomDate := start.Add(randomDuration)
    return randomDate.Format("2006-01-02")
}

func generate_file_contents(patterns []string, pattern_count int, total_count int) string {
    fmt.Println("generating file contents")
    // random number generator
    rand.Seed(time.Now().UnixNano())

    // list of endpoint choices
    endpointChoices := []string{"GET", "PUT", "POST", "DELETE"}

    // list of filename choices
    filenames := []string{"/wp-admin","/wp-content", "/search/tag/list", "/app/main/posts", "/list", "/explore", "/posts/posts/explore"}

    output := ""

    ratio := total_count / pattern_count
    curr_count := 0

    // create line_amount number of random lines
    for i := 0; i <= total_count; i++ {
        // random ip address
        ipAddress := fmt.Sprintf("%d.%d.%d.%d", rand.Intn(256), rand.Intn(256), rand.Intn(256), rand.Intn(256))

        // random date
        date := generate_date()

        // endpoint type 
        randomIndex := rand.Intn(len(endpointChoices))
        endpoint := endpointChoices[randomIndex]

        // random file name
        filename := ""
        if i % ratio == 0 && curr_count != pattern_count{ // generate from wanted patterns
            randomIndex = rand.Intn(len(patterns))
            filename = patterns[randomIndex]
            curr_count += 1
        } else { // generate from default patterns
            randomIndex = rand.Intn(len(filenames))
            filename = filenames[randomIndex]
        }

        contents := ipAddress +  " - - " + " [" + date + "] " + endpoint + " " + filename + " HTTP/1.0" + "\n"
        
        output += contents
    }
    fmt.Println("for loop one done")
    
    for curr_count < pattern_count {
        fmt.Println("for loop two")
        // random ip address
        ipAddress := fmt.Sprintf("%d.%d.%d.%d", rand.Intn(256), rand.Intn(256), rand.Intn(256), rand.Intn(256))

        // random date
        date := generate_date()

        // endpoint type 
        randomIndex1 := rand.Intn(len(endpointChoices))
        endpoint := endpointChoices[randomIndex1]

        // filename
        randomIndex := rand.Intn(len(patterns))
        filename := patterns[randomIndex]

        contents := ipAddress +  " - - " + " [" + date + "] " + endpoint + " " + filename + " HTTP/1.0" + "\n"
        
        output += contents
        curr_count += 1
    }
    fmt.Println("generated done")
    return output
}