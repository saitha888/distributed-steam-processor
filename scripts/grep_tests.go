package scripts

import (
    "fmt"
    "net"
    "io"
    "math/rand"
    "time"
    "strings"
    "strconv"
)

// global variable for vm ports
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
						"fa24-cs425-1210.cs.illinois.edu:8080",
                    }


// structure to keep track of test cases
type Test struct {
    testName string
    countCheck int
    patterns []string
}

func main() {
    // structure of Tests to map tests to expected values
    testSet := []Test{
        {   
            testName: "Infrequent Pattern Test",
            countCheck: 500,
            patterns: []string{"/mango"},
        },
        {
            testName: "Frequent Pattern Test",
            countCheck: 5000,
            patterns: []string{"/cherry"},
        },
        {
            testName: "RegEx Pattern Test",      
            countCheck: 5000,
            patterns: []string{"/kiwi", "/cherry"},
        },
        {
            testName: "Occurs in One Machine Test",
            countCheck: 100,
            patterns: []string{"/banana"},
        },
        {
            testName: "Occurs in Some Machine Test",
            countCheck: 2500,
            patterns: []string{"/apple"},
        },
    }
    // run all test cases
    testAll(testSet)
}

// TEST CASE FUNCTIONS

// test case to test an infrequent pattern 
func testInfrequent() int{
    // file generation
    patterns := []string{"/mango"}
    pattern_count := 50
    total_count := 1500

    // grep command to send from machine
    grep := "client /mango"

    // generate files across all machines
    for i := 0; i < len(ports); i++ {
        sendLogFiles(generate_file_contents(patterns, pattern_count, total_count), ports[i])
    }
    // send grep command from machine
    totalLines := sendCommand("fa24-cs425-1207.cs.illinois.edu:8087", grep)
    out, err := strconv.Atoi(totalLines)
    if err != nil {
        fmt.Print(err)
    }
    return out
}

// test case to test an frequent pattern 
func testFrequent() int{
    // file generation
    patterns := []string{"/cherry"}
    pattern_count := 500
    total_count := 1500

    // grep command to send from machine
    grep := "client /cherry"

    // generate files across all machines
    for i := 0; i < len(ports); i++ {
        sendLogFiles(generate_file_contents(patterns, pattern_count, total_count), ports[i])
    }
    // send grep command from machine
    totalLines := sendCommand("fa24-cs425-1208.cs.illinois.edu:8088", grep)
    out, err := strconv.Atoi(totalLines)
    if err != nil {
        fmt.Print(err)
    }
    return out
}

// test case to test regex patterns
func testRegex() int{
    // file generation
    patterns := []string{"/kiwi", "/cherry"}
    pattern_count := 500
    total_count := 1500

    // grep command to send from machine
    grep := "client -E '/kiwi|/cherry'"

    // generate files across all machines
    for i := 0; i < len(ports); i++ {
        sendLogFiles(generate_file_contents(patterns, pattern_count, total_count), ports[i])
    }
    // send grep command from machine
    totalLines := sendCommand("fa24-cs425-1209.cs.illinois.edu:8089", grep)
    out, err := strconv.Atoi(totalLines)
    if err != nil {
        fmt.Print(err)
    }
    return out
}

// test case to test occurs in one machine
func testOccursInOne() int{
    // file generation for one machine
    patterns := []string{"/banana"}
    pattern_count := 100
    total_count := 1500

    // grep command to send from machine
    grep := "client /banana"

    // generate files across all machines
    sendLogFiles(generate_file_contents(patterns, pattern_count, total_count), ports[0])
    for i := 1; i < len(ports); i++ {
        sendLogFiles(generate_file_contents(patterns, 0, total_count), ports[i])
    }

    // send grep command from machine
    totalLines := sendCommand("fa24-cs425-1201.cs.illinois.edu:8081", grep)
    out, err := strconv.Atoi(totalLines)
    if err != nil {
        fmt.Print(err)
    }
    return out
}


// test case to test occurs in some machines
func testOccursInSome() int{
    // file generation for one machine
    patterns := []string{"/apple"}
    pattern_count := 500
    total_count := 1500

    // grep command to send from machine
    grep := "client /apple"

    // generate files with pattern across half machines
    for i := 0; i < len(ports); i++ {
        if i % 2 == 0{
            sendLogFiles(generate_file_contents(patterns, pattern_count, total_count), ports[i])
        } else {
            sendLogFiles(generate_file_contents(patterns, 0, total_count), ports[i])
        }
        
    }

    // send grep command from machine
    totalLines := sendCommand("fa24-cs425-1205.cs.illinois.edu:8085", grep)
    out, err := strconv.Atoi(totalLines)
    if err != nil {
        fmt.Print(err)
    }
    return out
}

// functon that will run all test cases
func testAll(testSet []Test) {
    for i := 0; i < 5; i++ {
        output := 0
        if i == 0 {
            output = testInfrequent()
        } else if i == 1 {
            output = testFrequent()
        } else if i == 2 {
            output = testRegex()
        } else if i == 3 {
            output = testOccursInOne()
        } else {
            output = testOccursInSome()
        }
        
        // check that the count of lines recieved is what is expected
        if !checkCount(output, testSet[i].countCheck){
            fmt.Println("Test Failed: " + testSet[i].testName)
        } else {
            fmt.Println("Test Passed: " + testSet[i].testName)
        }
    }  
}

// MACHINE CONNECTION FUNCTIONS

// function to write a log file to a specific machine given the content and port address
func sendLogFiles(content string, address string) {
    // Connect to the machine's server
    conn, err := net.Dial("tcp", address)
    if err != nil {
        fmt.Println(err)
    }
    defer conn.Close()

    // write the file contents to the machine
    conn.Write([]byte(content))
}

// function to send an intitial grep command as the machine passed in
func sendCommand(port string, message string) string {
    // conect to the port
    conn, err := net.Dial("tcp", port)
    if err != nil {
        fmt.Println(err)
        return ""
    }
    defer conn.Close()

    // send the to the machine
    conn.Write([]byte(message))

    // Read the response and append it to the result string
    var result strings.Builder
    buf := make([]byte, 1024)

    
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

    // string that has the output from the "initial machine"
    return result.String()
}


// HELPER FUNCTIONS

// check functions for test cases
func checkCount(output int, countCheck int) bool {
    return output == countCheck
}


// function to generate a random date between Jan 1, 2000 and now
func generate_date() string {
    start := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
    end := time.Now()
    randomDuration := time.Duration(rand.Int63n(end.Unix() - start.Unix())) * time.Second
    randomDate := start.Add(randomDuration)
    return randomDate.Format("2006-01-02")
}

// function to get the prefix of a line
func get_prefix() string {
    // random number generator
    rand.Seed(time.Now().UnixNano())

    // list of endpoint choices
    endpointChoices := []string{"GET", "PUT", "POST", "DELETE"}
    
    // random ip address
    ipAddress := fmt.Sprintf("%d.%d.%d.%d", rand.Intn(256), rand.Intn(256), rand.Intn(256), rand.Intn(256))

    // random date
    date := generate_date()

    // random endpoint
    randomIndex := rand.Intn(len(endpointChoices))
    endpoint := endpointChoices[randomIndex]

    return ipAddress + " " + date + " " + endpoint

}

// function to generate random file contents 
func generate_file_contents(patterns []string, pattern_count int, total_count int) string {
    // random number generator
    rand.Seed(time.Now().UnixNano())

    // list of filename choices
    filenames := []string{"/wp-admin","/wp-content", "/search/tag/list", "/app/main/posts", "/list", "/explore", "/posts/posts/explore"}

    output := ""
    ratio := 0
    if pattern_count != 0 {
        ratio = total_count / pattern_count
    }
    curr_count := 0

    // create line_amount number of random lines
    for i := 0; i <= total_count; i++ {

        prefix := get_prefix()
        // random file name
        filename := ""
        randomIndex := 0
        if curr_count != pattern_count && i % ratio == 0{ // generate from wanted patterns
            randomIndex = rand.Intn(len(patterns))
            filename = patterns[randomIndex]
            curr_count += 1
        } else { // generate from default patterns
            randomIndex = rand.Intn(len(filenames))
            filename = filenames[randomIndex]
        }

        contents := prefix + " " + filename + " HTTP/1.0" + "\n"
        
        output += contents
    }
    
    for curr_count < pattern_count {
        prefix := get_prefix()

        // filename
        randomIndex := rand.Intn(len(patterns))
        filename := patterns[randomIndex]

        contents := prefix + " " + filename + " HTTP/1.0" + "\n"
        
        output += contents
        curr_count += 1
    }
    return output
}
