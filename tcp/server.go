package tcp 

import (
    "fmt"
    "net"
    "os"
    "os/exec"
    "io"
    "io/ioutil"
    "strconv"
    "github.com/joho/godotenv"
    "strings"
)

var err = godotenv.Load(".env")
var tcp_port string = os.Getenv("TCP_PORT")

//starts tcp server that listens for grep commands
func TcpServer() {

    // listen for connection from other machine 
    ln, err := net.Listen("tcp", ":" + tcp_port)
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


//handler of any incoming connection from other machines
func handleConnection(conn net.Conn) {

    // Close the connection when we're done
    defer conn.Close()

    // Get the connection message
    buf := make([]byte, 1024)
    n, _ := conn.Read(buf)
    message := string(buf[:n])

    // Check if message is a grep command
    if message[:4] == "grep" {
        grep := message + " " + filename

        // run the grep command on machine
        cmd := exec.Command("sh", "-c", grep)
        output, err := cmd.CombinedOutput()
        
        if err != nil {
            fmt.Println(err)
            return
        }
        
        // send the result back to the initial machine
        conn.Write(output)

    // Check if message is a client call (for testing)
    } else if message[:6] == "client" {
        totalLines := TcpClient(message[7:])
        conn.Write([]byte(strconv.Itoa(totalLines)))

    //if not grep or command call, must be call to create a log file
    } else if message[:3] == "get" {
        filename := message[4:]
        filepath := "file-store/" + filename
        if _, err := os.Stat(filepath); os.IsNotExist(err) {
            // If the file does not exist, send an error message
            conn.Write([]byte("Error: File not found\n"))
        }

        file_content, err := ioutil.ReadFile(filepath)
        if err != nil {
            fmt.Println(err)
            return
        }
    
        // Send the file contents to the client
        conn.Write(file_content)
        fmt.Printf("Sent file contents of %s to client\n", filename)

    } else if message[:6] == "create" {
        words := strings.Split(message, " ")
        HyDFSfilename := words[1]
        replica_num := words[2]

        // check if the file already exists
        _, err := os.Stat("file-store/" + replica_num + "-" + HyDFSfilename)
	
        if os.IsNotExist(err) {
            argument_length := 11 + len(HyDFSfilename)
            fmt.Println(message[:argument_length+1])
            file_contents := message[argument_length:]

            file, err := os.Create("file-store/" + replica_num + "-" + HyDFSfilename)
            if err != nil {
                fmt.Println("Error creating the file:", err)
                return
            }

            defer file.Close()

            _, err = file.WriteString(file_contents)
            if err != nil {
                fmt.Println("Error writing to the file:", err)
                return
            }
        } else {
            fmt.Println("File already exists")
        }

    } else if len(message) >= 4 && message[:4] == "pull" {
        fmt.Println("message to pull")
        dir := "./file-store"
        files, err := ioutil.ReadDir(dir)
        if err != nil {
            fmt.Println("Error reading directory:", err)
        }

        // go through all the files
        for _, file := range files {
            if !file.IsDir() {
                filename = file.Name()
                // if file is from origin server send it back 
                if filename[:2] == tcp_port[1:3] {
                    file_path := dir + "/" + filename
                    content, err := ioutil.ReadFile(file_path)
                    if err != nil {
                        fmt.Println("Error reading file:", filename, err)
                    }

                    // Send the file name and content to the client
                    message := fmt.Sprintf("%s %s", filename, string(content))
                    _, err = conn.Write([]byte(message))
                    if err != nil {
                        fmt.Println("Error sending file content:", err)
                    }
                }
            }
        }
    }  else { 

        // Open the file to write the contents
        file, err := os.Create(filename)
        if err != nil {
            fmt.Println(err)
            return
        }
        defer file.Close()

        // Write initial chunk to the file
        _, err = file.Write(buf[:n])
        if err != nil {
            fmt.Println(err)
            return
        }

        // Read from the connection in chunks and write to the file
        for {
            n, err := conn.Read(buf)

            if err != nil {

                //break once at the end of the buffer
                if err == io.EOF {
                    break
                }
                fmt.Println(err)
                return 
            }

            // Write the chunk to the file
            _, err = file.Write(buf[:n])
            if err != nil {
                fmt.Println(err)
                return
            }
        }
    }
}