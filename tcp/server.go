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
    "distributed_system/udp"
    "time"
    "regexp"
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
    buf := make([]byte, 1000000)
    n, _ := conn.Read(buf)
    message := string(buf[:n])


    // Check if message is a grep command
    if len(message) >= 4 && message[:4] == "grep" {
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
    } else if len(message) >= 6 && message[:6] == "client" {
        totalLines := TcpClient(message[7:])
        conn.Write([]byte(strconv.Itoa(totalLines)))
    //if not grep or command call, must be call to create a log file
    } else if len(message) >= 3 && message[:3] == "get" {
        filename := message[4:]
        log := "Message received to retrieve file " + filename + " at " + time.Now().Format("15:04:05.000")
        udp.AppendToFile(log, os.Getenv("HDYFS_FILENAME"))
        file_content := []byte(udp.GetFileContents(filename))
        conn.Write(file_content)
        log = "File " + filename + " sent back to client at " + time.Now().Format("15:04:05.000")
        udp.AppendToFile(log, os.Getenv("HDYFS_FILENAME"))
    } else if len(message) >= 6 && message[:6] == "create" {
        words := strings.Split(message, " ")
        HyDFSfilename := words[1]
        cache_set[HyDFSfilename] = true
        cache_path := "./cache" + "/" + HyDFSfilename
        argument_length := 11 + len(HyDFSfilename)
        err = WriteToFile(cache_path, message[argument_length:])
        cache_set[HyDFSfilename] = true
        replica_num := words[2]
        
        // check if the file already exists
        file_path := "file-store/" + replica_num + "-" + HyDFSfilename
        _, err := os.Stat(file_path)

        if os.IsNotExist(err) {
            log := "Message received to create file " + HyDFSfilename + " at " + time.Now().Format("15:04:05.000")
            udp.AppendToFile(log, os.Getenv("HDYFS_FILENAME"))
            file_contents := message[argument_length:]
            WriteToFile(file_path, file_contents)
            conn.Write([]byte(HyDFSfilename + " created on machine " + udp.GetNodeID()))
            
            log = HyDFSfilename + " created at " + time.Now().Format("15:04:05.000")
            udp.AppendToFile(log, os.Getenv("HDYFS_FILENAME"))
        } 
    } else if len(message) >= 10 && message[:10] == "append-req"{
        words := strings.Split(message, " ")
        hydfs_file := words[2]
        local_file := words[1]
        AppendFile(local_file, hydfs_file)
    } else if len(message) >=6 && message[:6] == "append" {
        timestamp := time.Now().Format("15:04:05.000")
        words := strings.Split(message, " ")
        HyDFSfilename := words[1]
        HyDFSfilename = HyDFSfilename + "-" + timestamp
        replica_num := words[2]
        // check if the file already exists
        file_path := "file-store/" + replica_num + "-" + HyDFSfilename
        _, err := os.Stat(file_path)
        
        if os.IsNotExist(err) {
            // File doesn't exist, use original filename
        } else {
            // File exists, add 3 milliseconds to timestamp
            timestamp := HyDFSfilename[len(HyDFSfilename)-12:]
            newTime, _ := time.Parse("15:04:05.000", timestamp)
            formattedTime := newTime.Add(3 * time.Millisecond).Format("15:04:05.000")
            HyDFSfilename = HyDFSfilename[:len(HyDFSfilename)-12] + formattedTime
            file_path = "file-store/" + replica_num + "-" + HyDFSfilename
        }
        udp.RemoveFromCache(HyDFSfilename[3:])
        log := "Message received to create append chunk" + HyDFSfilename + " at " + time.Now().Format("15:04:05.000")
        udp.AppendToFile(log, os.Getenv("HDYFS_FILENAME"))
        argument_length := 11 + len(HyDFSfilename) - 13
        file_contents := message[argument_length:]
        WriteToFile(file_path, file_contents)
        conn.Write([]byte("append chunk "+ HyDFSfilename + " created on machine " + udp.GetNodeID()))
        
        log = "append chunk " + HyDFSfilename + " created at " + time.Now().Format("15:04:05.000")
        udp.AppendToFile(log, os.Getenv("HDYFS_FILENAME"))

    } else if len(message) >= 6 && message[:6] == "pull-3" {
        parts := strings.Split(message, " ")
        prefix := parts[1][13:15]
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
                if filename[:2] == prefix || filename[:2] == udp.GetFilePrefix() {
                    machinePrefix := udp.GetFilePrefix()
                    // Check conditions and update filename if needed
                    if filename[:2] == prefix {
                        filename = machinePrefix + filename[2:]
                    }
                    file_path := dir + "/" + filename
                    content, err := ioutil.ReadFile(file_path)
                    if err != nil {
                        fmt.Println("Error reading file:", filename, err)
                    }
                    // Send the file name and content to the client
                    message := fmt.Sprintf("%s %s\n---END_OF_MESSAGE---\n", filename, string(content))
                    _, err = conn.Write([]byte(message))
                    if err != nil {
                        fmt.Println("Error sending file content:", err)
                    }
                }
            }
        }
    } else if len(message) >= 4 && message[:4] == "pull" {
        dir := "./file-store"
        files, err := ioutil.ReadDir(dir)
        if err != nil {
            fmt.Println("Error reading directory:", err)
        }
        // go through all the files
        for _, file := range files {
            if !file.IsDir() {
                filename = file.Name()

                pattern := `\d{2}:\d{2}:\d{2}\.\d{3}$`
                match, _ := regexp.MatchString(pattern, filename)
                if filename[:2] == udp.GetFilePrefix() && !match {
                    file_path := dir + "/" + filename
                    content, err := ioutil.ReadFile(file_path)
                    if err != nil {
                        fmt.Println("Error reading file:", filename, err)
                    }
                    // Send the file name and content to the client
                    message := fmt.Sprintf("%s %s\n---END_OF_MESSAGE---\n", filename, string(content))
                    _, err = conn.Write([]byte(message))
                    if err != nil {
                        fmt.Println("Error sending file content:", err)
                    }
                }
            }
        }
    }  else if len(message) >= 5 && message[:5] == "split" {
        dir := "./file-store"

        files, err := ioutil.ReadDir(dir)
        if err != nil {
            fmt.Println("Error reading directory:", err)
        }

        pred_port := strings.TrimRight(message[6:], " \t\n")
        pred_hash := udp.GetHash(pred_port)
        self_hash := udp.GetHash(udp.GetTCPVersion(udp.GetNodeID()))

        // go through all the files
        for _, file := range files {
            if !file.IsDir() {
                filename := file.Name()
                // if the file is from the origin server
                if strings.HasPrefix(filename, udp.GetFilePrefix()) || strings.HasPrefix(filename, pred_port[13:15]) {
                    file_hash := udp.GetHash(filename[3:])
                    file_path := dir + "/" + filename
                    content, err := ioutil.ReadFile(file_path)
                    if err != nil {
                        fmt.Println("Error reading file:", filename, err)
                    }
                    if pred_hash >= file_hash && file_hash < self_hash { // if the hash now maps to the new server 
                        new_filename := filename[3:] // rename the file and send it back
                        message := fmt.Sprintf("%s %s\n---END_OF_MESSAGE---\n", new_filename, string(content))
                        _, err = conn.Write([]byte(message))
                        if err != nil {
                            fmt.Println("Error sending file content:", err)
                        }
                        // rename file in own directory
                        renamed := dir+"/"+pred_port[13:15]+"-"+new_filename
                        err = os.Rename(dir+"/"+filename, renamed)
                        if err != nil {
                            fmt.Println("Error renaming file:", err)
                        }
                    }
                }
            }   
        }
    } else if len(message) >= 6 && message[:6] == "chunks" {
        dir := "./file-store"
        files, err := ioutil.ReadDir(dir)
        if err != nil {
            fmt.Println("Error reading directory:", err)
        }
        words := strings.Split(message, " ")
        name := words[1]
        msg := ""
        for _, file := range files {
            if !file.IsDir() && strings.Contains(file.Name(), name)  {
                pattern := `\d{2}:\d{2}:\d{2}\.\d{3}$`
                match, _ := regexp.MatchString(pattern, file.Name())
                if match {
                    filePath := dir + "/" + file.Name()       
                    content, err_r := ioutil.ReadFile(filePath)
                    if err_r != nil {
                        fmt.Println("Error reading file:", err_r)
                        continue
                    }
                    msg += fmt.Sprintf("%s %s\n---BREAK---\n", file.Name(), string(content))
                    err_rem := os.Remove(filePath)
                    if err_rem != nil {
                        fmt.Println("Error deleting file:", err_rem)
                    } 
                } 
            }
        }
        _, err = conn.Write([]byte(msg))
        if err != nil {
            fmt.Println("Error sending file content:", err)
        }
    } else if len(message) >= 5 && message[:5] == "merge" {
        words := strings.Split(message, " ")
        hydfs_file := words[1]
        merged_content := message[7 + len(hydfs_file):]
        origin_num := udp.GetFileServers(udp.GetHash(hydfs_file))[0][13:15]
        hydfs_file = origin_num + "-" + hydfs_file
        file_path := "./file-store/" + hydfs_file

        file, err := os.OpenFile(file_path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
        if err != nil {
            fmt.Println("Error opening file:", err)
            return
        }
        defer file.Close()
        // Append the merged content to the file
        _, err = file.WriteString(merged_content)
        if err != nil {
            fmt.Println("Error writing to file:", err)
            return
        }
        msg := hydfs_file + " updated with merge"
        _, err = conn.Write([]byte(msg))
        if err != nil {
            fmt.Println("Error sending file content:", err)
        }

    } else { 
        // Open the file to write the contents
        file, err := os.Create(filename)
        if err != nil {
            fmt.Println(err)
        }
        defer file.Close()

        // Write initial chunk to the file
        _, err = file.Write(buf[:n])
        if err != nil {
            fmt.Println(err)
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
            }

            // Write the chunk to the file
            _, err = file.Write(buf[:n])
            if err != nil {
                fmt.Println(err)
            }
        }
    }
}

