package tcp 

import (
    "fmt"
    "net"
    "os"
    "github.com/joho/godotenv"
    "distributed_system/udp"
    "time"
    "encoding/json"
    "strings"
    "io/ioutil"
    "regexp"
)

var err = godotenv.Load(".env")
var tcp_port string = os.Getenv("TCP_PORT")

type Message struct {
    Action string
	Filename  string
	FileContents string
}

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
    var received_data Message
	decoder := json.NewDecoder(conn)
	_ = decoder.Decode(&received_data)

    // Check if message is a grep command
    // if len(message) >= 4 && message[:4] == "grep" {
    //     grep := message + " " + filename
    //     // run the grep command on machine
    //     cmd := exec.Command("sh", "-c", grep)
    //     output, err := cmd.CombinedOutput()
    //     if err != nil {
    //         fmt.Println(err)
    //         return
    //     }
    //     // send the result back to the initial machine
    //     conn.Write(output)
    // // Check if message is a client call (for testing)
    // } else if len(message) >= 6 && message[:6] == "client" {
    //     totalLines := TcpClient(message[7:])
    //     conn.Write([]byte(strconv.Itoa(totalLines)))
    //if not grep or command call, must be call to create a log file
    // }
    if received_data.Action == "get" {
        log := "Message received to retrieve file " + received_data.Filename + " at " + time.Now().Format("15:04:05.000")
        udp.AppendToFile(log, os.Getenv("HDYFS_FILENAME"))

        dir := "./file-store"
        files, err := ioutil.ReadDir(dir)
        if err != nil {
            fmt.Println("Error reading directory:", err)
        }
        name := received_data.Filename
        for _, file := range files {
            if !file.IsDir() && strings.Contains(file.Name(), name)  {
                file_content, _ := os.ReadFile("file-store/" + file.Name())
                responseStruct := Message {
                    Action:    "",
                    Filename:  "",
                    FileContents: string(file_content),
                }
                encoder := json.NewEncoder(conn)
                err = encoder.Encode(responseStruct)
            }
        }
        log = "File " + filename + " sent back to client at " + time.Now().Format("15:04:05.000")
        udp.AppendToFile(log, os.Getenv("HDYFS_FILENAME"))
    } else if received_data.Action == "create" {
        cache_set[received_data.Filename[3:]] = true
        cache_path := "./cache/" + received_data.Filename[3:]
        err = WriteToFile(cache_path, received_data.FileContents[3:])
        
        // check if the file already exists
        file_path := "file-store/" + received_data.Filename
        _, err := os.Stat(file_path)

        if os.IsNotExist(err) {
            log := "Message received to create file " + received_data.Filename + " at " + time.Now().Format("15:04:05.000")
            udp.AppendToFile(log, os.Getenv("HDYFS_FILENAME"))
            WriteToFile(file_path, received_data.FileContents)
            log = received_data.Filename + " created at " + time.Now().Format("15:04:05.000")
            udp.AppendToFile(log, os.Getenv("HDYFS_FILENAME"))
        } 
    } else if len(received_data.Action) >= 10 && received_data.Action[:10] == "append-req"{
        words := strings.Split(received_data.Action, " ")
        hydfs_file := words[2]
        local_file := words[1]
        AppendFile(local_file, hydfs_file)
    } else if received_data.Action == "append" {
        timestamp := time.Now().Format("15:04:05.000")
        HyDFSfilename := received_data.Filename + "-" + timestamp
        // check if the file already exists
        file_path := "file-store/" + HyDFSfilename
        _, err := os.Stat(file_path)
        
        if os.IsNotExist(err) {
            // File doesn't exist, use original filename
        } else {
            // File exists, add 3 milliseconds to timestamp
            timestamp := HyDFSfilename[len(HyDFSfilename)-12:]
            newTime, _ := time.Parse("15:04:05.000", timestamp)
            formattedTime := newTime.Add(3 * time.Millisecond).Format("15:04:05.000")
            HyDFSfilename = HyDFSfilename[:len(HyDFSfilename)-12] + formattedTime
            file_path = "file-store/" + HyDFSfilename
        }
        udp.RemoveFromCache(HyDFSfilename[3:len(HyDFSfilename)-12])
        log := "Message received to create append chunk" + HyDFSfilename + " at " + time.Now().Format("15:04:05.000")
        udp.AppendToFile(log, os.Getenv("HDYFS_FILENAME"))
        WriteToFile(file_path, received_data.FileContents)        
        log = "append chunk " + HyDFSfilename + " created at " + time.Now().Format("15:04:05.000")
        udp.AppendToFile(log, os.Getenv("HDYFS_FILENAME"))
    } else if len(received_data.Action) >= 6 && received_data.Action[:6] == "pull-3" {
        parts := strings.Split(received_data.Action, " ")
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
                    responseStruct := Message{
                        Action:    "",
                        Filename:  filename,
                        FileContents: string(content),
                    }
                    encoder := json.NewEncoder(conn)
                    err = encoder.Encode(responseStruct)
                }
            }
        }
    } else if received_data.Action == "pull" {
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
                    responseStruct := Message{
                        Action:    "",
                        Filename:  filename,
                        FileContents: string(content),
                    }
                    encoder := json.NewEncoder(conn)
                    err = encoder.Encode(responseStruct)
                }
            }
        }
    } else if len(received_data.Action) >= 5 && received_data.Action[:5] == "split" {
        dir := "./file-store"

        files, err := ioutil.ReadDir(dir)
        if err != nil {
            fmt.Println("Error reading directory:", err)
        }

        pred_port := strings.TrimRight(received_data.Action[6:], " \t\n")
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
                        responseStruct := Message{
                            Action:    "",
                            Filename:  pred_port[13:15] + new_filename,
                            FileContents: string(content),
                        }
                        encoder := json.NewEncoder(conn)
                        err = encoder.Encode(responseStruct)
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
    } else if received_data.Action == "chunks" {
        dir := "./file-store"
        files, err := ioutil.ReadDir(dir)
        if err != nil {
            fmt.Println("Error reading directory:", err)
        }
        name := received_data.Filename
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
                    chunk_message := Message{
                        Action: "",
                        Filename: file.Name(),
                        FileContents: string(content),
                    }
                    encoder := json.NewEncoder(conn)
                    err_send := encoder.Encode(chunk_message)
                    if err_send != nil {
                        fmt.Println("error sending chunk", err_send)
                    }
                    err_rem := os.Remove(filePath)
                    if err_rem != nil {
                        fmt.Println("Error deleting file:", err_rem)
                    } 
                } 
            }
        }
    } else if received_data.Action == "merge" {
        hydfs_file := received_data.Filename
        merged_content := received_data.FileContents
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
    } 
//else { 
//         // Open the file to write the contents
//         file, err := os.Create(filename)
//         if err != nil {
//             fmt.Println(err)
//         }
//         defer file.Close()

//         // Write initial chunk to the file
//         _, err = file.Write(buf[:n])
//         if err != nil {
//             fmt.Println(err)
//         }

//         // Read from the connection in chunks and write to the file
//         for {
//             n, err := conn.Read(buf)

//             if err != nil {

//                 //break once at the end of the buffer
//                 if err == io.EOF {
//                     break
//                 }
//                 fmt.Println(err)
//             }

//             // Write the chunk to the file
//             _, err = file.Write(buf[:n])
//             if err != nil {
//                 fmt.Println(err)
//             }
//         }
//     }
//}

}
