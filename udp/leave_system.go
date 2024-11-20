package udp

import (
    "fmt"
    "net"
    "os"
    "strings"
    "bufio"
    "encoding/json"
    "io"
)

//Function to leave the system
func LeaveList() {
    // Change own status to left, inform other machines to change status to left
    for i,node :=  range membership_list {
        if node.NodeID == node_id { // check if at self
            changeStatus(i, "leave")
        } else { 
            node_address := node.NodeID[:36]
            conn, err := DialUDPClient(node_address)
            defer conn.Close()

            // Send leave message
            message := fmt.Sprintf("leave " + node_id)
            _, err = conn.Write([]byte(message))
            if err != nil {
                fmt.Println("Error sending leave message:", err)
                return
            }
        }
    }
}

// Remove a machine from the membership list
func RemoveNode(id_to_rem string) {
    bytes := []byte(node_id)
	bytes[32] = '8'
	
	node_id = string(bytes)

    for index,node := range membership_list {
        if id_to_rem == node.NodeID { // remove the node if it's found
            membership_list = append(membership_list[:index], membership_list[index+1:]...)
        }
    }

    bytes_remove := []byte(id_to_rem)
	
	bytes_remove[32] = '8'
	
	id_to_remove := string(bytes_remove)

    ring_map := GetRing()
    iterator := IteratorAt(ring_map, id_to_remove)
    id := ""
    if (!iterator.Next()) {
        iterator.First()
    }
    id = iterator.Value().(string)
    if (id == node_id) {
        //if removed node is right before this node
        //this node becomes new origin for failed node, rename files
        RenameFilesWithPrefix(id_to_remove[13:15], node_id[13:15])

        //pull files of origin n-3
        nod := IteratorAtNMinusSteps(ring_map, node_id, 3)
        port := nod[:36]
        // pull for files
        conn_pred, err := net.Dial("tcp", port )
        if err != nil {
            fmt.Println(err)
            return
        }
        defer conn_pred.Close()
        message := fmt.Sprintf("pull")
        conn_pred.Write([]byte(message))
        reader := bufio.NewReader(conn_pred)
        buffer := ""

        for {
            // Read up to the next newline in chunks
            part, err := reader.ReadString('\n')
            if err != nil {
                fmt.Println("Error reading from server:", err)
                break
            }

            // Append the read part to the buffer
            buffer += part

            // Check if buffer contains the custom delimiter
            if strings.Contains(buffer, "\n---END_OF_MESSAGE---\n") {
                // Split buffer by the custom delimiter
                parts := strings.Split(buffer, "\n---END_OF_MESSAGE---\n")

                // Process all complete messages in parts
                for i := 0; i < len(parts)-1; i++ {
                    if strings.TrimSpace(parts[i]) != "" { // Ignore empty messages
                        filename := strings.Split(parts[i], " ")[0]
                        argument_length := 1 + len(filename)
                        contents := parts[i][argument_length:]
                        new_filename := "./file-store/" + filename
        
                        file, err := os.OpenFile(new_filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
                        if err != nil {
                            fmt.Println(err)
                        }
                        defer file.Close()
        
                        _, err = file.WriteString(contents)
                        if err != nil {
                            fmt.Println(err)
                        }
                    }
                }
                // Retain the last part in the buffer (incomplete message)
                buffer = parts[len(parts)-1]
            }
        }
    }
    id2 := ""
    if (!iterator.Next()) {
        iterator.First()
    }
    id2 = iterator.Value().(string)
    if (id2 == node_id) {
        //if removed node is 2 nodes before this node
        //rename files of origin n-2 to n-1 
        RenameFilesWithPrefix(IteratorAtNMinusSteps(ring_map, node_id, 2)[13:15], IteratorAtNMinusSteps(ring_map, node_id, 1)[13:15])

        //pull files of origin n-3
        nod := IteratorAtNMinusSteps(ring_map, node_id, 3)
        port := nod[:36]
        // pull for files
        conn_pred, err := net.Dial("tcp", port )
        if err != nil {
            fmt.Println(err)
            return
        }
        defer conn_pred.Close()
        message := fmt.Sprintf("pull")
        conn_pred.Write([]byte(message))
        reader := bufio.NewReader(conn_pred)
        buffer := ""

        for {
            // Read up to the next newline in chunks
            part, err := reader.ReadString('\n')
            if err != nil {
                fmt.Println("Error reading from server:", err)
                break
            }

            // Append the read part to the buffer
            buffer += part

            // Check if buffer contains the custom delimiter
            if strings.Contains(buffer, "\n---END_OF_MESSAGE---\n") {
                // Split buffer by the custom delimiter
                parts := strings.Split(buffer, "\n---END_OF_MESSAGE---\n")

                // Process all complete messages in parts
                for i := 0; i < len(parts)-1; i++ {
                    if strings.TrimSpace(parts[i]) != "" { // Ignore empty messages
                        filename := strings.Split(parts[i], " ")[0]
                        argument_length := 1 + len(filename)
                        contents := parts[i][argument_length:]
                        new_filename := "./file-store/" + filename
        
                        file, err := os.OpenFile(new_filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
                        if err != nil {
                            fmt.Println(err)
                        }
                        defer file.Close()
        
                        _, err = file.WriteString(contents)
                        if err != nil {
                            fmt.Println(err)
                        }
                    }
                }
                // Retain the last part in the buffer (incomplete message)
                buffer = parts[len(parts)-1]
            }
        }
    } 
    id3 := ""
    if (!iterator.Next()) {
        iterator.First()
    }
    //3, 1, 2, 4, 5
    id3 = iterator.Value().(string)
    if (id3 == node_id) {
        nod := IteratorAtNMinusSteps(ring_map, node_id, 2)
        port := nod[:36]
        // pull for files
        conn_pred, err := net.Dial("tcp", port )
        if err != nil {
            fmt.Println(err)
            return
        }
        defer conn_pred.Close()
        data := Message{
            Action: fmt.Sprintf("pull-3 %s", id_to_remove),
            Filename:  "",
            FileContents: "",
        }
        encoder := json.NewEncoder(conn_pred)
        err = encoder.Encode(data)
        if err != nil {
            fmt.Println("Error encoding data in pull-3", err)
        } 

        decoder := json.NewDecoder(conn_pred)
        for {
            var response Message
            err = decoder.Decode(&response)
            if err != nil {
                if err == io.EOF {
                    // End of the response from the server
                    fmt.Println("All messages received.")
                    break
                }
                fmt.Println("Error decoding message from server:", err)
                return
            }
            file, err := os.OpenFile(response.Filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
            if err != nil {
                fmt.Println(err)
            }
            defer file.Close()

            _, err = file.WriteString(response.FileContents)
            if err != nil {
                fmt.Println(err)
            }

        }
    }
    ring_map.Remove(GetHash(id_to_remove))
}