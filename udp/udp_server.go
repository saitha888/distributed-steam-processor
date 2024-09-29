package udp

import (
    "fmt"
    "os"
    "github.com/joho/godotenv"
    "time"
    "strconv"
)

// global variables``
var err = godotenv.Load(".env")
var udp_port string = os.Getenv("UDP_PORT")
var membership_list []Node
var logfile string = os.Getenv("LOG_FILENAME")
var inc_num int = 0


// struct for each process
type Node struct {
    NodeID    string  
    Status    string    
    Inc int 
}

//starts udp server that listens for pings
func UdpServer() {
    conn, _ := ConnectToMachine(udp_port)
    defer conn.Close()

    buf := make([]byte, 1024)

    for {
        // read response from machine
        n, addr, err := conn.ReadFromUDP(buf)
        if err != nil {
            fmt.Println("Error reading from UDP:", err)
            continue
        }

        message := string(buf[:n])
        if message == "mem_list" { // introducer asking for membership list
            result := MembershiplistToString()
            conn.WriteToUDP([]byte(result), addr)
        } else if message == "ping" { // machine checking health
            dropped := induceDrop(0)
            if !dropped {
                ack := node_id + " " + strconv.Itoa(inc_num)
                conn.WriteToUDP([]byte(ack), addr)
            }
        } else if message[:4] == "fail" { // machine failure detected
            failed_node := message[5:]
            RemoveNode(failed_node)
            message := "Node failure message recieved for: " + failed_node + " at " + time.Now().Format("15:04:05") + "\n"
            appendToFile(message, logfile)
        } else if message[:4] == "join" { // new machine joined
            joined_node := message[5:]
            index := FindNode(joined_node)
            if index >= 0 { // machine was found
                changeStatus(index, "alive")
            } else { // machine was not found
                AddNode(joined_node, 1, "alive")
            }
            message := "Node join detected for: " + joined_node + " at " + time.Now().Format("15:04:05") + "\n"
            appendToFile(message, logfile)
        } else if message[:5] == "leave" { // machine left
            left_node := message[6:]
            index := FindNode(left_node)
            if index >= 0 { // machine was found
                changeStatus(index, "leave")
            }
            message := "Node leave detected for: " + left_node + " at " + time.Now().Format("15:04:05") + "\n"
            appendToFile(message, logfile)
        } else if message[:9] == "suspected" { // machine left
            if enabled_sus {
                sus_node := message[10:]
                message := "Node suspect detected for: " + sus_node + " at " + time.Now().Format("15:04:05") + "\n"
                appendToFile(message, logfile)
                index := FindNode(sus_node)
                if sus_node == node_id {
                    fmt.Println("Node is currently suspected")
                    inc_num += 1
                    if index >= 0 { // machine was found
                        membership_list[index].Inc = inc_num
                    }
                } else {
                    if index >= 0 { // machine was found
                        changeStatus(index, " sus ")
                    }
                }
            }
        } else if message[:5] == "alive" { // machine unsuspected
            alive_node := message[6:62]
            inc_num, _ := strconv.Atoi(message[63:])
            message := "Suspected node cleared for: " + alive_node + " at " + time.Now().Format("15:04:05") + "\n"
            appendToFile(message, logfile)
            index := FindNode(alive_node)
            if index >= 0 {
                changeStatus(index, "alive")
                changeInc(index, inc_num)
            }
        }   
    }
}