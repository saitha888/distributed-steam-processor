package udp

import (
    "fmt"
    "net"
    "os"
    "github.com/joho/godotenv"
    "strings"
    "time"
    "strconv"
    "math/rand"
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
            dropped := induceDrop(0.1)
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

func ListMem() {
    if len(membership_list) == 0 {
        fmt.Println("Membership list is empty.")
        return
    }

    nodeIDWidth := 54
    statusWidth := 4

    fmt.Printf("%-*s | %-*s | %s\n", nodeIDWidth, "NodeID", statusWidth, "Status", "Incarnation #")
    fmt.Println(strings.Repeat("-", nodeIDWidth+statusWidth+25))

    // Go through membership list and print each entry
    for _, node := range membership_list {
        fmt.Printf("%s | %s  | %s\n",node.NodeID, node.Status, strconv.Itoa(node.Inc))
    }
    fmt.Println()
    fmt.Print("> ")
}


// Function to connect to another machine
func ConnectToMachine(port string) (*net.UDPConn, error){
    addr, err := net.ResolveUDPAddr("udp", ":" + port)
    if err != nil {
        fmt.Println("Error resolving address:", err)
    }

    conn, err := net.ListenUDP("udp", addr)
    if err != nil {
        fmt.Println("Error starting UDP server:", err)
    }
    return conn, nil
}

// Turn the membership list global variable into a string
func MembershiplistToString() string{
    nodes := make([]string, 0)
    for _,node := range membership_list {
        current_node := node.NodeID + " " + node.Status + " " + strconv.Itoa(node.Inc)
        nodes = append(nodes, current_node)
    }
    result := strings.Join(nodes, ", ")
    return result
}

// Remove a machine from the membership list
func RemoveNode(node_id string) {
    for index,node := range membership_list {
        if node_id == node.NodeID { // remove the node if it's found
            membership_list = append(membership_list[:index], membership_list[index+1:]...)
        }
    }
}

//function to add node
func AddNode(node_id string, node_inc int, status string){
    new_node := Node{
        NodeID:    node_id,  
        Status:    status,           
        Inc: node_inc,
    }
    membership_list = append(membership_list, new_node)
}


// Get the index of a machine in the list
func FindNode(node_id string) int {
    for index,node := range membership_list { 
        if node_id == node.NodeID {
            return index
        }
    }
    return -1
}

// Change the status of a machine in the list
func changeStatus(index int, message string){
    membership_list[index].Status = message
}

// Change the status of a machine in the list
func changeInc(index int, message int){
    membership_list[index].Inc = message
}


// Function to append a string to a file
func appendToFile(content string, filename string) error {
	// Open the file or create it 
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	// Write the content to the file
	_, err = file.WriteString(content)
	if err != nil {
		return err
	}

	return nil
}

func induceDrop(x float64) bool {
    // Seed the random number generator
    rand.Seed(time.Now().UnixNano())

    // Generate a random float between 0 and 1
    return rand.Float64() < x
}

