package udp

import (
    "fmt"
    "net"
    "os"
    "strings"
    "time"
    "strconv"
    "math/rand"
)

func SendMessage(target_node string, to_send string, node_to_send string) {
    target_addr := target_node[:36]
    conn, err := DialUDPClient(target_addr)
    defer conn.Close()

    message := to_send + " " + node_to_send
    _, err = conn.Write([]byte(message))
    if err != nil {
        fmt.Println("Error sending fail message:", err)
        return
    }
}

// Function to send a failure message
func SendAlive(node_id string, to_clear string, inc_num string) {

    target_addr := node_id[:36]
    conn, err := DialUDPClient(target_addr)
    defer conn.Close()

    message := "alive " + to_clear + " " + inc_num
    _, err = conn.Write([]byte(message))
    if err != nil {
        fmt.Println("Error sending alive message:", err)
        return
    }
}

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

// Function to resolve and dial a UDP connection to a given address
func DialUDPClient(target_addr string) (*net.UDPConn, error) {

    // Resolve the UDP address
    addr, err := net.ResolveUDPAddr("udp", target_addr)
    if err != nil {
        fmt.Println("Error resolving target address:", err)
        return nil, err
    }

    // Dial UDP to the target node
    conn, err := net.DialUDP("udp", nil, addr)
    if err != nil {
        fmt.Println("Error connecting to target node:", err)
        return nil, err
    }
    return conn, nil
}

// Function to randomly select an alive node in the system
func SelectRandomNode() *Node {
    rand.Seed(time.Now().UnixNano())
    var target_node *Node
    for {
        random_index := rand.Intn(len(membership_list))
        selected_node := membership_list[random_index]
        if selected_node.NodeID != node_id && selected_node.Status != "leave" { 
            target_node = &selected_node
            break
        }
    }
    return target_node
}

func GetSelfID() string {
    return node_id
}

// Run the function for exactly 4 seconds
func susTimeout(duration time.Duration, sus_id string, inc_num int) {
	// Create a channel that will send a signal after the specified duration
	timeout := time.After(duration)
	// Run the work in a loop
	for {
		select {
		case <-timeout:
            RemoveNode(sus_id)
            return
		default:
			// Continue doing the work
            index := FindNode(sus_id)
            if index >= 0 && membership_list[index].Inc > inc_num {
                message := "Node suspect removed for: " + sus_id + "\n"
                appendToFile(message, logfile)
                return
            }
		}
	}
}

func checkStatus(node string) string {
    index := FindNode(node)
    if index >= 0 {
        return membership_list[index].Status
    }
    return "none"
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