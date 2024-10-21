package udp

import (
    "fmt"
    "net"
    "os"
    "strings"
    "time"
    "strconv"
    "math/rand"
    "crypto/sha256"
    "github.com/emirpasic/gods/maps/treemap"
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
func SelectRandomNode() Node {
    rand.Seed(time.Now().UnixNano())
    var target_node Node
    for {
        random_index := rand.Intn(len(membership_list))
        selected_node := membership_list[random_index]
        if selected_node.NodeID != node_id && selected_node.Status != "leave" { 
            target_node = selected_node
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
    ring_map.Remove(GetHash(node_id))
}

//function to add node
func AddNode(node_id string, node_inc int, status string){
    new_node := Node{
        NodeID:    node_id,  
        Status:    status,           
        Inc: node_inc,
    }
    membership_list = append(membership_list, new_node)
    ring_map.Put(GetHash(node_id), node_id)
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

func GetHash(data string) string {
	hash := sha256.Sum256([]byte(data))
	return fmt.Sprintf("%x", hash)  // Returns the hex string representation of the hash
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


func ListMem(list_to_print []Node) {
    if len(list_to_print) == 0 {
        fmt.Println("List is empty.")
        return
    }

    nodeIDWidth := 54
    statusWidth := 4

    fmt.Printf("%-*s | %-*s | %s\n", nodeIDWidth, "NodeID", statusWidth, "Status", "Incarnation #")
    fmt.Println(strings.Repeat("-", nodeIDWidth+statusWidth+25))

    // Go through membership list and print each entry
    for _, node := range list_to_print {
        fmt.Printf("%s | %s  | %s\n",node.NodeID, node.Status, strconv.Itoa(node.Inc))
    }
    fmt.Println()
    fmt.Print("> ")
}

func GetRing() *treemap.Map {
    return ring_map
}

func ListRing(treeMap *treemap.Map) {
    keys := treeMap.Keys()
    for _, hash := range keys {
        id, _ := treeMap.Get(hash)  // Get the value associated with the key
		fmt.Printf("Hash: %s, Node: %s\n", hash, id)
    }
}

func FindSusMachines() []Node {
	var susList []Node
	for _, node := range membership_list {
        if node.Status == " sus "{
			susList = append(susList, node)
		}
    }
	return susList
}

func GetMembershipList() []Node {
	return membership_list
}

func DefineTargetPorts() {
    for i := 1; i < 5; i++ {
        // index, _ := strconv.Itoa(i)
        wanted_machine := "MACHINE_" + strconv.Itoa(i)
        target_ports = append(target_ports, os.Getenv(wanted_machine))
    }
}

func FindNodeWithPort(port string) int {
    for index,node := range(membership_list) {
        if port == node.NodeID[:36] {
            return index
        }
    }
    return -1
}