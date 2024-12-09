package util

import (
    "fmt"
	"net"
    "os"
    "strings"
    "strconv"
    "io/ioutil"
    "regexp"
    "distributed_system/global"
    "crypto/sha256"
    "encoding/binary"
)

// Function to write content to a local file
func WriteToFile(filename string, content string) error {
    file, err := os.Create(filename)
    if err != nil {
        return err
    }
    defer file.Close()

    _, err = file.WriteString(content)
    if err != nil {
        return err
    }
    return nil
}


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

// Function to append a string to a file
func AppendToFile(content string, filename string) error {
	// Open the file or create it 
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	// Write the content to the file
	_, err = file.WriteString(content + "\n")
	if err != nil {
		return err
	}

	return nil
}

func FindSusMachines() []global.Node {
	var susList []global.Node
	for _, node := range global.Membership_list {
        if node.Status == " sus "{
			susList = append(susList, node)
		}
    }
	return susList
}

// get the tcp version of the node_id (value in the ring map)
func GetTCPVersion(id string) string {
    bytes := []byte(id)
	bytes[32] = '8'
	id = string(bytes)
    
    return id
}

func ListStore() map[string]bool {
    timestamp_pattern := `^[\d]{2}:[\d]{2}:[\d]{2}\.[\d]{3}$`
    fmt.Println("STORE FOR: " + global.Node_id[:31] + " - " + "HASH: " + strconv.Itoa(GetHash(global.Ring_id)))
    dir := "./file-store"

    files, err := ioutil.ReadDir(dir)
    if err != nil {
        fmt.Println("Error reading directory:", err)
    }

    file_set := make(map[string]bool)

    for _, file := range files {
        filename := file.Name()
        last_dash := strings.LastIndex(filename, "-")
        if last_dash != -1 {
            prefix := filename[:last_dash]
	        timestamp := filename[last_dash+1:]
            matched, _ := regexp.MatchString(timestamp_pattern, timestamp)
            if matched {
                filename = prefix[3:]
            }
        }
        if file_set[filename] {
            continue
        }
        file_set[filename] = true
        if !file.IsDir() {
            fmt.Println("File: " + filename + " \tFile Hash: " + strconv.Itoa(GetHash(filename)))
        }
    }
    return file_set
}

// Get the deterministic hash of a string
func GetHash(data string) int {
	hash := sha256.Sum256([]byte(data))
    truncated_hash := binary.BigEndian.Uint64(hash[:8])
    ring_hash := truncated_hash % 2048
	return (int)(ring_hash)
}

// Get the deterministic hash of a string
func GetUniqueNodeID(data string) int {
	hash := sha256.Sum256([]byte(data))
    truncated_hash := binary.BigEndian.Uint64(hash[:8])
    ring_hash := truncated_hash
	return (int)(ring_hash)
}

// Change the status of a machine in the list
func ChangeStatus(index int, message string){
    global.Membership_list[index].Status = message
}

// Turn the membership list global variable into a string
func MembershiplistToString() string{
    nodes := make([]string, 0)
    for _,node := range global.Membership_list {
        current_node := node.NodeID + " " + node.Status + " " + strconv.Itoa(node.Inc)
        nodes = append(nodes, current_node)
    }
    result := strings.Join(nodes, ", ")
    return result
}

// Change the status of a machine in the list
func ChangeInc(index int, message int){
    global.Membership_list[index].Inc = message
}

// Function to resolve and dial a TCP connection to a given address
func DialTCPClient(target_port string) (net.Conn, error) {
	conn, err := net.Dial("tcp", target_port)
	if err != nil {
		fmt.Println("Error dialing TCP connection:", err)
		return nil, err
	}
	return conn, nil
}

func Contains(slice []string, value string) bool {
	for _, v := range slice {
		if v == value {
			return true
		}
	}
	return false
}

func DisplaySchedule() {
	for stage, tasks := range global.Schedule {
		var ports []string
		for _, task := range tasks {
			if port, exists := task["Port"]; exists {
				ports = append(ports, port)
			}
		}

		// Print the stage and its ports
		fmt.Printf("Stage %d: %s\n", stage, strings.Join(ports, ", "))
	}
}

func ParseArguments(input string) []string {
    var args []string
    var currentArg strings.Builder
    inQuotes := false
 
 
    for _, char := range input {
        switch char {
        case '"':
            inQuotes = !inQuotes // Toggle the quotes state
        case ' ':
            if !inQuotes { // If not inside quotes, consider this as a separator
                if currentArg.Len() > 0 {
                    args = append(args, currentArg.String())
                    currentArg.Reset()
                }
            } else {
                currentArg.WriteRune(char) // Add space to the current argument if inside quotes
            }
        default:
            currentArg.WriteRune(char)
        }
    }
 
 
    // Add the last argument, if any
    if currentArg.Len() > 0 {
        args = append(args, currentArg.String())
    }
 
 
    return args
 }
