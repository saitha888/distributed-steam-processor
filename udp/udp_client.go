package udp

import (
    "fmt"
    "time"
    "strings"
    "strconv"
)

// Global variable to save unique node ID
var node_id string = ""
var enabled_sus = false
var target_ports []string

// Function to join system through the introducer
func JoinSystem(address string) {
    // increment the incarnation number
    inc_num += 1
    // set the target ports
    DefineTargetPorts()
    // Connect to introducer
    conn, err := DialUDPClient(introducer_address)
    defer conn.Close()

    // Initialize node id for machine.
    if node_id == "" {
        node_id = address + "_" + time.Now().Format("2006-01-02_15:04:05")
    }

    // Send join message to introducer
    message := fmt.Sprintf("join %s", node_id)
    _, err = conn.Write([]byte(message))
    if err != nil {
        fmt.Println("Error sending message to introducer:", err)
        return
    }

    buf := make([]byte, 1024)

    // Read the response from the introducer (membership list to copy)
    n, _, err := conn.ReadFromUDP(buf)
    if err != nil {
        fmt.Println("Error reading from introducer:", err)
        return
    }
    memb_list_string := string(buf[:n])
    memb_list := strings.Split(memb_list_string,", ")

    // Clear existing membership list if dealing with a node that left
    membership_list = nil

    // Update machine's membership list
    for _,node :=  range memb_list {
        node_vars := strings.Split(node, " ")
        inc, _ := strconv.Atoi(node_vars[2])
        AddNode(node_vars[0], inc, node_vars[1])
    }

    // Print the response from the introducer (e.g., acknowledgment or membership list)
    fmt.Printf("Received mem_list from introducer\n")

}

// ping 4 subset nodes, and 1 random node
func PingNodes(plus_s bool) {
    for _,node := range target_ports {
        i := FindNodeWithPort(node)
        if i >= 0{
            PingClient(plus_s, membership_list[i])
        }
    }
    target_node := SelectRandomNode()
    PingClient(plus_s, target_node)
}

// Function to randomly select a node from the system and ping it
func PingClient(plus_s bool, target_node Node) {
    enabled_sus = plus_s
    target_addr := target_node.NodeID[:36]
    // Connect to the randomly selected node
    conn, err := DialUDPClient(target_addr)
    defer conn.Close()

    // Send a ping message
    message := fmt.Sprintf("ping")
    _, err = conn.Write([]byte(message))
    if err != nil {
        fmt.Println("Error sending ping message:", err)
        return
    }
    buf := make([]byte, 1024)

    // If no response is recieved in .5 seconds close the connection
    conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))

    n, _, err2 := conn.ReadFromUDP(buf)
    if err2 != nil {
        fmt.Println("Error reading from target node:", err2)
    } else { // response was recieved
        ack := string(buf[:n])
        recieved_node_id := ack[:56]
        recieved_inc_str := ack[57:]
        recieved_inc, _ := strconv.Atoi(ack[57:])
        index := FindNode(recieved_node_id)
        if index >= 0 {
            if membership_list[index].Status == " sus " || membership_list[index].Inc < recieved_inc { // If the machine was suspected it is now cleared
                message := "Node suspect cleared for: " + target_node.NodeID + " from machine " + udp_port + " at " + time.Now().Format("15:04:05") + "\n"
                appendToFile(message, logfile)
                for _,node := range membership_list { // let all machines know suspected node is alive
                    SendAlive(node.NodeID, target_node.NodeID, recieved_inc_str)
                }

            }
            // update status and inc number
            membership_list[index].Status = "alive"
            membership_list[index].Inc = recieved_inc
        }

    }
    if err2 != nil { // no response was recieved
        if plus_s == false { // if there's no suspicion immediately fail the machine
            message := "Node failure detected for: " + target_node.NodeID + " from machine " + udp_port + " at " + time.Now().Format("15:04:05") + "\n"
            appendToFile(message, logfile)
            RemoveNode(target_node.NodeID)
            for _,node := range membership_list {
                SendMessage(node.NodeID, "fail", target_node.NodeID)
            }
        }
        if plus_s && checkStatus(target_node.NodeID) != " sus "  { // if there is suspicion and the node isn't already sus
            message := "Node suspect detected for: " + target_node.NodeID + " from machine " + udp_port + " at " + time.Now().Format("15:04:05") + "\n"
            appendToFile(message, logfile)
            for _,node := range membership_list { // let all machines know node is suspected
                SendMessage(node.NodeID, "suspected",target_node.NodeID)
            }
            susTimeout(9*time.Second, target_node.NodeID, target_node.Inc) // wait 6 seconds to recieve update about node status
            index := FindNode(target_node.NodeID)
            if index < 0 { // if node was removed
                for _, node := range(membership_list) { // let all other nodes know node has failed
                    SendMessage(node.NodeID, "fail", target_node.NodeID)
                }
            }

        }
    }
}


