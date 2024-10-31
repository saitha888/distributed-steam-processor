package udp

import (
    "fmt"
    "time"
    "strconv"
)
//5, 3, 1, 4, 2
//5 failed, 3 did not get 4s files, n-3 mapped to 2

//5, 2, 3, 1, 4

// Global variable to save unique node ID
var node_id string = ""
var enabled_sus = false

var ports = []string{
    "fa24-cs425-1201.cs.illinois.edu:9081", 
    "fa24-cs425-1202.cs.illinois.edu:9082", 
    "fa24-cs425-1203.cs.illinois.edu:9083", 
    "fa24-cs425-1204.cs.illinois.edu:9084", 
    "fa24-cs425-1205.cs.illinois.edu:9085", 
    // "fa24-cs425-1206.cs.illinois.edu:9086", 
    // "fa24-cs425-1207.cs.illinois.edu:9087", 
    // "fa24-cs425-1208.cs.illinois.edu:9088", 
    // "fa24-cs425-1209.cs.illinois.edu:9089",
    // "fa24-cs425-1210.cs.illinois.edu:9080",
}

// Function to join system
func JoinSystem(address string) {
    if machine_address == introducer_address {
        IntroducerJoin()
    } else {
        ProcessJoin(address)
    }
}

// Function to randomly select a node from the system and ping it
func PingClient(plus_s bool) {
    target_node := SelectRandomNode()
    index := FindNode(target_node.NodeID)
    if index < 0 {
        return
    }
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
    if err2 != nil { // no response was receieved
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
            susTimeout(6*time.Second, target_node.NodeID, target_node.Inc) // wait 6 seconds to recieve update about node status
            index := FindNode(target_node.NodeID)
            if index < 0 { // if node was removed
                for _, node := range(membership_list) { // let all other nodes know node has failed
                    SendMessage(node.NodeID, "fail", target_node.NodeID)
                }
            }

        }
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
}