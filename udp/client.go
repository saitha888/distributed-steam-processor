package udp

import (
    "fmt"
    "time"
    "strconv"
)

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
            AppendToFile(message, logfile)
            RemoveNode(target_node.NodeID)
            for _,node := range membership_list {
                SendMessage(node.NodeID, "fail", target_node.NodeID)
            }
        }
        if plus_s && checkStatus(target_node.NodeID) != " sus "  { // if there is suspicion and the node isn't already sus
            message := "Node suspect detected for: " + target_node.NodeID + " from machine " + udp_port + " at " + time.Now().Format("15:04:05") + "\n"
            AppendToFile(message, logfile)
            for _,node := range membership_list { // let all machines know node is suspected
                SendMessage(node.NodeID, "suspected",target_node.NodeID)
            }
            susTimeout(6*time.Second, target_node.NodeID, target_node.Inc) // wait 6 seconds to recieve update about node status
            index := FindNode(target_node.NodeID)
            fmt.Println("status of node that was suspected: " + membership_list[index].NodeID)
            if index < 0 { // if node was removed
                for _, node := range(membership_list) { // let all other nodes know node has failed
                    SendMessage(node.NodeID, "fail", target_node.NodeID)
                }
            }

        }
    } else { // response was recieved
        ack := string(buf[:n])
        if len(ack) < 56 {
            return
        }
        recieved_node_id := ack[:56]
        recieved_inc_str := ack[57:]
        recieved_inc, _ := strconv.Atoi(ack[57:])
        index := FindNode(recieved_node_id)
        if index >= 0 {
            if membership_list[index].Status == " sus " || membership_list[index].Inc < recieved_inc { // If the machine was suspected it is now cleared
                message := "Node suspect cleared for: " + target_node.NodeID + " from machine " + udp_port + " at " + time.Now().Format("15:04:05") + "\n"
                AppendToFile(message, logfile)
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