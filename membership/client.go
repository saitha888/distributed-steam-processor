package membership

import (
    "fmt"
    "time"
    "strconv"
    "distributed_system/global"
    "distributed_system/util"
)

// Function to randomly select a node from the system and ping it
func PingClient(plus_s bool) {
    target_node := util.SelectRandomNode()
    index := util.FindNode(target_node.NodeID)
    if index < 0 {
        return
    }
    global.Enabled_sus = plus_s
    target_addr := target_node.NodeID[:36]
    // Connect to the randomly selected node
    conn, err := util.DialUDPClient(target_addr)
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
            message := "Node failure detected for: " + target_node.NodeID + " from machine " + global.Udp_port + " at " + time.Now().Format("15:04:05") + "\n"
            util.AppendToFile(message, global.Membership_log)
            RemoveNode(target_node.NodeID)
            for _,node := range global.Membership_list {
                util.SendMessage(node.NodeID, "fail", target_node.NodeID)
            }
        }
        if plus_s && util.CheckStatus(target_node.NodeID) != " sus "  { // if there is suspicion and the node isn't already sus
            message := "Node suspect detected for: " + target_node.NodeID + " from machine " + global.Udp_port + " at " + time.Now().Format("15:04:05") + "\n"
            util.AppendToFile(message, global.Membership_log)
            for _,node := range global.Membership_list { // let all machines know node is suspected
                util.SendMessage(node.NodeID, "suspected",target_node.NodeID)
            }
            susTimeout(6*time.Second, target_node.NodeID, target_node.Inc) // wait 6 seconds to recieve update about node status
            index := util.FindNode(target_node.NodeID)
            if index < 0 || util.CheckStatus(target_node.NodeID) != "alive" { // if node was removed
                RemoveNode(target_node.NodeID)
                for _, node := range(global.Membership_list) { // let all other nodes know node has failed
                    util.SendMessage(node.NodeID, "fail", target_node.NodeID)
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
        index := util.FindNode(recieved_node_id)
        if index >= 0 {
            if global.Membership_list[index].Status == " sus " || global.Membership_list[index].Inc < recieved_inc { // If the machine was suspected it is now cleared
                message := "Node suspect cleared for: " + target_node.NodeID + " from machine " + global.Udp_port + " at " + time.Now().Format("15:04:05") + "\n"
                util.AppendToFile(message, global.Membership_log)
                for _,node := range global.Membership_list { // let all machines know suspected node is alive
                    util.SendAlive(node.NodeID, target_node.NodeID, recieved_inc_str)
                }
            }
            // update status and inc number
            global.Membership_list[index].Status = "alive"
            global.Membership_list[index].Inc = recieved_inc
        }
    }
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
            index := util.FindNode(sus_id)
            if index >= 0 && global.Membership_list[index].Inc > inc_num {
                message := "Node suspect removed for: " + sus_id + "\n"
                util.AppendToFile(message, global.Membership_log)
                return
            }
		}
	}
}

