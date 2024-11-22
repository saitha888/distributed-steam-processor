package servers

import (
    "fmt"
    "time"
    "strconv"
    "distributed_system/global"
	"distributed_system/util"
	"distributed_system/membership"
    "distributed_system/hydfs"
)

//starts udp server that listens for pings
func UdpServer() {
    conn, _ :=  util.ConnectToMachine(global.Udp_port)
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
            result := util.MembershiplistToString()
            conn.WriteToUDP([]byte(result), addr)
        } else if message == "ping" { // machine checking health
            ack := global.Node_id + " " + strconv.Itoa(global.Inc_num)
            conn.WriteToUDP([]byte(ack), addr)
        } else if message[:4] == "fail" { // machine failure detected
            failed_node := message[5:]
            membership.RemoveNode(failed_node)
            message := "Node failure message recieved for: " + failed_node + " at " + time.Now().Format("15:04:05") + "\n"
            util.AppendToFile(message, global.Membership_log)
        } else if message[:4] == "join" { // new machine joined
            recieved_node := message[5:]
            if global.Udp_address == global.Introducer_address {
                // get the node id and timestamp
                message := "Node join detected for: " + recieved_node + " at " + time.Now().Format("15:04:05") + "\n"
                util.AppendToFile(message, global.Membership_log)
                index := membership.FindNode(recieved_node)
                if index >= 0 { // node is already in membership list
                    util.ChangeStatus(index, "alive")
                } else { // need to add new node
                    membership.AddNode(recieved_node, 1, "alive")
                }

                // send membership list back 
                result := util.MembershiplistToString()
                conn.WriteToUDP([]byte(result), addr)
                
                // send to all other members that new node joined
                for _,node := range global.Membership_list {
                    if node.Status == "alive" {
                        node_address := node.NodeID[:36]
                        if node_address != global.Udp_address { // check that it's not self
                            conn, _ := membership.DialUDPClient(node_address)

                            result := "join " + recieved_node
                            // send join message
                            conn.Write([]byte(result))
                        }
                    }
                }
                hydfs.HandleRingJoin(recieved_node)
            } else {
                if recieved_node[:36] != global.Udp_address {
                    membership.ProcessJoinMessage(message)
                }
            }
        } else if message[:5] == "leave" { // machine left
            left_node := message[6:]
            index := membership.FindNode(left_node)
            if index >= 0 { // machine was found
                util.ChangeStatus(index, "leave")
            }
            message := "Node leave detected for: " + left_node + " at " + time.Now().Format("15:04:05") + "\n"
            util.AppendToFile(message, global.Membership_log)
        } else if message[:9] == "suspected" { // machine left
            if global.Enabled_sus {
                sus_node := message[10:]
                message := "Node suspect detected for: " + sus_node + " at " + time.Now().Format("15:04:05") + "\n"
                util.AppendToFile(message, global.Membership_log)
                index := membership.FindNode(sus_node)
                if sus_node == global.Node_id {
                    fmt.Println("Node is currently suspected")
                    global.Inc_num += 1
                    if index >= 0 { // machine was found
                        global.Membership_list[index].Inc = global.Inc_num
                    }
                } else {
                    if index >= 0 { // machine was found
                        util.ChangeStatus(index, " sus ")
                    }
                }
            }
        } else if message[:5] == "alive" { // machine unsuspected
            alive_node := message[6:62]
            inc_num, _ := strconv.Atoi(message[63:])
            message := "Suspected node cleared for: " + alive_node + " at " + time.Now().Format("15:04:05") + "\n"
            util.AppendToFile(message, global.Membership_log)
            index := membership.FindNode(alive_node)
            if index >= 0 {
                util.ChangeStatus(index, "alive")
                util.ChangeInc(index, inc_num)
            }
        }   
    }
}
