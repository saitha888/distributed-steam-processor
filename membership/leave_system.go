package membership

import (
    "fmt"
    "distributed_system/global"
    "distributed_system/util"
    "distributed_system/hydfs"
)

//Function to leave the system
func LeaveList() {
    // Change own status to left, inform other machines to change status to left
    for i,node :=  range global.Membership_list {
        if node.NodeID == global.Node_id { // check if at self
            util.ChangeStatus(i, "leave")
        } else { 
            node_address := node.NodeID[:36]
            conn, err := DialUDPClient(node_address)
            defer conn.Close()

            // Send leave message
            message := fmt.Sprintf("leave " + global.Node_id)
            _, err = conn.Write([]byte(message))
            if err != nil {
                fmt.Println("Error sending leave message:", err)
                return
            }
        }
    }
}

// Remove a machine from the system
func RemoveNode(id_to_rem string) {

    //remove from membership list
    for index,node := range global.Membership_list {
        if id_to_rem == node.NodeID { // remove the node if it's found
            global.Membership_list = append(global.Membership_list[:index], global.Membership_list[index+1:]...)
        }
    }
    //remove node from ring and update file storing
    hydfs.HandleRingRemove(id_to_rem)
}
