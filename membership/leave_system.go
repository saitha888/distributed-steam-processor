package membership

import (
    "fmt"
    "net"
    "distributed_system/global"
    "distributed_system/util"
    
)

//Function to leave the system
func LeaveList() {
    // Change own status to left, inform other machines to change status to left
    for i,node :=  range global.Membership_list {
        if node.NodeID == global.Node_id { // check if at self
            util.ChangeStatus(i, "leave")
        } else { 
            node_address := node.NodeID[:36]
            conn, err := util.DialUDPClient(node_address)
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

// Remove a machine from the membership list
func RemoveNode(id_to_rem string) {


    for index,node := range global.Membership_list {
        if id_to_rem == node.NodeID { // remove the node if it's found
            global.Membership_list = append(global.Membership_list[:index], global.Membership_list[index+1:]...)
        }
    }

    bytes := []byte(global.Node_id)
	bytes[32] = '8'
	
	node_id := string(bytes)

    bytes_remove := []byte(id_to_rem)
	
	bytes_remove[32] = '8'
	
	id_to_remove := string(bytes_remove)

    iterator := util.IteratorAt(global.Ring_map, id_to_remove)
    id := ""
    if (!iterator.Next()) {
        iterator.First()
    }
    id = iterator.Value().(string)
    if (id == node_id) {
        //if removed node is right before this node
        //this node becomes new origin for failed node, rename files
        util.RenameFilesWithPrefix(id_to_remove[13:15], node_id[13:15])

        //pull files of origin n-3
        nod := util.IteratorAtNMinusSteps(global.Ring_map, node_id, 3)
        port := nod[:36]
        // pull for files
        conn_pred, err := net.Dial("tcp", port )
        if err != nil {
            fmt.Println(err)
            return
        }
        defer conn_pred.Close()
        data := global.Message{
            Action: "pull",
            Filename:  "",
            FileContents: "",
        }
        util.GetFiles(conn_pred, data)
    }
    id2 := ""
    if (!iterator.Next()) {
        iterator.First()
    }
    id2 = iterator.Value().(string)
    if (id2 == node_id) {
        //if removed node is 2 nodes before this node
        //rename files of origin n-2 to n-1 
        util.RenameFilesWithPrefix(util.IteratorAtNMinusSteps(global.Ring_map, node_id, 2)[13:15], util.IteratorAtNMinusSteps(global.Ring_map, node_id, 1)[13:15])

        //pull files of origin n-3
        nod := util.IteratorAtNMinusSteps(global.Ring_map, node_id, 3)
        port := nod[:36]
        // pull for files
        conn_pred, err := net.Dial("tcp", port )
        if err != nil {
            fmt.Println(err)
            return
        }
        defer conn_pred.Close()
        data := global.Message{
            Action: "pull",
            Filename:  "",
            FileContents: "",
        }
        util.GetFiles(conn_pred, data)
    } 
    id3 := ""
    if (!iterator.Next()) {
        iterator.First()
    }
    //3, 1, 2, 4, 5
    id3 = iterator.Value().(string)
    if (id3 == node_id) {
        nod := util.IteratorAtNMinusSteps(global.Ring_map, node_id, 2)
        port := nod[:36]
        // pull for files
        conn_pred, err := net.Dial("tcp", port )
        if err != nil {
            fmt.Println(err)
            return
        }
        defer conn_pred.Close()
        data := global.Message{
            Action: fmt.Sprintf("pull-3 %s", id_to_remove),
            Filename:  "",
            FileContents: "",
        }
        util.GetFiles(conn_pred,data)
    }
    global.Ring_map.Remove(util.GetHash(id_to_remove))
}
