package udp

import (
    "fmt"
    "net"
)

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

// Remove a machine from the membership list
func RemoveNode(id_to_rem string) {
    bytes := []byte(node_id)
	bytes[32] = '8'
	
	node_id = string(bytes)

    for index,node := range membership_list {
        if id_to_rem == node.NodeID { // remove the node if it's found
            membership_list = append(membership_list[:index], membership_list[index+1:]...)
        }
    }

    bytes_remove := []byte(id_to_rem)
	
	bytes_remove[32] = '8'
	
	id_to_remove := string(bytes_remove)

    ring_map := GetRing()
    iterator := IteratorAt(ring_map, id_to_remove)
    id := ""
    if (!iterator.Next()) {
        iterator.First()
    }
    id = iterator.Value().(string)
    if (id == node_id) {
        //if removed node is right before this node
        //this node becomes new origin for failed node, rename files
        RenameFilesWithPrefix(id_to_remove[13:15], node_id[13:15])

        //pull files of origin n-3
        nod := IteratorAtNMinusSteps(ring_map, node_id, 3)
        port := nod[:36]
        // pull for files
        conn_pred, err := net.Dial("tcp", port )
        if err != nil {
            fmt.Println(err)
            return
        }
        defer conn_pred.Close()
        data := Message{
            Action: "pull",
            Filename:  "",
            FileContents: "",
        }
        GetFiles(conn_pred, data)
    }
    id2 := ""
    if (!iterator.Next()) {
        iterator.First()
    }
    id2 = iterator.Value().(string)
    if (id2 == node_id) {
        //if removed node is 2 nodes before this node
        //rename files of origin n-2 to n-1 
        RenameFilesWithPrefix(IteratorAtNMinusSteps(ring_map, node_id, 2)[13:15], IteratorAtNMinusSteps(ring_map, node_id, 1)[13:15])

        //pull files of origin n-3
        nod := IteratorAtNMinusSteps(ring_map, node_id, 3)
        port := nod[:36]
        // pull for files
        conn_pred, err := net.Dial("tcp", port )
        if err != nil {
            fmt.Println(err)
            return
        }
        defer conn_pred.Close()
        data := Message{
            Action: "pull",
            Filename:  "",
            FileContents: "",
        }
        GetFiles(conn_pred, data)
    } 
    id3 := ""
    if (!iterator.Next()) {
        iterator.First()
    }
    //3, 1, 2, 4, 5
    id3 = iterator.Value().(string)
    if (id3 == node_id) {
        nod := IteratorAtNMinusSteps(ring_map, node_id, 2)
        port := nod[:36]
        // pull for files
        conn_pred, err := net.Dial("tcp", port )
        if err != nil {
            fmt.Println(err)
            return
        }
        defer conn_pred.Close()
        data := Message{
            Action: fmt.Sprintf("pull-3 %s", id_to_remove),
            Filename:  "",
            FileContents: "",
        }
        GetFiles(conn_pred,data)
    }
    ring_map.Remove(GetHash(id_to_remove))
}