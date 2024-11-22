package hydfs

import (
    "fmt"
    "distributed_system/global"
    "distributed_system/util"
    "net"
)

func HandleRingRemove(id_to_rem string) {
    bytes := []byte(global.Node_id)
	bytes[32] = '8'
	
	node_id := string(bytes)

    bytes_remove := []byte(id_to_rem)
	
	bytes_remove[32] = '8'
	
	id_to_remove := string(bytes_remove)

    iterator := IteratorAt(global.Ring_map, id_to_remove)
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
        nod := IteratorAtNMinusSteps(global.Ring_map, node_id, 3)
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
        RenameFilesWithPrefix(IteratorAtNMinusSteps(global.Ring_map, node_id, 2)[13:15], IteratorAtNMinusSteps(global.Ring_map, node_id, 1)[13:15])

        //pull files of origin n-3
        nod := IteratorAtNMinusSteps(global.Ring_map, node_id, 3)
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
        GetFiles(conn_pred, data)
    } 
    id3 := ""
    if (!iterator.Next()) {
        iterator.First()
    }
    //3, 1, 2, 4, 5
    id3 = iterator.Value().(string)
    if (id3 == node_id) {
        nod := IteratorAtNMinusSteps(global.Ring_map, node_id, 2)
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
        GetFiles(conn_pred,data)
    }
    global.Ring_map.Remove(util.GetHash(id_to_remove))
}