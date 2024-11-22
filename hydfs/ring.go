package hydfs

import (
    "fmt"
    "github.com/emirpasic/gods/maps/treemap"
    "distributed_system/global"
    "distributed_system/util"
    "strings"
    "strconv"
    "sort"
    "net"
    "os"
    "io/ioutil"
    "log"
)


func ListMemRing(list_to_print []global.Node) {
    if len(list_to_print) == 0 {
        fmt.Println("List is empty.")
        return
    }

    nodeIDWidth := 56
    ringIDWidth := 4
    statusWidth := 4

    sort.Slice(list_to_print, func(i, j int) bool {
        return list_to_print[i].RingID < list_to_print[j].RingID
    })

    fmt.Printf("%-*s | %-*s | %-*s | %s | \n", ringIDWidth, "RingID", nodeIDWidth, "NodeID", statusWidth, "Status", "Incarnation #")
    fmt.Println(strings.Repeat("-", nodeIDWidth+statusWidth+ringIDWidth+30))

    // Go through membership list and print each entry
    for _, node := range list_to_print {
        fmt.Printf("%-*s | %s | %s  | %s\n",8,strconv.Itoa(node.RingID),node.NodeID, node.Status, strconv.Itoa(node.Inc))
    }
    fmt.Println()
    fmt.Print("> ")
}

func ListMem(list_to_print []global.Node) {
    if len(list_to_print) == 0 {
        fmt.Println("List is empty.")
        return
    }

    nodeIDWidth := 54
    statusWidth := 4

    fmt.Printf("%-*s | %-*s | %s\n", nodeIDWidth, "NodeID", statusWidth, "Status", "Incarnation #")
    fmt.Println(strings.Repeat("-", nodeIDWidth+statusWidth+25))

    // Go through membership list and print each entry
    for _, node := range list_to_print {
        fmt.Printf("%s | %s  | %s\n",node.NodeID, node.Status, strconv.Itoa(node.Inc))
    }
    fmt.Println()
    fmt.Print("> ")
}


func FindNodeWithPort(port string) int {
    for index,node := range(global.Membership_list) {
        if port == node.NodeID[:36] {
            return index
        }
    }
    return -1
}

// list the nodes in the ring map
func ListRing(treeMap *treemap.Map) {
    keys := treeMap.Keys()
    for _, hash := range keys {
        id, _ := treeMap.Get(hash)  // Get the value associated with the key
		fmt.Printf("Hash: %d, Node: %s\n", hash, id)
    }
}

func RereplicateRemove(id_to_rem string) {
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

// Checks if a new predecessor got added and files need to be updated
func NewJoin(joined_node string) {
    // get ring ids for both nodes
    self_id := util.GetTCPVersion(global.Node_id)
    joined_node = util.GetTCPVersion(joined_node)

    // get predecessors
    predecessors := util.GetPredecessors(self_id)

    dir := "./file-store" 

    // find all the prefixes for what files may need to be removed
    curr_prefix := global.Udp_address[13:15]
    first_pred_prefix, second_pred_prefix, third_pred_prefix := "","",""
    if len(predecessors[0]) > 0 {
        first_pred_prefix = predecessors[0][13:15]
    }
    if len(predecessors[1]) > 0 {
        second_pred_prefix = predecessors[1][13:15]
    }
    if len(predecessors[2]) > 0 {
        third_pred_prefix = predecessors[2][13:15]
    }

    // get all the files in the directory
    files, err := ioutil.ReadDir(dir)
    if err != nil {
        log.Fatal(err)
    }
    
    for i,p :=  range predecessors {
        if p == joined_node && i == 0 { // if it's immediate predecessor
            pred_hash := util.GetHash(p) // get the hash of the predecessor
            for _, file := range files {
                filename := file.Name()
                file_hash := util.GetHash(filename[3:])
                // find files with prefix of current server
                if !file.IsDir() && strings.HasPrefix(filename, curr_prefix) {
                    // if the hash now routes to predecessor change the prefix
                    if pred_hash >= file_hash && file_hash < util.GetHash(self_id) {
                        old_filename := "file-store/" + filename
                        new_filename := "file-store/" + p[13:15] + "-" + filename[3:]
                        os.Rename(old_filename, new_filename)
                    }
                }
                // find files with prefix of third predecessor and remove them
                if !file.IsDir() && strings.HasPrefix(filename, third_pred_prefix) {
                    err := os.Remove(dir + "/" + filename)
                    if err != nil {
                        fmt.Println("Error removing file:", err)
                    }
                }
            }
        } else if p == joined_node && i == 1{ // if it's second predecessor
            second_pred_hash := util.GetHash(p)
            pred_hash := util.GetHash(predecessors[0])
            for _, file := range files {
                filename := file.Name()
                file_hash := util.GetHash(filename[3:])
                // find files with prefix of first predecessor
                if !file.IsDir() && strings.HasPrefix(filename, first_pred_prefix) {
                    // if the hash now routes to second predecessor change the prefix
                    if second_pred_hash >= file_hash && file_hash < pred_hash {
                        old_filename := "file-store/" + filename
                        new_filename := "file-store/" + second_pred_prefix + "-" + filename[3:]
                        os.Rename(old_filename, new_filename)
                    }
                }
                // find files with prefix of third predecessor and remove
                if !file.IsDir() && strings.HasPrefix(filename, third_pred_prefix) {
                    err := os.Remove(dir + "/" + filename)
                    if err != nil {
                        fmt.Println("Error removing file:", err)
                    }
                }
            }
        } else if p == joined_node && i == 2 { // if it's third predecessor
            third_pred_hash := util.GetHash(p)
            second_pred_hash := util.GetHash(predecessors[1])
            for _, file := range files {
                filename := file.Name()
                file_hash := util.GetHash(filename[3:])
                // find files with prefix of second predecessor
                if !file.IsDir() && strings.HasPrefix(filename, second_pred_prefix) {
                    // if the hash now routes to third predecessor remove
                    if third_pred_hash >= file_hash && file_hash < second_pred_hash {
                        err := os.Remove(dir + "/" + filename)
                        if err != nil {
                            fmt.Println("Error removing file:", err)
                        }
                    }
                }
            }
        }
    }
}

// Handles a process pulling files from successors and predecessors when it joins the system
func SelfJoin(ring_id string) {
    // find successor and connect
    successor := util.GetSuccessor(ring_id)
    if len(successor) == 0 {
        return
    }
    successor_port := successor[:36]

    if successor_port != global.Tcp_address {
        conn_successor, err := net.Dial("tcp", successor_port)
        if err != nil {
            fmt.Println("Error connecting to successor when joining system: ", err)
        }
        defer conn_successor.Close()

        // Send a split message to the successor
        fmt.Fprintln(conn_successor, "split " + ring_id)
        data := global.Message{
            Action: "split" + ring_id,
            Filename:  "",
            FileContents: "",
        }
        util.GetFiles(conn_successor, data)
    }

    // find the predecessors
    predecessors := util.GetPredecessors(ring_id)
    // go through each predecessor
    for i,p :=  range predecessors {
        if i == 2 || len(p) == 0 { // if it's the third predecessor or empty continue
            continue
        }
        // connect to the predecesorr
        pred_port := p[:36]
        if pred_port != global.Tcp_address {
            conn_pred, err := net.Dial("tcp", pred_port)
            if err != nil {
                fmt.Println("Error connecting to server:", err)
            }
            defer conn_pred.Close()

            // Send a global.Message to the server
            data := global.Message{
                Action: "pull",
                Filename:  "",
                FileContents: "",
            }
            util.GetFiles(conn_pred,data)
        }
    }
}
