package membership

import (
    "time"
    "math/rand"
    "distributed_system/global"
)

// Function to randomly select an alive node in the system
func SelectRandomNode() global.Node {
    rand.Seed(time.Now().UnixNano())
    var target_node global.Node
    for {
        random_index := rand.Intn(len(global.Membership_list))
        selected_node := global.Membership_list[random_index]
        if selected_node.NodeID != global.Node_id && selected_node.Status != "leave" { 
            target_node = selected_node
            break
        }
    }
    return target_node
}

// Get the index of a machine in the list
func FindNode(node_id string) int {
    for index,node := range global.Membership_list { 
        if node_id == node.NodeID {
            return index
        }
    }
    return -1
}

func CheckStatus(node string) string {
    index := FindNode(node)
    if index >= 0 {
        return global.Membership_list[index].Status
    }
    return "none"
}