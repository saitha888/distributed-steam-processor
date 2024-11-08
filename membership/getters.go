package membership

import (
    "github.com/emirpasic/gods/maps/treemap"
)

func GetNodeID() string {
    return node_id
}

func GetRing() *treemap.Map {
    return ring_map
}

func GetMembershipList() []Node {
	return membership_list
}