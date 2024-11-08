package membership

import (
    "fmt"
    "crypto/sha256"
    "github.com/emirpasic/gods/maps/treemap"
    "encoding/binary"
)

// iteratorAt finds the iterator positioned at the given key
func IteratorAt(ringMap *treemap.Map, start_val string) *treemap.Iterator {
	iterator := ringMap.Iterator()
	for iterator.Next() {
		if iterator.Value().(string) == start_val {
			// Return the iterator at the position of startKey
			return &iterator
		}
	}
	// Return nil if the key is not found
    iterator.First()
	return &iterator
}

// IteratorAtNMinusSteps moves backward by `steps` from the position of `start_val`, wrapping around if necessary.
func IteratorAtNMinusSteps(ringMap *treemap.Map, start_val string, steps int) string {
	iterator := ringMap.Iterator()
	found := false

	// Locate the starting position of `start_val`
	for iterator.Next() {
		if iterator.Value().(string) == start_val {
			found = true
			break
		}
	}

	// If `start_val` is not found, return an empty string
	if !found {
		return ""
	}

	// Move backward by `steps`, wrapping around as necessary
	for i := 0; i < steps; i++ {
		// Attempt to move backward
		if !iterator.Prev() {
			// If at the beginning, wrap around to the last element
            iterator.First()
            temp := iterator
			for iterator.Next() {
                temp = iterator
            } // Move to the last element
            iterator = temp
		}
        fmt.Printf("%s is at n-%d\n", iterator.Value().(string), i+1)
	}

	// Return the value at the final position
	return iterator.Value().(string)
}

// Get the deterministic hash of a string
func GetHash(data string) int {
	hash := sha256.Sum256([]byte(data))
    truncated_hash := binary.BigEndian.Uint64(hash[:8])
    ring_hash := truncated_hash % 2048
	return (int)(ring_hash)
}

// list the nodes in the ring map
func ListRing(treeMap *treemap.Map) {
    keys := treeMap.Keys()
    for _, hash := range keys {
        id, _ := treeMap.Get(hash)  // Get the value associated with the key
		fmt.Printf("Hash: %d, Node: %s\n", hash, id)
    }
}

// Get the 3 nodes before a node in the ring
func GetPredecessors(self_id string) [3]string{
    var prev1, prev2, prev3 string

	// Create an iterator to go through the ring map
	it := ring_map.Iterator()

	for it.Next() { // get the three predecessors
		if it.Value().(string) == self_id {
			break
		}
		prev3 = prev2
		prev2 = prev1
		prev1 = it.Value().(string)
	}

	if prev1 == "" { // if the first predecessor wasn't set (current node is at the start of map)
		_, v1 := ring_map.Max()
		prev1 = v1.(string)
	}
	if prev2 == "" { // if the second predecessor wasn't set
		_, max_value := ring_map.Max()
		if prev1 == max_value.(string) { // if the first predecessor is already the last map value
			it = ring_map.Iterator()
			for it.Next() {
				if it.Value().(string) == prev1 { // find the value before the first predecessor
					break
				}
				prev2 = it.Value().(string)
			}
		} else { // set to last value in map
			prev2 = max_value.(string)
		}
	}
	if prev3 == "" { // if the third predecessor wasn't set
		_, max_value := ring_map.Max()
		if prev1 == max_value.(string) || prev2 == max_value.(string) { // if the first or second predecessor is already the last map value
			it = ring_map.Iterator()
			for it.Next() {
				if it.Value().(string) == prev2 { // find the value before the second predecessor
					break
				}
				prev3 = it.Value().(string)
			}
		} else { // set to last value in map
			prev3 = max_value.(string)
		}
	}

	predecessors := [3]string{prev1, prev2, prev3}
    return predecessors
}

// Get the 1 node after a node in the ring
func GetSuccessor(ring_id string) string{
    successor := ""

    //iterate through the ring map
    it := ring_map.Iterator()
    for it.Next() {
		if it.Value().(string) == ring_id { // if we found the current value
            if it.Next() { // set the succesor to the next value if it's valid
				successor = it.Value().(string)
			} else { // set to first value in map (wrap around)
				_, successor_val := ring_map.Min()
                successor = successor_val.(string)
			}
		}
	}
    return successor
}