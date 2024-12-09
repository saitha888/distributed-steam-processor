package hydfs

import (
    "fmt"
    "github.com/emirpasic/gods/maps/treemap"
    "distributed_system/global"
    "distributed_system/util"
    "strings"
    "strconv"
    "sort"
    "os"
    "io/ioutil"
    "log"
    "regexp"
    "net"
    "encoding/json"
    "io"
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

// renameFilesWithPrefix renames files in the "filestore" directory that start with oldPrefix to start with newPrefix
func RenameFilesWithPrefix(oldPrefix string, newPrefix string) {
	dir := "file-store"

	// Read the directory contents
	files, err := ioutil.ReadDir(dir)
	if err != nil {
        fmt.Println("cannot get to directory")
	}

	// Regular expression to match filenames starting with the oldPrefix followed by a dash
	re := regexp.MustCompile(fmt.Sprintf(`^(%s)-(.*)`, oldPrefix))

	// Iterate through all the files
	for _, file := range files {
		// Get the file name
		oldName := file.Name()

		// Use regex to check if the filename starts with oldPrefix and a dash
		matches := re.FindStringSubmatch(oldName)
		if matches == nil {
			// If there's no match, skip the file
			continue
		}

		// Create the new filename with newPrefix instead of oldPrefix
		newName := fmt.Sprintf("%s-%s", newPrefix, matches[2])

		// Construct full paths for renaming
		oldPath := fmt.Sprintf("%s/%s", dir, oldName)
		newPath := fmt.Sprintf("%s/%s", dir, newName)

		// Rename the file
		err = os.Rename(oldPath, newPath)
		if err != nil {
			log.Printf("Error renaming file %s to %s: %v", oldPath, newPath, err)
		} else {
			fmt.Printf("Renamed %s to %s\n", oldName, newName)
		}
	}
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

// Get the 3 nodes before a node in the ring
func GetPredecessors(self_id string) [3]string{
    var prev1, prev2, prev3 string

	// Create an iterator to go through the ring map
	it := global.Ring_map.Iterator()

	for it.Next() { // get the three predecessors
		if it.Value().(string) == self_id {
			break
		}
		prev3 = prev2
		prev2 = prev1
		prev1 = it.Value().(string)
	}

	if prev1 == "" { // if the first predecessor wasn't set (current node is at the start of map)
		_, v1 := global.Ring_map.Max()
		prev1 = v1.(string)
	}
	if prev2 == "" { // if the second predecessor wasn't set
		_, max_value := global.Ring_map.Max()
		if prev1 == max_value.(string) { // if the first predecessor is already the last map value
			it = global.Ring_map.Iterator()
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
		_, max_value := global.Ring_map.Max()
		if prev1 == max_value.(string) || prev2 == max_value.(string) { // if the first or second predecessor is already the last map value
			it = global.Ring_map.Iterator()
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
    it := global.Ring_map.Iterator()
    for it.Next() {
		if it.Value().(string) == ring_id { // if we found the current value
            if it.Next() { // set the succesor to the next value if it's valid
				successor = it.Value().(string)
			} else { // set to first value in map (wrap around)
				_, successor_val := global.Ring_map.Min()
                successor = successor_val.(string)
			}
		}
	}
    return successor
}

func GetFiles(conn net.Conn, data global.Message) {
    encoder := json.NewEncoder(conn)
    err := encoder.Encode(data)
    if err != nil {
        fmt.Println("Error encoding data in pull-files in self join", err)
    } 

    decoder := json.NewDecoder(conn)
    for {
        var response global.Message
        err := decoder.Decode(&response)
        if err != nil {
            if err == io.EOF {
                // End of the response from the server
                break
            }
            fmt.Println("Error decoding message from server:", err)
            return
        }
        file, err := os.OpenFile("file-store/"+response.Filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
        if err != nil {
            fmt.Println(err)
        }
        defer file.Close()

        _, err = file.WriteString(response.FileContents)
        if err != nil {
            fmt.Println(err)
        }
    }
}

func RemoveFromCache(filename string) {
    dir := "./cache"

    files, err := ioutil.ReadDir(dir)
    if err != nil {
        fmt.Println("Error reading directory:", err)
    }

    for _, file := range files {
        curr_file := file.Name()
        if curr_file == filename {
            os.Remove(dir + "/" + filename)
        }
    }
    delete(global.Cache_set, filename)
}

func ListServers(HyDFSfilename string) {
    file_hash := util.GetHash(HyDFSfilename)
    fmt.Println("File ID: ", strconv.Itoa(file_hash))

    node_ids := GetFileServers(file_hash)
    for _,node := range node_ids {
        fmt.Println(node)
    }
}


func GetFileServers(file_hash int) []string {
    node_ids := []string{}
	iterator := global.Ring_map.Iterator()
	for iterator.Next() {
		if iterator.Key().(int)> file_hash {
			node_ids = append(node_ids, iterator.Value().(string))
			for i := 0; i < 2; i++ {
				if (iterator.Next()) {
					node_ids = append(node_ids, iterator.Value().(string))
				} else {
					iterator.First()
					node_ids = append(node_ids, iterator.Value().(string))
				}
			}
			break
		}
	} 
	if len(node_ids) == 0 {
		iterator.First()
		for i := 0; i < 3; i++ {
			node_ids = append(node_ids, iterator.Value().(string))
			iterator.Next()
		}
	}

    return node_ids
}

