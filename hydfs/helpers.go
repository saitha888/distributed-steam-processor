package hydfs

import (
    "fmt"
    "github.com/emirpasic/gods/maps/treemap"
    "distributed_system/global"
    "strings"
    "strconv"
    "sort"
    "os"
    "io/ioutil"
    "log"
    "regexp"
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