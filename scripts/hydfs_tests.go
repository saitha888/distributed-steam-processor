package scripts
 
import (
	"distributed_system/global"
	"distributed_system/hydfs"
	"strconv"
	"math/rand"
	"time"
	"io/ioutil"
	"fmt"
	"strings"
	"path/filepath"
	"os"
)

func TestCreate(local_filenames [5]string) {
	hydfs_filenames := [5]string{"file_cat", "file_zebra", "file_kangaroo", "file_chimpanzee", "file_aardvark"}
	for i := 0; i < 5; i++ {
		local_file := "local-files/" + local_filenames[i]
		hydfs_file := hydfs_filenames[i]
		hydfs.CreateFile(local_file, hydfs_file)
	}
}

func TestAppend() {
	hydfs.AppendFile("local-files/business_9.txt", "file_zebra")
	hydfs.AppendFile("local-files/business_20.txt", "file_zebra")
}


func MergePerformance(num_clients string, append_size string) {
	// create the 10 mb initial file
	localfilename := "10mb_file.txt"
	HyDFSfilename := "merge_performance"
	hydfs.CreateFile(localfilename, HyDFSfilename)

	// populate client list
	client_num, _ := strconv.Atoi(num_clients)
	clients := GetRandomClients(client_num)

	// find number of iterations
	iterations := 1000 / client_num

	// populate local files list
	var local_files []string

	for j := 0; j < client_num; j++ {
		if append_size == "4" {
			local_files = append(local_files,"4kb_file.txt")
		} else {
			local_files = append(local_files,"40kb_file.txt")
		}
	}

	// launch multi append specific number of times
	for i := 0; i < iterations; i++ {
		hydfs.MultiAppend("merge_performance", clients, local_files)
	}
}

func GetRandomClients(num_clients int) [] string{
	membership_list := global.Membership_list
	rand.Seed(time.Now().UnixNano())

	selected := make(map[int]bool)
	result := make([]string, 0, num_clients)

	for len(result) < num_clients {
		index := rand.Intn(len(membership_list))
		if !selected[index] {
			selected[index] = true
			result = append(result, membership_list[index].NodeID[:36])
		}
	}
	return result
}
func CachePerformance() {
	LoadFiles("./dataset")
	RunGets(true)
	RunGets(false)
}

var loaded = false

func LoadFiles(folder_name string) {
	if !loaded {
		datasetFolder := folder_name

		files, err := ioutil.ReadDir(datasetFolder)
		if err != nil {
			fmt.Println("Error reading directory:", err)
			return
		}
	
		for _, file := range files {
			if !file.IsDir() && strings.HasSuffix(file.Name(), ".txt") {
				localFilename := datasetFolder + "/" + file.Name()
				HyDFSfilename := file.Name()
				hydfs.CreateFile(localFilename, HyDFSfilename)
			}
		}
	
		fmt.Println("Completed loading dataset into HyDFS.")
		loaded = true
	}
}

func createDirectory(dirName string) {
	err := os.MkdirAll(dirName, os.ModePerm)
	if err != nil {
		fmt.Printf("Error creating directory %s: %v\n", dirName, err)
	}
}


func RunGets(append bool) {
	rand.Seed(time.Now().UnixNano())

	createDirectory("random")
	createDirectory("not-random")

	start := time.Now()
	
	// Perform random selection
	for i := 0; i < 20000; i++ {
		fileIndex := rand.Intn(10000) + 1 
		hydfsFile := fmt.Sprintf("file_%d", fileIndex)
		localFile := filepath.Join("random", fmt.Sprintf("file_copy_%d.txt", fileIndex))
		if append && rand.Float64() < 0.1 {
			hydfs.AppendFile(localFile, hydfsFile) // 10% chance to append
		} else {
			hydfs.GetFile(hydfsFile, localFile) // 90% chance to get
		}
	}
	stop1 := time.Since(start).String()
	

	start2 := time.Now()
	// Perform weighted selection
	for i := 0; i < 20000; i++ {
		var fileIndex int
		// 70% chance to pick from the first 50 files
		if rand.Float64() < 0.5 {
			fileIndex = rand.Intn(50) + 1
		} else {
			fileIndex = rand.Intn(10000) + 1
		}
		hydfsFile := fmt.Sprintf("file_%d", fileIndex)
		localFile := filepath.Join("not-random", fmt.Sprintf("file_copy_%d.txt", fileIndex))
		if append && rand.Float64() < 0.1 {
			hydfs.AppendFile(localFile, hydfsFile) // 10% chance to append
		} else {
			hydfs.GetFile(hydfsFile, localFile) // 90% chance to get
		}
	}
	stop2 := time.Since(start2).String()
	outputFile := "cache_performance_output.txt"
	file, err := os.Create(outputFile)
	if err != nil {
		fmt.Printf("Error creating output file: %v\n", err)
		return
	}
	defer file.Close()

	// Write latency results to the file
	_, err = file.WriteString(fmt.Sprintf("Latency of random reads: %s\n", stop1))
	if err != nil {
		fmt.Printf("Error writing to file: %v\n", err)
		return
	}
	_, err = file.WriteString(fmt.Sprintf("Latency of weighted reads: %s\n", stop2))
	if err != nil {
		fmt.Printf("Error writing to file: %v\n", err)
		return
	}

	fmt.Println("Latency results written to cache_performance_output.txt")
}

