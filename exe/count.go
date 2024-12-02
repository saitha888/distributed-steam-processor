package main

import (
	"fmt"
	"github.com/gofrs/flock"
	"io/ioutil"
	"os"
	"strings"
	"strconv"
)

func PerformTask(word string) {
	// Create a file lock
	fileLock := flock.New("counts.txt")

	// Acquire the lock
	locked, err := fileLock.TryLock()
	if err != nil {
		fmt.Println("Error acquiring lock:", err)
		return
	}
	if !locked {
		fmt.Println("File is locked by another process")
		return
	}
	defer fileLock.Unlock() // Ensure the lock is released

	// Read the file
	data, err := ioutil.ReadFile("counts.txt")
	if err != nil {
		fmt.Println("Error reading file:", err)
		return
	}

	word_count := GetWordCount(string(data), word)

	new_word_count := word_count + 1

	// Open the file in append mode, creating it if it doesn't exist
	file, err := os.OpenFile("counts.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	// Append the new word and count to the file
	_, err = file.WriteString(fmt.Sprintf("%s %d\n", word, new_word_count))
	if err != nil {
		fmt.Println("Error writing to file:", err)
		return
	}

}

func GetWordCount(contents string, word string) int {
	lines := strings.Split(contents, "\n") // Split contents into lines

	// Iterate over lines in reverse order
	for i := len(lines) - 1; i >= 0; i-- {
		line := lines[i]

		// Split the line into parts (word and count)
		parts := strings.Fields(line)
		if len(parts) != 2 {
			continue // Skip invalid lines
		}

		// Check if the word matches
		if parts[0] == word {
			count, err := strconv.Atoi(parts[1]) // Convert the count to an integer
			if err != nil {
				continue // Skip if the count is not a valid number
			}
			return count // Return the first matching count
		}
	}

	return -1 // Return -1 if the word is not found
}


func main() {
	word := os.Args[1]
	fmt.Println("got message in count exe to count for: ", word)
	PerformTask(word)
}