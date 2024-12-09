package main

import (
	"fmt"
	"os"
	"strings"
	"strconv"
)

func GetWordCount(contents string, word string) string {
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
			result := fmt.Sprintf("%s %d\n", word, count+1)
			return result // Return the first matching count
		}
	}
	result := fmt.Sprintf("%s %d\n", word, 1)
	return result
}


func main() {
	word := os.Args[1]
	content := os.Args[2]
	result := GetWordCount(content, word)
	fmt.Println(result)
}