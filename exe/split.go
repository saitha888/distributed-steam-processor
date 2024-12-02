
package main 
import (
	"fmt"
	"os"
	"strings"
)

func parseAndTransform(key string, value string) []string {
	// Split the value (file content) into words
	words := strings.Fields(value)
	var wordTuples []string

	// Create tuples <word, 1>
	for _, word := range words {
		wordTuples = append(wordTuples, fmt.Sprintf("(%s, 1)", word))
	}

	return wordTuples
}

func main() {
	// Ensure correct number of arguments
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "Usage: go run main.go <key> <value>")
		os.Exit(1)
	}

	key := os.Args[1]   // First argument: key (e.g., filename:linenumber)
	value := os.Args[2] // Second argument: value (e.g., file content)

	// Call the function to parse and transform the input
	result := parseAndTransform(key, value)

	// Print the output tuples
	for _, tuple := range result {
		fmt.Println(tuple)
	}
}