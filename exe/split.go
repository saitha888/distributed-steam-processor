
package main 
import (
	"fmt"
	"os"
	"strings"
	"unicode"
)

func parseAndTransform(key string, value string) []string {
	// Remove punctuation by filtering non-letter and non-digit characters
	cleanedValue := strings.Map(func(r rune) rune {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || unicode.IsSpace(r) {
			return r
		}
		return -1
	}, value)
	cleanedValue = strings.ToLower(cleanedValue)
	// Split the cleaned value into words
	words := strings.Fields(cleanedValue)
	var wordTuples []string

	// Create tuples <word, 1>
	for _, word := range words {
		wordTuples = append(wordTuples, fmt.Sprintf("(%s, 1)", word))
	}

	return wordTuples
}

func main() {
	// Ensure correct number of arguments

	key := os.Args[1]   // First argument: key (e.g., filename:linenumber)
	value := os.Args[2] // Second argument: value (e.g., file content)

	// Call the function to parse and transform the input
	result := parseAndTransform(key, value)

	// Print the output tuples
	for _, tuple := range result {
		fmt.Println(tuple)
	}
}