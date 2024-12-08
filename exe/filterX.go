package main

import (
	"fmt"
	"os"
	"strings"
)

func filterByPatternX(key string, value string, pattern string) string {
	result := ""
	value = strings.ReplaceAll(value, "\n", "")
	value = strings.ReplaceAll(value, "\"", "")

	if strings.Contains(value, pattern) {
		result = fmt.Sprintf("%s %s", key, value)
	}

	return result
}

func main() {
	key := os.Args[1]  
	value := os.Args[2]
	pattern := os.Args[3]

	result := filterByPatternX(key, value, pattern)
	if result != "" {
		fmt.Println(result)
	}
}
