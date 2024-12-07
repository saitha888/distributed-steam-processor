package main

import (
	"fmt"
	"os"
	"strings"
)

func filterSignByX(key string, value string, pattern string) string {
	result := ""
	fields := strings.Split(value, ",")

	if fields[6] == pattern {
		result = fmt.Sprintf("%s %s", key, value)
	}
	return result
}

func main() {
	key := os.Args[1]  
	value := os.Args[2]
	pattern := os.Args[3]

	result := filterSignByX(key, value, pattern)
	if result != "" {
		fmt.Println(result)
	}
}