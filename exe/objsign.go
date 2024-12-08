package main

import (
	"fmt"
	"os"
	"strings"
)

func extractObjectIDAndSignType(value string) string {
	columns := strings.Split(value, ",")

	objectID := columns[2] 
	signType := columns[3]

	result := fmt.Sprintf("%s %s", objectID, signType)
	return result

}

func main() {
	value := os.Args[2]

	fmt.Println(extractObjectIDAndSignType(value))
}
