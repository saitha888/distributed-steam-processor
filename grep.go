package main

import (
	"strconv"
	"strings"
	"os/exec"
	"fmt"
)

func main() {
	// get the hydfs file and place it in local file
	localfilename := "grep_file.txt"

	// grep the file
	// pattern = strings.TrimSpace(pattern)
	command := "grep -c " + "-- " + "-9121578439823682884" + " " + localfilename
	cmd := exec.Command("sh", "-c", command)
	output, err := cmd.CombinedOutput()

	if err != nil { // couldn't find pattern
		fmt.Println(err)
	}

	// return the line count
	line_count, _ := strconv.Atoi(strings.TrimSpace(string(output)))
	fmt.Println(line_count)
}


