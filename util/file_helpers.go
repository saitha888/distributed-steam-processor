package util

import (
	"os"
)

// Function to write content to a local file
func WriteToFile(filename string, content string) error {
    file, err := os.Create(filename)
    if err != nil {
        return err
    }
    defer file.Close()

    _, err = file.WriteString(content)
    if err != nil {
        return err
    }
    return nil
}

