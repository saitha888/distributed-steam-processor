package scripts

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

func GenerateDataset() {
	// Define the original file path and destination folder
	originalFile := "4kb_file.txt"
	destinationFolder := "dataset"

	// Create the destination folder if it doesn't exist
	err := os.MkdirAll(destinationFolder, os.ModePerm)
	if err != nil {
		fmt.Println("Error creating directory:", err)
		return
	}

	// Create 10,000 copies of the file
	for i := 1; i <= 10000; i++ {
		newFileName := fmt.Sprintf("file_%d.txt", i)
		newFilePath := filepath.Join(destinationFolder, newFileName)

		err := copyFile(originalFile, newFilePath)
		if err != nil {
			fmt.Println("Error copying file:", err)
			return
		}
	}

	fmt.Println("Completed creating 10,000 copies of the file.")
}

// copyFile copies a file from src to dst
func copyFile(src, dst string) error {
	sourceFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	destinationFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destinationFile.Close()

	_, err = io.Copy(destinationFile, sourceFile)
	return err
}

