package test_scripts

import (
	"distributed_system/tcp"
)

func Test1(local_filenames [5]string) {
	hydfs_filenames := [5]string{"file1", "file2", "file3", "file4", "file5"}
	for i := 0; i < 5; i++ {
		local_file := local_filenames[i]
		hydfs_file := hydfs_filenames[i]
		tcp.CreateFile(local_file, hydfs_file)
	}
}

func Test4() {
	tcp.AppendFile("business_9.txt", "file1")
	tcp.AppendFile("business_20.txt", "file1")
}