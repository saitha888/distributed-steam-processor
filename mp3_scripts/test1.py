import sys
import subprocess

local_files = sys.argv[1:]

filenames = ["file1","file2","file3","file4","file5"]

for i in range(5):
    local_file = local_files[i]
    hydfs_filename = filenames[i]
    command = "create " + local_file + hydfs_filename 
    subprocess.run([command])