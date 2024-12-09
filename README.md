# CS425-mp3-g12: Hybrid Distributed File System

## Introduction

Rainstorm is a distributed stream-processing program which utilizes HyDFS, our membership list in mp2, and our grep program from mp1.

## Instructions

Clone the repository on each of your 


### Common Workflow

1) Pull the repo on all the necessary VMs

2) Create a .env file and fill in the necessary fields. Below is an example of a .env file of machine 9:

    TCP_PORT = 8087
    UDP_PORT = 9087
    RAINSTORM_PORT=7087
    MACHINE_NUMBER = 7
    MEMBERSHIP_FILENAME = "logs/membership.log"
    HYDFS_FILENAME = "logs/hydfs.log"
    MACHINE_UDP_ADDRESS = "fa24-cs425-1207.cs.illinois.edu:9087"
    MACHINE_TCP_ADDRESS = "fa24-cs425-1207.cs.illinois.edu:8087"
    MACHINE_RAINSTORM_ADDRESS = "fa24-cs425-1207.cs.illinois.edu:7087"
    LEADER_ADDRESS = "fa24-cs425-1210.cs.illinois.edu:7080"
    INTRODUCER_ADDRESS = "fa24-cs425-1210.cs.illinois.edu:9080"

3) Select 1 machine to be the introducer/leader. In the example above, machine 10 is this.

4) Start the service by running *go run main.go* on the introducer. Type *join* to join the system.

5) Do the same steps on all other machines you want to be in the system 

6) To invoke Rainstorm, you need to:
    i. create a HyDFS file using on of the local files in the 'local-files' directory
    ii. Rainstorm can then be invoked by calling *rainstorm <op1_exe> <pattern_param> <op2_exe> <hydfs_src_file> <hydfs_dest_filename> <num_tasks>*

7) In order to see the output file and the append logs in a readable format, call *merge <hydfs_dest_filename>* and *merge-logs*
