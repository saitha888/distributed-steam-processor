# CS425-mp1-g12: Distributed Log Query System

This project implements a distributed log querying system where each machine can act as both a server (listener) and a client (sender). The client sends `grep` queries to multiple machines, and the results are aggregated and returned. 

## Instructions

### Running a Server (Listener)

To start a server on a machine, run the following command:

go run logging.go

### Running a Client 

To start a client on a machine and send a grep command to all other machines in the system, run the following command:

go run logging.go client "<**grep pattern**>"

Make sure not to write the full grep command, but only the pattern which will query the files. For example, below is a valid call to the machine:

go run logging.go client GET


### Running the Test Suite

To run the test suite on any machine, run the following command:

go run test.go

This will execute our test file, which will go through each of our unit test cases and print whether or not we passed them.


### Common Workflow

1) Run the server command on all machines that are part of your distributed system 

2) On one of the machines run the client command followed by a grep pattern 

3) Check the output.txt file of the client machine for the aggregated output
