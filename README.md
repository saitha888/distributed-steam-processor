# CS425-mp3-g12: Hybrid Distributed File System

## Introduction


## Instructions

Clone the repository on each of your 


### Common Workflow

1) Pull the introducer code onto one machine

2) Pull the main branch code onto all other machines

3) Once you've picked the machine to be your introducer, go to the .env file of each machine and change the "INTRODUCER_ADDRESS" variable to match the address of the chosen introducer

4) Start the service by running *go run logging.go* on the introducer. Type *join* to join the system.

5) Do the same steps on all other machines you want to be in the system 

6) Other commands to run now are: 

    - list_mem: list the membership list
    - list_self: list self’s id
    - join: join the group 
    - leave: voluntarily leave the group
    - enable_sus / disable_sus: enable/disable suspicion
    - status_sus: suspicion on/off status
    - sus_list: command to list suspected nodes
    - client **pattern**: query all log files for a pattern

7) If you would like to induce a specific drop rate, change the "DROP_RATE" variable in the .env file
