package udp

import (
    "os"
    "github.com/emirpasic/gods/maps/treemap"
    "github.com/joho/godotenv"
)

// struct for each node in system
type Node struct {
    NodeID    string // unique node id (udp port version)
    Status    string // alive/sus  
    Inc int // incarnation number
    RingID int // unique ring id (tcp port version)
}

// node_id (includes timestamp) that is set when a process joins the system
var node_id string = ""
var ring_id string = ""

// variable that enables suspicion mechanism. set based on logging.go
var enabled_sus = false

// list of nodes for members in system, initially empty
var membership_list []Node

// list of nodes in ring
var ring_map = treemap.NewWithIntComparator()

// inc_num, inceremented as soon as process joins system, and every time node is suspected
var inc_num int = 0

// loading godotenv library
var err = godotenv.Load(".env")

// list of udp ports in process
var ports = []string{
    "fa24-cs425-1201.cs.illinois.edu:9081", 
    "fa24-cs425-1202.cs.illinois.edu:9082", 
    "fa24-cs425-1203.cs.illinois.edu:9083", 
    "fa24-cs425-1204.cs.illinois.edu:9084", 
    "fa24-cs425-1205.cs.illinois.edu:9085", 
    "fa24-cs425-1206.cs.illinois.edu:9086", 
    "fa24-cs425-1207.cs.illinois.edu:9087", 
    "fa24-cs425-1208.cs.illinois.edu:9088", 
    "fa24-cs425-1209.cs.illinois.edu:9089",
    "fa24-cs425-1210.cs.illinois.edu:9080",
}

// env file variables
var logfile string = os.Getenv("MEMBERSHIP_FILENAME")
var introducer_address string = os.Getenv("INTRODUCER_ADDRESS")
var udp_address string = os.Getenv("MACHINE_UDP_ADDRESS")
var machine_number string = os.Getenv("MACHINE_NUMBER")
var udp_port string = os.Getenv("UDP_PORT")
var tcp_address string = os.Getenv("MACHINE_TCP_ADDRESS")

var file_prefix string = udp_address[13:15]