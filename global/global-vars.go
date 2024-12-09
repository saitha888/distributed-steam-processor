package global

import (
	"os"
	"github.com/emirpasic/gods/maps/treemap"
	"github.com/joho/godotenv"
    "sync"
)

var err = godotenv.Load(".env")

var Tcp_ports = []string{
    "fa24-cs425-1201.cs.illinois.edu:8081", 
    "fa24-cs425-1202.cs.illinois.edu:8082", 
    "fa24-cs425-1203.cs.illinois.edu:8083", 
    "fa24-cs425-1204.cs.illinois.edu:8084", 
    "fa24-cs425-1205.cs.illinois.edu:8085", 
    "fa24-cs425-1206.cs.illinois.edu:8086", 
    "fa24-cs425-1207.cs.illinois.edu:8087", 
    "fa24-cs425-1208.cs.illinois.edu:8088", 
    "fa24-cs425-1209.cs.illinois.edu:8089",
    "fa24-cs425-1210.cs.illinois.edu:8080",
}

var Udp_ports = []string{
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

var Rainstorm_ports = []string{
    "fa24-cs425-1201.cs.illinois.edu:7081", 
    "fa24-cs425-1202.cs.illinois.edu:7082", 
    "fa24-cs425-1203.cs.illinois.edu:7083", 
    "fa24-cs425-1204.cs.illinois.edu:7084", 
    "fa24-cs425-1205.cs.illinois.edu:7085", 
    "fa24-cs425-1206.cs.illinois.edu:7086", 
    "fa24-cs425-1207.cs.illinois.edu:7087", 
    "fa24-cs425-1208.cs.illinois.edu:7088", 
    "fa24-cs425-1209.cs.illinois.edu:7089",
    "fa24-cs425-1210.cs.illinois.edu:7080",
}


var Tcp_port string = os.Getenv("TCP_PORT")
var Udp_port string = os.Getenv("UDP_PORT")
var Rainstorm_port string = os.Getenv("RAINSTORM_PORT")
var Machine_number string = os.Getenv("MACHINE_NUMBER")
var Membership_log string = os.Getenv("MEMBERSHIP_FILENAME")
var Hydfs_log string = os.Getenv("HYDFS_FILENAME")
var Udp_address string = os.Getenv("MACHINE_UDP_ADDRESS")
var Tcp_address string = os.Getenv("MACHINE_TCP_ADDRESS")
var Rainstorm_address string = os.Getenv("MACHINE_RAINSTORM_ADDRESS")
var Introducer_address string = os.Getenv("INTRODUCER_ADDRESS")
var Leader_address string = os.Getenv("LEADER_ADDRESS")

var Node_id string = ""
var Ring_id string = ""
var Inc_num int = 0
var Ring_map = treemap.NewWithIntComparator()
var Membership_list []Node
var Schedule = make(map[int][]map[string]string)
var Enabled_sus = false
var Cache_set = make(map[string]bool)
var File_prefix string = Udp_address[13:15]
var Partitions [][]int
var Batches = make(map[string][]Tuple) // destination to list of tuples
var AckBatches = make(map[string]string) // destination to list of tuple ids
var BatchesMutex sync.Mutex
var AckBatchesMutex sync.Mutex
var AppendMutex sync.Mutex
var DestMutex sync.Mutex
var ScheduleMutex sync.Mutex
var Reschedule_called = false
var StateMutex sync.Mutex
var TimerMutex sync.Mutex


