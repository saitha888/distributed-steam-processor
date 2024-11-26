package global

type Message struct {
    Action string
	Filename  string
	FileContents string
}

type Node struct {
    NodeID    string // unique node id (udp port version)
    Status    string // alive/sus  
    Inc int // incarnation number
    RingID int // unique ring id (tcp port version)
}

type Stream struct {
    src_file string
    dest_file string
    tuple []string
}

type Stage struct {
    Name  string
    Position int
}