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
    Src_file string
    Dest_file string
    Tuple []string
}

type SourceTask struct {
    Start int
    End int
    Src_file string
    Dest_file string
}