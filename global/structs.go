package global

type Message struct {
	Action       string
	Filename     string
	FileContents string
}

type Node struct {
	NodeID string // unique node id (udp port version)
	Status string // alive/sus
	Inc    int    // incarnation number
	RingID int    // unique ring id (tcp port version)
}

type SourceTask struct {
	Start    int
	End      int
	Src_file string
}

type Tuple struct {
	ID    string
	Key   string
	Value string
	Src   string
	Stage int
}