package scripts
 
import (
	"time"
	"distributed_system/global"
	"distributed_system/util"
	"fmt"
	"encoding/json"
)

var machine_to_rainstorm = make(map[int]string)

func KillVMS(machine_num1 int, machine_num2 int) {
	for i := 0; i < 10; i++ {
		machine_to_rainstorm[i+1] = global.Rainstorm_ports[i]
	}

	time.Sleep(1*time.Second + 500*time.Millisecond)

	ports := []string{
		machine_to_rainstorm[machine_num1],
		machine_to_rainstorm[machine_num2],
	}

	for _,port := range ports {
		conn, err := util.DialTCPClient(port)
		if err != nil {
			fmt.Println("Error dialing client in send schedule", err)
			continue
		}
		defer conn.Close()
	
		// send kill message
		message := make(map[string]string)
		message["kill"] = "kill"
		encoder  := json.NewEncoder(conn)
		err = encoder.Encode(message)
		if err != nil {
			fmt.Println("Error encoding data in send schedule", err)
			continue
		}
	}	
}	