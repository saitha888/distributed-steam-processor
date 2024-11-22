package membership

import (
    "time"
    "distributed_system/global"
    "distributed_system/util"
)

// Run the function for exactly 4 seconds
func susTimeout(duration time.Duration, sus_id string, inc_num int) {
	// Create a channel that will send a signal after the specified duration
	timeout := time.After(duration)
	// Run the work in a loop
	for {
		select {
		case <-timeout:
            RemoveNode(sus_id)
            return
		default:
			// Continue doing the work
            index := util.FindNode(sus_id)
            if index >= 0 && global.Membership_list[index].Inc > inc_num {
                message := "Node suspect removed for: " + sus_id + "\n"
                util.AppendToFile(message, global.Membership_log)
                return
            }
		}
	}
}

