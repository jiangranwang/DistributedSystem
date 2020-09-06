package main

import (
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"time"
)

// func FailDetector()
// ------------------------------------------------------------------
// Description: A routine that will keep running at backend checking
//              if there is any node in member list that timed out
// Input:   None
// Output:  None
func FailDetector() {
	// a function that detects failure nodes and broadcast fail message
	for {
		// go through all monitoring nodes and check for node that haven't received its message for 5s
		failList := make([]int, 0)

		for _, url := range monitorList {
			key := 0
			found := false
			for key1, url1 := range memberHost {
				if url == url1 {
					key = key1
					found = true
				}
			}

			if !found {
				WriteLog(logFile, "Member in monitorList not found in memberHost\n", false)
				continue
			}

			_, ok := lastUpdate[key]
			if !ok {
				// logMsg := fmt.Sprintf("Node %v not found in lastUpdate map\n", key)
				// WriteLog(logFile, logMsg, false)
				continue
			}

			// check if the current node times out
			lastTimeStamp := lastUpdateLocal[key]
			if lastTimeStamp.Add(FAILTIME).Before(time.Now()) {
				failList = append(failList, key)
			}
		}

		if len(failList) == 0 {
			time.Sleep(time.Duration(15) * time.Millisecond)
			continue
		}

		// construct failure message for each failure node
		// append all failure message to failMsgList
		failMsgList := make([]string, 0)
		for _, key := range failList {
			failNodeStr := strconv.Itoa(key)
			selfNodeStr := strconv.Itoa(selfID)
			failMsg := MakeMessage(FAIL, failNodeStr, selfNodeStr)
			failMsgList = append(failMsgList, failMsg)

			logMsg := fmt.Sprintf("Detect Failed Node %d: %v \n", key, memberHost[key])
			logMsg += fmt.Sprintf("Last update at: %s\n", lastUpdateLocal[key])
			WriteLog(logFile, logMsg, false)
			fmt.Print(logMsg)

			memberLock.Lock()
			if key != selfID {
				delete(memberHost, key)
				delete(replicateCounter, strconv.Itoa(key))
			}
			memberLock.Unlock()

			if isMaster {
				updateReplicaList(strconv.Itoa(key))
			}

			// check if master fails
			go newElection(key)
		}

		// update critical file for contact node
		if isContact {
			recordsWritten, _ := json.Marshal(memberHost)
			WriteLog(criticalFile, string(recordsWritten), true)
		}

		// send all fail message to all monitoring nodes
		for _, addr := range targetAddr {

			conn, err := net.Dial("udp",  addr + ":" + PORT)
			ErrorHandler("Cannot Dial to Contact Address", err, false)

			for _, msg := range failMsgList {
				_, err = conn.Write([]byte(msg))
				ErrorHandler("Write fail message fails :(", err, false)
			}

			err = conn.Close()
			ErrorHandler("Closing connection fails :(", err, false)
		}

		UpdateHeartbeatTarget()
		// execute this routine for every 200 ms
		time.Sleep(time.Duration(200) * time.Millisecond)
	}
}


// func HeartBeating()
// ------------------------------------------------------------------
// Description: A routine that will keep running at backend that keeps
//              heartbeating to the node's heartbeat targets
// Input:   None
// Output:  None
func HeartBeating() {
	for {
		// send heartbeat message to heartbeat target every 100 ms
		selfKey := strconv.Itoa(selfID)
		hbMsg := MakeMessage(HEARTBEAT, "", selfKey)
		for _, addr := range targetAddr {
			conn, err := net.Dial("udp", addr + ":" + PORT)
			if err != nil {
				continue
			}

			_, err = conn.Write([]byte(hbMsg))
			ErrorHandler("Write heartbeat fails :(", err, false)
			_ = conn.Close()
			time.Sleep(time.Duration(33) * time.Millisecond)
		}
		time.Sleep(100 * time.Millisecond)
	}

}

