package main

import (
	"fmt"
	"strconv"
	"time"
)


// func newElection(failNodeID int)
// ------------------------------------------------------------------
// Description: This function starts a new election protocol when
//				the master node fails or the election protocol has
//				not been started
// Input:   failNodeID int: the node id of the fail/leave node
// Output:  None
func newElection(failNodeID int) {
	// check if master fails
	if failNodeID != masterID {
		return
	}

	// check if election already starts
	electionLock.Lock()
	if isElecting == true {
		electionLock.Unlock()
		return
	}
	isElecting = true
	electionLock.Unlock()

	logMsg := fmt.Sprint("Master leave/fail. Start master election...\n")
	fmt.Print(logMsg)
	WriteLog(logFile, logMsg, false)

	keyArr := make([]int, 0, len(memberHost))
	// get all the keys
	for key := range memberHost {
		if key < selfID {
			continue
		}
		keyArr = append(keyArr, key)
	}

	// if the current node has the highest id
	if len(keyArr) == 0 {
		logMsg := fmt.Sprint("Current node has the highest node ID. Current node becomes master\n")
		fmt.Print(logMsg)
		WriteLog(logFile, logMsg, false)

		coordinator(failNodeID)
		return
	}

	// send election message to node with higher id
	msgSent := MakeMessage(ELECTION, strconv.Itoa(failNodeID), strconv.Itoa(selfID))
	for _, key := range keyArr {
		logMsg := fmt.Sprintf("Sending election message to node %v: %v\n", key, memberHost[key])
		fmt.Print(logMsg)
		WriteLog(logFile, logMsg, false)

		go sendRequest(key, msgSent)
		time.Sleep(5 * time.Millisecond)
	}

	// wait for ok message
	chanLock.Lock()
	okChanNum += 1
	chanLock.Unlock()
	select {
	case <- okWaitChan:
		break
	case <- time.After(OKWAITTIME):
		chanLock.Lock()
		okChanNum -= 1
		chanLock.Unlock()
		// wait ok timeout, self node becomes coordinator
		logMsg := fmt.Sprint("Waiting for ok timeout. Current node becomes master\n")
		fmt.Print(logMsg)
		WriteLog(logFile, logMsg, false)

		coordinator(failNodeID)
		return
	}

	// wait for coordinator message
	chanLock.Lock()
	coChanNum += 1
	chanLock.Unlock()
	select {
	case <- coWaitChan:
		return
	case <- time.After(COWAITTIME):
		chanLock.Lock()
		coChanNum -= 1
		chanLock.Unlock()
		// wait coordinator timeout, start new election run
		logMsg := fmt.Sprint("Waiting for coordinator message timeout. New coordinator may fail. Start new election run...\n")
		fmt.Print(logMsg)
		WriteLog(logFile, logMsg, false)

		electionLock.Lock()
		isElecting = false
		electionLock.Unlock()
		newElection(failNodeID)
	}
}


// func coordinator(failNodeID int)
// ------------------------------------------------------------------
// Description: This function is called when the current node becomes
//				the master node. It sends the coordinator message to
//				all other nodes in the system
// Input:   failNodeID int: the node id of the fail/leave node
// Output:  None
func coordinator(failNodeID int) {
	// current node becomes the master node
	isMaster = true
	masterID = selfID
	updateReplicaList(strconv.Itoa(failNodeID))
	msgSent := MakeMessage(COORDINATOR, "", strconv.Itoa(selfID))

	time.Sleep(100 * time.Millisecond)
	// send coordinator message to all other nodes
	for key := range memberHost {
		fmt.Printf("send coordinator message to node %v: %v\n", key, memberHost[key])
		sendRequest(key, msgSent)
		time.Sleep(5 * time.Millisecond)
	}
}