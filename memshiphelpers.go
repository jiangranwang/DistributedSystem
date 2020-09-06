package main

import (
	"encoding/json"
	"fmt"
	"net"
	"sort"
	"strconv"
	"time"
)

// func UpdateRecentMessageList(uniqueKey string)
// ------------------------------------------------------------------
// Description: A helper function that update the key of received message to
//              recentMessages and pop the oldest key if the list size
//              is greater than 50
// Input:   uniqueKey string: the unique key of every message
// Output:  None
func UpdateRecentMessageList(uniqueKey string) {
	if len(recentMessages) < SIZERECENTMSG {
		recentMessages = append(recentMessages, uniqueKey)
	} else {
		_, recentMessages = recentMessages[0], recentMessages[1:]
		recentMessages = append(recentMessages, uniqueKey)
	}
}


// func BasicMessageHandler(msgReceived map[int]string) bool
// ------------------------------------------------------------------
// Description: A helper function helps to decide whether a message
//              has been previously received and forward the message to
//              everyone else if not
// Input:   msgReceived map[int]string: the decoded message
// Output:  false if the message has been received and need to be dropped
func BasicMessageHandler(msgReceived *map[int]string) bool {
	// to see whether the message should be accepted or dropped
	// if accepted (true) see if the message need to be broadcast to others
	for _, msg := range recentMessages {
		// the message has been received before
		if (*msgReceived)[UNIQUEID] == msg {
			return false
		}
	}

	UpdateRecentMessageList((*msgReceived)[UNIQUEID])
	if (*msgReceived)[MSGTYPE] == FAIL || (*msgReceived)[MSGTYPE] == LEAVE || (*msgReceived)[MSGTYPE] == UPDATELIST {
		for id, addr := range targetAddr {
			sender, _ := strconv.Atoi((*msgReceived)[SENDER])
			if sender == targetList[id] || id >= targetMonitorNum {
				continue
			}

			logMsg := fmt.Sprintf("Forwarding %v message to Node: %v\n", helperMap[(*msgReceived)[MSGTYPE]], memberHost[targetList[id]])
			fmt.Print(logMsg)
			WriteLog(logFile, logMsg, false)

			conn, err := net.Dial("udp",  addr + ":" + PORT)
			if err != nil {
				continue
			}
			msgStr, _ := json.Marshal(msgReceived)
			_, err = conn.Write(msgStr)
			logMsg = fmt.Sprintf("Fail forwarding %v message to Node: %v\n", helperMap[(*msgReceived)[MSGTYPE]], memberHost[targetList[id]])
			ErrorHandler(logMsg, err, false)
			_ = conn.Close()
		}
	}

	return true
}

// func PrintMessage(msgReceived *map[int]string)
// ------------------------------------------------------------------
// Description: This function prints the message received
// Input: msgReceived map[int]string: the decoded message
// Output: None
func PrintMessage(msgReceived *map[int]string) {
	if msgReceived == nil {
		fmt.Printf(">> ERROR: NULL MESSAGE!\n")
		return
	}
	fmt.Printf(">> NEW MESSAGE \n")
	fmt.Printf("-->> Message Type: %s\n", helperMap[(*msgReceived)[MSGTYPE]])
	fmt.Printf("-->> Sender ID   : %s\n", (*msgReceived)[SENDER])
	fmt.Printf("-->> Content     : %s\n", (*msgReceived)[CONTENT])
	fmt.Printf(">> MESSAGE END \n\n")
}


// func PrintMemberList()
// ------------------------------------------------------------------
// Description: This function prints the node ID and address in memberHost
// Input: None
// Output: None
func PrintMemberList(){
	fmt.Printf("\n%c[%d;%d;%dm%s-----------Membership List----------%c[0m \n", 0x1B, 37, 46, 1, "", 0x1B)
	for key, val := range memberHost {
		fmt.Printf("%c[%d;%d;%dm%s(ID<%s> HOST<%s> ADDR<%s>)%c[0m \n", 0x1B, 32, 40, 1, "", strconv.Itoa(key), val, memberAddr[key], 0x1B)
	}
	fmt.Printf("%c[%d;%d;%dm%s(ID<%s> HOST<%s> ADDR<%s>)%c[0m \n",0x1B, 33, 40, 1, "", strconv.Itoa(selfID), localHost, localAddr, 0x1B)
	fmt.Printf("%c[%d;%d;%dm%s---------END Membership List--------%c[0m \n", 0x1B, 37, 46, 1, "", 0x1B)
}


// func PrintHBTList()
// ------------------------------------------------------------------
// Description: This function prints the heartbeat list
// Input: None
// Output: None
func PrintHBTList(){
	if len(targetList) == 0 {
		fmt.Printf("\n%c[%d;%d;%dm%s--NO TARGET: Only one member in the system--%c[0m \n", 0x1B, 37, 46, 1, "", 0x1B)
	}

	fmt.Printf("\n%c[%d;%d;%dm%s----------HEARTBEAT TARGET----------%c[0m", 0x1B, 37, 46, 1, "", 0x1B)
	for idx, key := range targetList {
		if idx >= targetMonitorNum {
			continue
		}
		fmt.Printf( "%c[%d;%d;%dm%s(%s) %c[0m", 0x1B, 32, 40, 1, "", memberHost[key], 0x1B)
	}
	fmt.Printf("\n%c[%d;%d;%dm%s-----------MONITOR TARGET-----------%c[0m", 0x1B, 37, 46, 1, "", 0x1B)
	for key, addr := range monitorList {
		if key >= targetMonitorNum {
			continue
		}
		fmt.Printf( "%c[%d;%d;%dm%s(%s) %c[0m", 0x1B, 32, 40, 1, "", addr, 0x1B)
	}
	fmt.Print("\n")

}

// MakeMessage (msgType string, msgContent string, sender string) string
// ------------------------------------------------------------------
// Description: A helper function that generates structured message
//              using the given content
// Input:   msgType string: The identifier of different message type
//          msgContent string: the content which the message include
//          sender string: the id of the sender
// Output:  a json string which generated by the json encoder
func MakeMessage(msgType string, msgContent string, sender string) string {

	msgToSent := make(map[int]string)
	msgToSent[UNIQUEID] = geneUniqueID()
	msgToSent[TIMESTAMP] = time.Now().Format("2006-01-02T15:04:05.000Z")
	msgToSent[SENDER] = sender
	msgToSent[MSGTYPE] = msgType
	msgToSent[CONTENT] = msgContent
	msgString, _ := json.Marshal(msgToSent)
	UpdateRecentMessageList(msgToSent[UNIQUEID])

	return string(msgString)
}

// func UpdateHeartbeatTarget()
// ------------------------------------------------------------------
// Description: A helper function that helps each node decide their
//              heartbeat target list and monitor list
// Input:   None
// Output:  None
func UpdateHeartbeatTarget() {
	// clear old targets
	monitorList = make(map[int]string)
	targetList = make(map[int]int)
	targetAddr = make(map[int]string)

	if len(memberHost) == 0 {
		return
	}

	memberLock.Lock()
	// number of machine that is online
	numOnline := len(memberHost) + 1
	keyArr := make([]int, 0, len(memberHost))

	// get all the keys
	for key := range memberHost {
		keyArr = append(keyArr, key)
	}
	keyArr = append(keyArr, selfID)

	sort.Ints(keyArr)

	// find the index of the current node
	n := 0
	for _, key := range keyArr {
		if key == selfID {
			break
		}
		n ++
	}

	// every node is heartbeat to 3 predecessors
	// every node is monitoring 3 successors
	if numOnline == 2 {
		targetMonitorNum = 1
		n += numOnline
		targetList[0] = keyArr[(n - 1) % numOnline]
		targetAddr[0] = memberAddr[targetList[0]]
		monitorList[0] = memberHost[keyArr[(n + 1) % numOnline]]
		lastUpdateLocal[keyArr[(n + 1) % numOnline]] = time.Now()

	} else if numOnline == 3 {
		targetMonitorNum = 2
		n += numOnline
		targetList[0] = keyArr[(n - 1) % numOnline]
		targetAddr[0] = memberAddr[targetList[0]]
		targetList[1] = keyArr[(n - 2) % numOnline]
		targetAddr[1] = memberAddr[targetList[1]]
		monitorList[0] = memberHost[keyArr[(n + 1) % numOnline]]
		monitorList[1] = memberHost[keyArr[(n + 2) % numOnline]]
		lastUpdateLocal[keyArr[(n + 1) % numOnline]] = time.Now()
		lastUpdateLocal[keyArr[(n + 2) % numOnline]] = time.Now()

	} else if numOnline >= 4 {
		targetMonitorNum = 3
		n += numOnline
		targetList[0] = keyArr[(n - 1) % numOnline]
		targetAddr[0] = memberAddr[targetList[0]]
		targetList[1] = keyArr[(n - 2) % numOnline]
		targetAddr[1] = memberAddr[targetList[1]]
		targetList[2] = keyArr[(n - 3) % numOnline]
		targetAddr[2] = memberAddr[targetList[2]]
		monitorList[0] = memberHost[keyArr[(n + 1) % numOnline]]
		monitorList[1] = memberHost[keyArr[(n + 2) % numOnline]]
		monitorList[2] = memberHost[keyArr[(n + 3) % numOnline]]
		lastUpdateLocal[keyArr[(n + 1) % numOnline]] = time.Now()
		lastUpdateLocal[keyArr[(n + 2) % numOnline]] = time.Now()
		lastUpdateLocal[keyArr[(n + 3) % numOnline]] = time.Now()

	} else if numOnline == 1 {
		targetMonitorNum = 0
	}
	memberLock.Unlock()

	PrintHBTList()
}
