package main

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

///////////////////////////////////////////////////
/////////                        //////////////////
/////////  Node Initialization   //////////////////
/////////  Procedures            //////////////////
/////////                        //////////////////
///////////////////////////////////////////////////


// func ReqJoin () bool
// ------------------------------------------------------------------
// Description: Initialization procedures for non-contact node
// Input:   None
// Output:  true if succeed, false if not
func ReqJoin () bool {
	// write log
	WriteLog(logFile, "Send join request to contact node\n", false)

	// 1. Initialize local variables
	localHost, _ = os.Hostname()
	localAddrArr, _ := net.LookupHost(localHost)
	localAddr = localAddrArr[0]
	isContact = false
	isMaster = false

	// 2. Prepare for joining request message
	localHostName, _ := os.Hostname()
	msg := MakeMessage(JOINREQ, localHostName, localAddr)

	// 3. Connect to contact address

	addr, err := net.ResolveUDPAddr("udp", contactAddress + ":" + PORT)
	memberHost[0] = contactAddress
	memberAddr[0] = strings.Split(addr.String(), ":")[0]
	conn, err := net.DialUDP("udp", nil, addr)
	ErrorHandler("Cannot connect to Contact Address", err, true)

	// 4. Send message to request joining the group
	_, err = conn.Write([]byte(msg))
	ErrorHandler("Cannot Send Req Join Msg to contact", err, true)

	// 5. Receive JoinACK message
	msgMap := make(map[int]string)
	rawMsg := make([]byte, 4096)
	n, _, err := conn.ReadFromUDP(rawMsg)
	// check if the contact node is running
	if n <= 0 {
		return false
	}

	err = json.Unmarshal(rawMsg[0:n], &msgMap)
	ErrorHandler("Read from contact error", err, false)
	logMsg := fmt.Sprintf("Receive JOINACK message from contact node\n")
	WriteLog(logFile, logMsg, false)
	fmt.Print(logMsg)
	_ = conn.Close()

	// 6. Update member list
	memberMap := make(map[string]string)
	err = json.Unmarshal([]byte(msgMap[CONTENT]), &memberMap)
	selfID, err = strconv.Atoi(memberMap[strconv.Itoa(SELFKEY)])
	ErrorHandler("Cannot Resolve self ID in group", err, true)
	for key, value := range memberMap {
		if key == strconv.Itoa(SELFKEY) || key == strconv.Itoa(selfID) {
			continue
		}
		intKey, _ := strconv.Atoi(key)
		urlAddrArr := strings.Split(value, "\n")
		memberHost[intKey] = urlAddrArr[0]
		memberAddr[intKey] = urlAddrArr[1]
	}

	PrintMemberList()
	// 7. Update target list
	UpdateHeartbeatTarget()

	return true
}


// func InitContact () bool
// ------------------------------------------------------------------
// Description: Initialization procedures for contact node
// Input:   None
// Output:  None
func InitContact() {
	// write log
	WriteLog(logFile, "Contact Node Initialization\n", false)

	// 1. Initialize local variables
	selfID = 0
	maxID = 1
	isContact = true
	isMaster = true
	masterID = 0

	// 2. read from log to prepare for reconnecting to previous member list
	savedMsg := ReadFromFile(criticalFile, true)
	localHost = contactAddress
	addrArr, _ := net.LookupHost(contactAddress)
	localAddr = addrArr[0]

	// 3. prepare for the message
	msgContent := make(map[int]string)
	_ = json.Unmarshal([]byte(savedMsg), &msgContent)

	for key := range msgContent {
		split := strings.Split(msgContent[key], "\n")
		memberHost[key] = split[0]
		memberAddr[key] = split[1]
	}

	msgContent[0] = localHost + "\n" + localAddr
	msgContentByte, _ := json.Marshal(msgContent)
	msgContentStr := string(msgContentByte)
	msg := MakeMessage(UPDATELIST, msgContentStr, strconv.Itoa(selfID))
	failureList := make([]int, 0)

	// 4. reconnecting
	for key, addr := range memberHost {
		// get original maxID
		if key + 1 > maxID {
			maxID = key + 1
		}
		addr, err := net.ResolveUDPAddr("udp", addr + ":" + PORT)
		ErrorHandler("Cannot Resolve Contact Address", err, false)
		conn, err := net.DialUDP("udp", nil, addr)
		if err != nil {
			fmt.Println("Fail getting connection during initialization!")
			WriteLog(logFile, "Fail getting connection during initialization", false)
			failureList = append(failureList, key)
			continue
		}
		_, _ = conn.Write([]byte(msg))

		// create artificial time stamp
		tempTime := time.Now().Format("2006-01-02T15:04:05.000Z")
		lastUpdate[key], _ = time.Parse("2006-01-02T15:04:05.000Z", tempTime)
		lastUpdateLocal[key] = time.Now()
		_ = conn.Close()
	}

	// 5. if any node times out, delete them from member list
	for _, key := range failureList {
		delete(memberHost, key)
		fmt.Printf(">> node %d deleted\n", key)
	}

	// 6. log the current member list to the log file
	writeCritical()
	UpdateHeartbeatTarget()

	// wait to get master information
	time.Sleep(500 * time.Millisecond)
	return
}
