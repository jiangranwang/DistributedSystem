package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)




///////////////////////////////////////////////////
/////////                     /////////////////////
/////////  Service Routines   /////////////////////
/////////                     /////////////////////
///////////////////////////////////////////////////


// function that send file to other node
func WriteToNode(senderName string, senderType string, receiverName string, receiverType string, receiveID int) {
	// check if we are simply duplicating file on the same node
	var senderPath string
	var receiverPath string
	if senderType == LOCALNAME {
		senderPath = LOCALFILEPATH
	} else {
		senderPath = SDFSFILEPATH
	}
	if receiverType == LOCALNAME {
		receiverPath = LOCALFILEPATH
	} else {
		receiverPath = SDFSFILEPATH
	}
	if receiveID == selfID {
		localCopy(senderPath + senderName, receiverPath + receiverName)
		return
	} else {
		FileTransferClient(memberAddr[receiveID], senderPath, senderName, receiverPath, receiverName)
	}
	fmt.Printf("Send file with <%v> name <%v> as <%v> name <%v> to node: %v\n", senderType, senderName, receiverType, receiverName, memberHost[receiveID])
}

// addNewFile(localFileName string, sdfsFileName string, localID string, recPointer *map[string]string)
// ------------------------------------------------------------------
// Description: This function adds a new entry in the replica list. The
// 				location of the replicas are determined by the node ID
// Input:   localFileName string: the name of the local file
// 			sdfsFileName string: the name of the sdfs file
// 			localID string: node id of which the local file is present
// 			recPointer *map[string]string: the map to be filled with replica information
// Output:  None
func addNewFile(sdfsFileName string, localID string, recPointer *map[string]string) {
	// this function should only be called by master node
	newFile := make(map[string]string)
	newFile[REPLICAONE] = localID
	newFile[REPLICATWO] = ""
	newFile[REPLICATHREE] = ""
	newFile[REPLICAFOUR] = ""
	newFile[LASTUPDATE] = time.Now().Format("2006-01-02T15:04:05.000Z")

	setReplicaID(localID, recPointer)
	var replicaArr []string

	// change replica list on master node
	fileLock.Lock()
	replicateList[sdfsFileName] = newFile
	// fill in master's file replica list
	for key, idStr := range *recPointer {
		id, _ := strconv.Atoi(idStr)
		if id == selfID {
			replicaArr = append(replicaArr, localHost)
		} else {
			replicaArr = append(replicaArr, memberHost[id])
		}
		replicateList[sdfsFileName][key] = idStr
	}
	fileLock.Unlock()

	logMsg := fmt.Sprintf("SDFS file %v is replicated at the following nodes: %v", sdfsFileName, replicaArr)
	fmt.Println(logMsg)
	WriteLog(logFile, logMsg, false)
}

// func getInput(sdfsFileName string) string
// ------------------------------------------------------------------
// Description: This function gets the input from user to determine
//				whether to overwrite an existing file
// Input:   sdfsFileName string: the file we are going to overwrite
// Output:  the input string from user
func getInput(sdfsFileName string) string {
	input := make(chan string, 1)
	quit := make(chan bool, 1)
	go func(input chan string) {
		reader := bufio.NewReader(os.Stdin)
		for {
			fmt.Printf("Last update of SDFS file %v is within one minute. Do you want to overwrite it? [y/n]\n", sdfsFileName)
			cmd, err := reader.ReadString('\n')
			ErrorHandler("Input error: ", err, false)
			select {
			case <- quit:
				return
			default:
				if cmd == "y\n" || cmd == "n\n" {
					input <- cmd
					return
				}
				fmt.Println("Please enter y for yes and n for no")
			}
		}
	}(input)

	cmd := "n\n"
	select {
	case cmd = <-input:
		break
	case <-time.After(30 * time.Second):
		fmt.Println("Response timed out. Press enter to continue.")
		quit <- true
		break
	}

	return strings.TrimSuffix(cmd, "\n")
}

// func handleLeave()
// ------------------------------------------------------------------
// Description: This function handles the leave instruction
// Input:   None
// Output:  None
func handleLeave(){
	fmt.Println("----------Leaving Group----------")
	msgSent := MakeMessage(LEAVE, strconv.Itoa(selfID), strconv.Itoa(selfID))

	var election int
	// send leave message to the monitoring nodes
	for _, nodeID := range targetList {
		sendRequest(nodeID, msgSent)
		time.Sleep(time.Duration(5) * time.Millisecond)
		election = nodeID
	}

	// remove all sdfs files
	err := os.RemoveAll(SDFSFILEPATH)
	ErrorHandler("Fail to remove sdfs files: ", err, false)
	_ = os.Mkdir(SDFSFILEPATH, os.ModePerm)

	WriteLog(logFile, "-------------------------SELF NODE LEAVE-------------------------\n", false)

	// send new election message to an arbitrary node
	if isMaster {
		msgSent = MakeMessage(NEWELECTION, "", strconv.Itoa(selfID))
		sendRequest(election, msgSent)
	}

	_ = fLog.Close()
	os.Exit(0)
}

// func PutWithPrefix(localDir string, sdfsPrefix string)
// ------------------------------------------------------------------
// Description: Put all files under a directory in local/ to sdfs system with a prefix added to every file
// Input:   localDir string: a directory under local directory
// 			sdfsPrefix string: the prefix that will be added to the files
// Output:  None
func PutWithPrefix(localDir string, sdfsPrefix string, isDir bool) {

	if isDir {
		if localDir[len(localDir) - 1] !=  '/' {
			localDir += "/"
		}
	}

	if _, err := os.Stat(LOCALFILEPATH + localDir); !os.IsNotExist(err) {
		// LOCALFILEPATH + localDir exists
		files, err := ioutil.ReadDir(LOCALFILEPATH + localDir)
		if err != nil {
			log.Fatal(err)
			return
		}

		i := 0

		for _, file := range files {
			i += 1
			fmt.Print(strconv.Itoa(i) + ": ")
			handlePut(localDir + file.Name(), sdfsPrefix + file.Name(), false)
			time.Sleep(time.Millisecond)
		}

		fmt.Printf("There are %d files put into sdfs systems in total\n", len(files))

	} else {
		fmt.Printf("The directory specified doesn't exists in local path\n")
	}
}

// func DeleteWithPrefix(sdfsPrefix string)
// ------------------------------------------------------------------
// Description: Delete all files in SDFS with the given prefix
// Input:   sdfsPrefix: string
// Output:  None
func DeleteWithPrefix(sdfsPrefix string) {
	var deleteList []string
	i := 0
	for fileName := range replicateList {
		if strings.HasPrefix(fileName, sdfsPrefix) {
			deleteList = append(deleteList, fileName)
		}
	}
	for _, fileName := range deleteList {
		i += 1
		fmt.Print(strconv.Itoa(i) + ": ")
		handleDelete(fileName)
		time.Sleep(time.Millisecond)
	}
	fmt.Printf("%d files deleted from sdfs system!\n", i)
}

// func handlePut(localFileName string, sdfsFileName string)
// ------------------------------------------------------------------
// Description: This function handles the put instruction
// Input:   localFileName string: local file name in the put instruction
// 			sdfsFileName string: sdfs file name in the put instruction
// Output:  None
func handlePut(localFileName string, sdfsFileName string, overwrite bool) {
	// check if local file exists
	if _, err := os.Stat(LOCALFILEPATH + localFileName); os.IsNotExist(err) {
		logMsg := fmt.Sprintf("Can't execute put instruction. Local file %v does not exist!\n", localFileName)
		fmt.Print(logMsg)
		WriteLog(logFile, logMsg, false)
		return
	}

	if isDir(LOCALFILEPATH + localFileName) {
		PutWithPrefix(localFileName, sdfsFileName, true)
		return
	}

	logMsg := fmt.Sprintf("Distributing local file %v as SDFS file %v\n", localFileName, sdfsFileName)
	fmt.Print(logMsg)
	WriteLog(logFile, logMsg, false)

	// check if current node is master
	if isMaster {
		// check if the file is already been replicated
		fileLock.RLock()
		_, ok := replicateList[sdfsFileName]
		if ok {
			receivedTime, err := time.Parse("2006-01-02T15:04:05.000Z", replicateList[sdfsFileName][LASTUPDATE])
			fileLock.RUnlock()
			ErrorHandler("Decoding last update time of replica list error: ", err, false)
			currentTimeString := time.Now().Format("2006-01-02T15:04:05.000Z")
			currentTime, _ := time.Parse("2006-01-02T15:04:05.000Z", currentTimeString)

			// if last update is within one minute
			if currentTime.Before(receivedTime.Add(time.Minute)) && !overwrite {
				cmd := getInput(sdfsFileName)
				if cmd == "n" {
					// reject update and do nothing
					return
				}
				// overwrite file
				logMsg := fmt.Sprintf("Overwriting SDFS file: %v\n", sdfsFileName)
				fmt.Print(logMsg)
				WriteLog(logFile, logMsg, false)
			}
			deleteSDFS(sdfsFileName)
		}
		if !ok {
			fileLock.RUnlock()
		}
		receiverMap := make(map[string]string)
		addNewFile(sdfsFileName, strconv.Itoa(selfID), &receiverMap)

		for _, idStr := range receiverMap {
			id, _ := strconv.Atoi(idStr)
			WriteToNode(localFileName, LOCALNAME, sdfsFileName, SDFSNAME, id)
		}
		return
	}

	// send request to master node
	logMsg = fmt.Sprintf("Sending put request to master node: %v\n", memberHost[masterID])
	fmt.Print(logMsg)
	WriteLog(logFile, logMsg, false)

	sentMap := make(map[string]string)
	sentMap[LOCALNAME] = localFileName
	sentMap[SDFSNAME] = sdfsFileName
	sentMap[OVERWRITE] = FALSE

	msgContent, _ := json.Marshal(sentMap)
	msgSent := MakeMessage(WRITEREQ, string(msgContent), strconv.Itoa(selfID))

	sendRequest(masterID, msgSent)
}

// func handleGet(localFileName string, sdfsFileName string)
// ------------------------------------------------------------------
// Description: This function handles the get instruction
// Input:   localFileName string: local file name in the get instruction
// 			sdfsFileName string: sdfs file name in the get instruction
// Output:  None
func handleGet(localFileName string, sdfsFileName string, localExist bool) {
	logMsg := fmt.Sprintf("Getting SDFS file: %v\n", sdfsFileName)
	// fmt.Print(logMsg)
	WriteLog(logFile, logMsg, false)
	// check if current node is master
	if isMaster {
		get(localFileName, sdfsFileName, strconv.Itoa(selfID), LOCALNAME, localExist)
		return
	}

	// send request to master node
	logMsg = fmt.Sprintf("Sending get request to master node: %v\n", memberHost[masterID])
	fmt.Print(logMsg)
	WriteLog(logFile, logMsg, false)

	sentMap := make(map[string]string)
	sentMap[LOCALNAME] = localFileName
	sentMap[SDFSNAME] = sdfsFileName
	sentMap[RECEIVERTYPE] = LOCALNAME
	sentMap[RECEIVERID] = strconv.Itoa(selfID)
	if localExist == true {
		sentMap[LOCALEXIST] = TRUE
	} else {
		sentMap[LOCALEXIST] = FALSE
	}

	msgContent, _ := json.Marshal(sentMap)
	msgSent := MakeMessage(READREQ, string(msgContent), strconv.Itoa(selfID))

	sendRequest(masterID, msgSent)
}

// func handleDelete(sdfsFileName string)
// ------------------------------------------------------------------
// Description: This function handles the put instruction
// Input:   sdfsFileName string: sdfs file name in the delete instruction
// Output:  None
func handleDelete(sdfsFileName string) {
	logMsg := fmt.Sprintf("Deleting SDFS file: %v\n", sdfsFileName)
	fmt.Print(logMsg)
	WriteLog(logFile, logMsg, false)
	
	// check if the current node is the master node
	if isMaster {
		if !deleteSDFS(sdfsFileName) {
			logMsg := fmt.Sprintf("SDFS file %v does not exists!\n", sdfsFileName)
			fmt.Print(logMsg)
			WriteLog(logFile, logMsg, false)
		}

		return
	}
	msgSent := MakeMessage(DELETEREQ, sdfsFileName, strconv.Itoa(selfID))

	sendRequest(masterID, msgSent)
}

// func get(localFileName string, sdfsFileName string, requester string)
// ------------------------------------------------------------------
// Description: The main function that handles the get operation. This
//				function find the replica location of the given file and
//				send write request to the corresponding node
// Input:   localFileName string: local file name in the get instruction
// 			sdfsFileName string: sdfs file name in the get instruction
//			requester string: the node id of the node that requests the file
// Output:  None
func get(localFileName string, sdfsFileName string, requester string, receiverType string, localExist bool) {
	// this function should only be called by master node
	var msgSent string
	var senderID int
	sender := getFileID(sdfsFileName, requester, localExist)

	// if we can't find the file
	if sender == FALSE {
		senderID, _ = strconv.Atoi(requester)
		// check if whether the master requests the file
		if senderID == selfID {
			logMsg := fmt.Sprintf("SDFS File: %v doesn't exists\n", sdfsFileName)
			fmt.Print(logMsg)
			WriteLog(logFile, logMsg, false)
			return
		}
		msgSent = MakeMessage(ERRORREAD, sdfsFileName, strconv.Itoa(selfID))
	} else {
		fmt.Printf("replace file %v by node %v send it to node %v\n", sdfsFileName, sender, requester)
		// file is present on some node
		senderID, _ = strconv.Atoi(sender)
		requesterID, _ := strconv.Atoi(requester)
		senderName := sdfsFileName
		senderType := SDFSNAME
		// check if the master has the file to be sent
		if senderID == selfID {
			WriteToNode(senderName, senderType, localFileName, receiverType, requesterID)
			return
		}
		receiverMap := make(map[string]string)
		receiverMap[SENDERNAME] = senderName
		receiverMap[RECEIVERNAME] = localFileName
		receiverMap[SENDERTYPE] = senderType
		receiverMap[RECEIVERTYPE] = receiverType
		receiverMap[REPLICAONE] = requester

		// send write instruction to the sender
		msgContent, _ := json.Marshal(receiverMap)
		msgSent = MakeMessage(WRITE, string(msgContent), strconv.Itoa(selfID))
	}

	sendRequest(senderID, msgSent)
}

// func deleteSDFS(sdfsFileName string)
// ------------------------------------------------------------------
// Description: The main function that handles the delete operation. This
//				function find the replica location of the given file and
//				send delete request to the corresponding node
// Input:   sdfsFileName string: sdfs file name to be deleted
// Output:  None
func deleteSDFS(sdfsFileName string) bool {
	// this function should only be called by the master node
	fileLock.RLock()
	// check if the sdfs file exists
	if _, ok := replicateList[sdfsFileName]; !ok {
		fileLock.RUnlock()
		return false
	}
	fileLock.RUnlock()
	// this function should only be called by master
	for _, key := range replicaMap {
		ok := true
		fileLock.RLock()
		if replicateList[sdfsFileName][key] != "" {
			deleteID, _ := strconv.Atoi(replicateList[sdfsFileName][key])
			replicateCounter[strconv.Itoa(deleteID)] -= 1
			fileLock.RUnlock()
			ok = false
			// check if the sdfs file is on master node
			if deleteID == selfID {
				err := os.Remove(SDFSFILEPATH + sdfsFileName)
				errMsg := fmt.Sprintf("Can't delete sdfs file %v. File does not exist!", sdfsFileName)
				ErrorHandler(errMsg, err, false)
				continue
			}

			logMsg := fmt.Sprintf("Sending delete sdfs file %v request to node: %v\n", sdfsFileName, memberHost[deleteID])
			fmt.Print(logMsg)
			WriteLog(logFile, logMsg, false)

			msgSent := MakeMessage(DELETE, sdfsFileName, strconv.Itoa(selfID))

			sendRequest(deleteID, msgSent)
			time.Sleep(time.Duration(5) * time.Millisecond)
		}
		if ok {
			fileLock.RUnlock()
		}
	}
	// delete entry in replica list
	fileLock.Lock()
	delete(replicateList, sdfsFileName)
	fileLock.Unlock()

	logMsg := fmt.Sprintf("Delete request of SDFS file %v handled\n", sdfsFileName)
	fmt.Print(logMsg)
	WriteLog(logFile, logMsg, false)

	return true
}

// func HeartBeating()
// ------------------------------------------------------------------
// Description: This routine is only executed by the master node which
//				send the updated replica list periodically
// Input:   None
// Output:  None
func sendReplicaList() {
	for {
		// wait until it becomes the master node
		time.Sleep(UPDATETIME)

		if isMaster {
			// master node send replicate list to all nodes periodically
			fileLock.RLock()
			replicaList, _ := json.Marshal(replicateList)
			replicaCounter, _ := json.Marshal(replicateCounter)
			fileLock.RUnlock()
			sendMap := make(map[string]string)
			sendMap[SDFSLIST] = string(replicaList)
			sendMap[SDFSCOUNT] = string(replicaCounter)
			msgContent, _ := json.Marshal(sendMap)
			msgSent := MakeMessage(REPLICALIST, string(msgContent), strconv.Itoa(selfID))

			for nodeID := range memberHost {
				sendTCPRequest(nodeID, msgSent)
				time.Sleep(time.Duration(5) * time.Millisecond)
			}
		}
	}
}


