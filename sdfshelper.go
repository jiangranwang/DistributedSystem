package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)

// func localCopy(src string, dest string)
// ------------------------------------------------------------------
// Description: A helper function helps to copy the file locally
// Input:   src string: path to file source
//			dest string: path to file destination
// Output:  None
func localCopy(src string, dest string) {
	in, err := os.Open(src)
	if err != nil {
		ErrorHandler("Error in opening file: ", err, false)
		return
	}

	out, err := os.Create(dest)
	if err != nil {
		ErrorHandler("Error in creating new file: ", err, false)
		return
	}

	_, err = io.Copy(out, in)
	ErrorHandler("Error in copying file: ", err, false)

	logMsg := fmt.Sprintf("Locally copying file from %v to %v\n", src, dest)
	fmt.Print(logMsg)
	WriteLog(logFile, logMsg, false)

	err = in.Close()
	ErrorHandler("Error in closing source file: ", err, false)
	err = out.Close()
	ErrorHandler("Error in closing destination file: ", err, false)
}


// func printSDFSFile(sdfsFileName string)
// ------------------------------------------------------------------
// Description: This function prints the replica list
// Input: 	sdfsFileName string: the name of the sdfs file we want to print
// Output: None
func printSDFSFile(sdfsFileName string) {
	// check if file exists
	if _, ok := replicateList[sdfsFileName]; !ok {
		fmt.Printf("SDFS File %v does not exist.\n", sdfsFileName)
		return
	}

	fmt.Printf("\n%c[%d;%d;%dm%s>>>>>>SDFS File %v Location<<<<<%c[0m \n", 0x1B, 37, 46, 1, "",sdfsFileName, 0x1B)
	for key, nodeIDStr := range replicateList[sdfsFileName] {
		if key != LASTUPDATE && replicateList[sdfsFileName][key] != "" {
			nodeID, err := strconv.Atoi(nodeIDStr)
			ErrorHandler("Can't get sdfs replica node id: ", err, false)
			domain := memberHost[nodeID]
			if nodeID == selfID {
				domain = localHost
			}
			fmt.Printf("%c[%d;%d;%dm%s--->>SDFS copy is at node: %v%c[0m\n",0x1B, 32, 40, 1, "", domain, 0x1B)
		}
	}
	fmt.Printf("%c[%d;%d;%dm%s>>>>END SDFS File %v Location<<<%c[0m \n", 0x1B, 37, 46, 1, "",sdfsFileName, 0x1B)
}


// func printLocalFile()
// ------------------------------------------------------------------
// Description: This function prints all the files that are stored on this node
// Input: None
// Output: None
func printLocalFile() {
	fmt.Printf("\n%c[%d;%d;%dm%s----->>>>>Current Replica List<<<<<-----%c[0m \n", 0x1B, 37, 46, 1, "", 0x1B)
	sdfsFiles, err := ioutil.ReadDir(SDFSFILEPATH)
	ErrorHandler("Can't get files in sdfs directory: ", err, false)
	for _, file := range sdfsFiles {
		fileInfo, _ := os.Stat(SDFSFILEPATH + file.Name())
		if fileInfo.IsDir() {
			fmt.Printf("%c[%d;%d;%dm%s-->>(SDFS Files) %v %c[0m\n", 0x1B, 32, 4, 1, "", file.Name() + "/", 0x1B)
		} else {
			fmt.Printf("%c[%d;%d;%dm%s-->>(SDFS Files) %v %c[0m\n", 0x1B, 32, 4, 1, "", file.Name(), 0x1B)
		}
	}
	fmt.Printf("%c[%d;%d;%dm%s----->>>>>  End Replica List  <<<<<-----%c[0m \n", 0x1B, 37, 46, 1, "", 0x1B)
}


// func setReplicaID(senderStr string, recPointer *map[string]string)
// ------------------------------------------------------------------
// Description: A helper function that helps to decide where will the
//				replicas being stored
// Input:   senderStr string: the id of the node which has the local file
// 			recPointer *map[string]string: the map to be filled in with replica node id
// Output:  None
func setReplicaID(senderStr string, recPointer *map[string]string) {
	type KV struct {
		Key 	string
		Value 	int
	}

	// number of machine that is online
	numOnline := len(replicateCounter)

	keyArr := make([]KV, 0, numOnline)
	for key, val := range replicateCounter {
		keyArr = append(keyArr, KV{key, val})
	}
	sort.Slice(keyArr, func(i, j int) bool {
		return keyArr[i].Value < keyArr[j].Value
	})

	replicaNode := make([]string, 0, numOnline - 1)
	for _, kv := range keyArr {
		if kv.Key == senderStr {
			continue
		}
		replicaNode = append(replicaNode, kv.Key)
	}

	(*recPointer)[REPLICAONE] = senderStr
	replicateCounter[senderStr]++

	// get id of the three successors
	if numOnline >= 2 {
		(*recPointer)[REPLICATWO] = replicaNode[0]
		replicateCounter[replicaNode[0]]++
	}
	if numOnline >= 3 {
		(*recPointer)[REPLICATHREE] = replicaNode[1]
		replicateCounter[replicaNode[1]]++
	}
	if numOnline >= 4 {
		(*recPointer)[REPLICAFOUR] = replicaNode[2]
		replicateCounter[replicaNode[2]]++
	}
}


// func getFileID(sdfsFileName string) (string, string)
// ------------------------------------------------------------------
// Description: A helper function that gets where the sdfs file is stored
// Input:   sdfsFileName string: the name of the sdfs file we want to get
// Output:  The first value specifies the stored type, and it should either
//			be a local file or a sdfs file. The second value specifies the
//			node id where the replica is stored
func getFileID(sdfsFileName string, requester string, localExist bool) string {
	requesterID, _ := strconv.Atoi(requester)
	fileLock.RLock()
	// check if file exists
	if _, ok := replicateList[sdfsFileName]; !ok {
		fileLock.RUnlock()
		fmt.Printf("sdfs file %v doesn't exist\n", sdfsFileName)
		return FALSE
	}

	avaiNode := ""
	// iterate through replicas
	for _, key := range replicaMap {
		if replicateList[sdfsFileName][key] != "" {
			nodeID, _ := strconv.Atoi(avaiNode)
			if nodeID == requesterID && localExist == true {
				fileLock.RUnlock()
				return strconv.Itoa(nodeID)
			}
			avaiNode = replicateList[sdfsFileName][key]
		}
	}

	fileLock.RUnlock()
	if avaiNode != "" {
		return avaiNode
	}

	fmt.Printf("ERROR! sdfs file %v doesn't exist\n", sdfsFileName)
	return FALSE
}


// func setReplaceID(sdfsFileName string, nodeID string)
// ------------------------------------------------------------------
// Description: This is the helper function of the updateReplicaList
//				function. This function is called when any node leave or
//				fail, and this function finds the new node that stores the
//				additional replicas and send the file to the other nodes.
// Input:   sdfsFileName string: the name of the sdfs file that is originally
//								 on the leave/fail node
//			nodeID string: the node id of the leave/fail node
// Output:  None
func setReplaceID(sdfsFileName string, nodeID string) {
	deleteKey := ""

	// create a copy of memberHost map
	memberHostCopy := make(map[int]string)
	for key := range memberHost {
		memberHostCopy[key] = ""
	}
	memberHostCopy[selfID] = ""

	fileLock.Lock()
	// traverse the map to find the fail/leave node
	for key := range replicateList[sdfsFileName] {
		if key != LASTUPDATE && replicateList[sdfsFileName][key] != "" {
			if nodeID == replicateList[sdfsFileName][key] {
				replicateList[sdfsFileName][key] = ""
				deleteKey = key
				continue
			}
			currID, _ := strconv.Atoi(replicateList[sdfsFileName][key])
			delete(memberHostCopy, currID)
		}
	}
	fileLock.Unlock()

	// check if current sdfs file is stored on the failed node
	if deleteKey == "" {
		return
	}

	// pick any node in the remaining members as the new node replica
	for newKey := range memberHostCopy {
		newKeyStr := strconv.Itoa(newKey)
		get(sdfsFileName, sdfsFileName, newKeyStr, SDFSNAME, false)
		replicateCounter[newKeyStr]++
		fileLock.Lock()
		replicateList[sdfsFileName][deleteKey] = newKeyStr
		fileLock.Unlock()
		return
	}
}


// func updateReplicaList(nodeID string)
// ------------------------------------------------------------------
// Description: This function send additional replicas to other nodes
//				when any node leave or fail
// Input:   nodeID string: the node id of the leave/fail node
// Output:  None
func updateReplicaList(nodeID string) {
	// traverse each sdfs file in the replica list
	for sdfsFileName := range replicateList {
		setReplaceID(sdfsFileName, nodeID)
	}
}


// func sendReplica(nodeID string)
// ------------------------------------------------------------------
// Description: This function send additional replicas to the newly joined
//				node when the original system does not store enough replicas
// Input:   nodeID string: the node id of the newly joined node
// Output:  None
func sendReplica(nodeID string) {
	// check if originally there were at least 4 members
	if len(memberHost) > 3 {
		return
	}

	// traverse the replica list
	for sdfsFileName, sdfsMap := range replicateList {
		// check for empty spot in replica list
		for key := range sdfsMap {
			if key != LASTUPDATE && sdfsMap[key] == "" {
				logMsg := fmt.Sprintf("Sending SDFS File %v replica to the newly joined node\n", sdfsFileName)
				fmt.Print(logMsg)
				WriteLog(logFile, logMsg, false)

				// replicate the current sdfs file to the new node
				get(sdfsFileName, sdfsFileName, nodeID, SDFSNAME, false)
				time.Sleep(time.Duration(5) * time.Millisecond)

				replicateList[sdfsFileName][key] = nodeID
				break
			}
		}
		replicateCounter[nodeID]++
	}
}


// func checkList()
// ------------------------------------------------------------------
// Description: This helper function checks whether the list received
//				from the master node is consistent with the files that
//				are present in the current node. This function is invoked
//				whenever we received a replica list message.
// Input:   None
// Output:  None
func checkList() {
	localSDFSFiles, err := ioutil.ReadDir(SDFSFILEPATH)
	ErrorHandler("Can't get files in sdfs directory: ", err, false)

	selfIDStr := strconv.Itoa(selfID)
	for sdfsFileName, sdfsMap := range replicateList {
		for key, val := range sdfsMap {
			// we only check if sdfs file replicas are consistent
			if key != LASTUPDATE && val == selfIDStr {
				exist := false
				for _, file := range localSDFSFiles {
					if file.Name() == sdfsFileName {
						exist = true
						break
					}
				}

				// check if the sdfs replica exists on the current node
				if exist == false {
					logMsg := fmt.Sprintf("SDFS File %v does not exist on the current node. Inconsistency found. Getting copies...\n", sdfsFileName)
					fmt.Print(logMsg)
					WriteLog(logFile, logMsg, false)

					// send artificial read request to master node
					sentMap := make(map[string]string)
					sentMap[LOCALNAME] = sdfsFileName
					sentMap[SDFSNAME] = sdfsFileName
					sentMap[RECEIVERTYPE] = SDFSNAME
					sentMap[RECEIVERID] = strconv.Itoa(selfID)
					sentMap[LOCALEXIST] = FALSE
					msgContent, _ := json.Marshal(sentMap)
					msgSent := MakeMessage(READREQ, string(msgContent), strconv.Itoa(selfID))

					sendRequest(masterID, msgSent)
					// prevent overwhelming send request
					time.Sleep(time.Duration(5) * time.Millisecond)
				}
			}
		}
	}
}
