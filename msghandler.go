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

// func ListeningToMessages()
// ------------------------------------------------------------------
// Description: A routine that will keep running to handle all
//              received messages
// Input:   None
// Output:  None
func ListeningToMessages() {
	// Listening to fixed port and receives message
	udpAddr, err := net.ResolveUDPAddr("udp", ":" + PORT)
	ErrorHandler("Listening Error", err, false)
	conn, err := net.ListenUDP("udp", udpAddr)
	ErrorHandler("Listening Error 2", err, false)

	// for every new connection
	for {
		// Decode the received message
		msgMap := make(map[int]string)
		msgByte := make([]byte, 4096)
		n, addr, err := conn.ReadFromUDP(msgByte)

		if err != nil {
			ErrorHandler("read from udp error: ", err, false)
			continue
		}

		if n == 0 {
			continue
		}

		err = json.Unmarshal(msgByte[0:n], &msgMap)
		if err != nil {
			ErrorHandler("read message error: ", err, false)
			continue
		}

		// Check if the message has been received previously
		if !BasicMessageHandler(&msgMap) {
			continue
		}

		senderID, _ := strconv.Atoi(msgMap[SENDER])
		domain := memberHost[senderID]


		///////////////////////////////
		// Heartbeat message handler //
		///////////////////////////////
		if msgMap[MSGTYPE] == HEARTBEAT {
			sender, _ := strconv.Atoi(msgMap[SENDER])
			validHeartbeat := false

			// check the monitor list to see if this message belongs to the node you monitor
			for _, url := range monitorList {
				if url == memberHost[sender] {
					validHeartbeat = true
				}
			}

			// if the node is one of the node in monitor list, check if the heartbeat message is the
			// newest heartbeat message
			if validHeartbeat {
				receivedTime, err := time.Parse("2006-01-02T15:04:05.000Z", msgMap[TIMESTAMP])
				// if not, drop the message
				if _, ok := lastUpdate[sender]; ok && receivedTime.Before(lastUpdate[sender]) {
					continue
				}
				// if the timestamp is later the the timestamp in lastUpdate list, update the
				// timestamp in the list to the newest one
				lastUpdate[sender] = receivedTime
				lastUpdateLocal[sender] = time.Now()
				ErrorHandler("Cannot Update timestamp", err, false)
			}

			///////////////////////////////////
			// Fail or Leave message handler //
			///////////////////////////////////
		} else if msgMap[MSGTYPE] == FAIL || msgMap[MSGTYPE] == LEAVE {
			// if receives a node failure message, delete the node from member list and update
			// monitor list and heartbeat list
			go func(failNodeID string) {
				failNodeIDInt, _ := strconv.Atoi(failNodeID)
				if _, ok := memberHost[failNodeIDInt]; !ok {
					return
				}
				delete(replicateCounter, failNodeID)

				logMsg := fmt.Sprintf("Node %v: %v %v\n", failNodeID, memberHost[failNodeIDInt], helperMap[msgMap[MSGTYPE]])
				WriteLog(logFile, logMsg, false)
				fmt.Print(logMsg)
				memberLock.Lock()
				_, ok := memberHost[failNodeIDInt]
				if !ok {
					memberLock.Unlock()
					return
				}
				delete(memberHost, failNodeIDInt)
				delete(memberAddr, failNodeIDInt)
				memberLock.Unlock()
				if isContact {
					writeCritical()
				}
				UpdateHeartbeatTarget()
				if isMaster {
					updateReplicaList(failNodeID)
				}
				PrintMemberList()
			}(msgMap[CONTENT])

			/////////////////////////////
			// JOINACK message handler //
			/////////////////////////////
		} else if msgMap[MSGTYPE] == JOINACK {
			// This kind of message should never be received by an existing node in the system
			continue

			/////////////////////////////
			// JOINREQ message handler //
			/////////////////////////////
		} else if msgMap[MSGTYPE] == JOINREQ {
			if !isContact {
				WriteLog(logFile, "Trying to send join request to non master node\n", false)
				continue
			}
			// Update the contact node's member list
			go func(mapPointer *map[int]string) {
				memberLock.Lock()
				memberHost[maxID] = (*mapPointer)[CONTENT]
				memberAddr[maxID] = (*mapPointer)[SENDER]
				memberLock.Unlock()

				PrintMemberList()
				// Prepare for ReqACK message
				msg := make(map[int]string)

				memberLock.Lock()
				for nodeID, addr := range memberHost{
					msg[nodeID] = addr + "\n" + memberAddr[nodeID]
				}
				memberLock.Unlock()

				// send join ack back to the newly joined node with the current membership
				replicateCounter[strconv.Itoa(maxID)] = 0
				msg[SELFKEY] = strconv.Itoa(maxID)
				msgContent, _ := json.Marshal(msg)
				msgSent := MakeMessage(JOINACK, string(msgContent), strconv.Itoa(selfID))

				logMsg := fmt.Sprintf("Receive join request from: %s\n", memberHost[maxID])
				fmt.Print(logMsg)
				WriteLog(logFile, logMsg, false)

				_, err = conn.WriteToUDP([]byte(msgSent), addr)
				ErrorHandler("Write to fails ", err, false)

				// send update list message to all nodes
				updateMsg := make(map[int]string)
				updateMsg[maxID] = memberHost[maxID] + "\n" + memberAddr[maxID]
				updateMsgContent, _ := json.Marshal(updateMsg)
				updateMsgSent := MakeMessage(UPDATELIST, string(updateMsgContent), strconv.Itoa(selfID))

				// Write contact information to the log of contact file
				writeCritical()

				for _, nodeID := range targetList {
					sendRequest(nodeID, updateMsgSent)
					time.Sleep(time.Duration(5) * time.Millisecond)
				}

				UpdateHeartbeatTarget()
				maxID += 1

				nodeIDStr := strconv.Itoa(maxID-1)

				// master sends the write request to every node that has sdfs files
				if isMaster {
					sendReplica(nodeIDStr)
					return
				}
			}(&msgMap)

			/////////////////////////////////////
			// handler for update list message //
			/////////////////////////////////////
		} else if msgMap[MSGTYPE] == UPDATELIST {
			// write log
			sender, _ := strconv.Atoi(msgMap[SENDER])
			logMsg := fmt.Sprintf("Recieved Update List Message from: %v\n", memberHost[sender])
			fmt.Print(logMsg)
			WriteLog(logFile, logMsg, false)
			memberMap := make(map[int]string)
			_ = json.Unmarshal([]byte(msgMap[CONTENT]), &memberMap)
			go func(msgPointer *map[int]string) {
				// new thread to update list
				memberLock.Lock()
				for key, newHost := range *msgPointer {
					if key == selfID {
						continue
					}

					if _, ok := memberHost[key]; !ok {
						if isMaster {
							urlAddrArr := strings.Split(newHost, "\n")
							memberHost[key] = urlAddrArr[0]
							memberAddr[key] = urlAddrArr[1]
							if _, ok := replicateCounter[strconv.Itoa(key)]; !ok {
								replicateCounter[strconv.Itoa(key)] = 0
							}

							sendReplica(strconv.Itoa(key))
							continue
						}
					}

					urlAddrArr := strings.Split(newHost, "\n")
					memberHost[key] = urlAddrArr[0]
					memberAddr[key] = urlAddrArr[1]
				}
				memberLock.Unlock()
				PrintMemberList()
				UpdateHeartbeatTarget()

				if isContact {
					writeCritical()
				}

				if isMaster {
					if _, ok := replicateCounter[msgMap[SENDER]]; !ok {
						replicateCounter[msgMap[SENDER]] = 0
					}
				}
			}(&memberMap)

			//////////////////////////////
			// WRITEREQ message handler //
			//////////////////////////////
		} else if msgMap[MSGTYPE] == WRITEREQ {
			// check if current node is the master
			if !isMaster {
				WriteLog(logFile, "Trying to send write request to non master node\n", false)
				continue
			}

			go func(msgPointer *map[int]string) {
				fileNames := make(map[string]string)
				err = json.Unmarshal([]byte((*msgPointer)[CONTENT]), &fileNames)
				ErrorHandler("Unmarshal error", err, false)
				sdfsFileName := fileNames[SDFSNAME]
				localFileName := fileNames[LOCALNAME]

				logMsg := fmt.Sprintf("Receive put SDFS File %v request from: %s\n", sdfsFileName, domain)
				fmt.Print(logMsg)
				WriteLog(logFile, logMsg, false)

				// check if file already exists
				fileLock.RLock()
				_, ok := replicateList[sdfsFileName]
				if ok {
					receivedTime, err := time.Parse("2006-01-02T15:04:05.000Z", replicateList[sdfsFileName][LASTUPDATE])
					fileLock.RUnlock()
					ErrorHandler("Decoding last update time of replica list error: ", err, false)
					currentTimeString := time.Now().Format("2006-01-02T15:04:05.000Z")
					currentTime, _ := time.Parse("2006-01-02T15:04:05.000Z", currentTimeString)

					// if last update is within one minute
					if currentTime.Before(receivedTime.Add(time.Minute)) {
						overwrite := fileNames[OVERWRITE]
						if overwrite == FALSE {
							// send overwrite request back to user
							sentMap := make(map[string]string)
							sentMap[LOCALNAME] = localFileName
							sentMap[SDFSNAME] = sdfsFileName

							msgContent, _ := json.Marshal(sentMap)
							msgSent := MakeMessage(OVERWRITE, string(msgContent), strconv.Itoa(selfID))

							senderID, _ := strconv.Atoi(msgMap[SENDER])
							sendRequest(senderID, msgSent)

							logMsg := fmt.Sprintf("Pending overwrite SDFS File %v request from sender: %s\n", sdfsFileName, domain)
							fmt.Print(logMsg)
							WriteLog(logFile, logMsg, false)

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
				sender := (*msgPointer)[SENDER]
				addNewFile(sdfsFileName, sender, &receiverMap)

				receiverMap[SENDERNAME] = localFileName
				receiverMap[RECEIVERNAME] = sdfsFileName
				receiverMap[SENDERTYPE] = LOCALNAME
				receiverMap[RECEIVERTYPE] = SDFSNAME
				if localFileName == SDFSNAME {
					receiverMap[SENDERNAME] = sdfsFileName
					receiverMap[SENDERTYPE] = SDFSNAME
				}

				// send replica id back to sender
				msgContent, _ := json.Marshal(receiverMap)
				msgSent := MakeMessage(WRITE, string(msgContent), strconv.Itoa(selfID))

				senderID, _ := strconv.Atoi(msgMap[SENDER])
				sendRequest(senderID, msgSent)

				logMsg = fmt.Sprintf("Send replica information back to node: %s\n", domain)
				fmt.Print(logMsg)
				WriteLog(logFile, logMsg, false)
			}(&msgMap)

			///////////////////////////////
			// OVERWRITE message handler //
			///////////////////////////////
		} else if msgMap[MSGTYPE] == OVERWRITE {
			// user confirm whether to overwrite the file
			go func(msgPointer *map[int]string) {
				fileNames := make(map[string]string)
				err = json.Unmarshal([]byte((*msgPointer)[CONTENT]), &fileNames)
				ErrorHandler("Unmarshal error", err, false)
				sdfsFileName := fileNames[SDFSNAME]
				localFileName := fileNames[LOCALNAME]

				logMsg := fmt.Sprintf("Receive overwrite SDFS File %v request from: %s\n", sdfsFileName, domain)
				fmt.Print(logMsg)
				WriteLog(logFile, logMsg, false)

				cmd := getInput(sdfsFileName)
				if cmd == "y" {
					// user want to overwrite the file
					sentMap := make(map[string]string)
					sentMap[LOCALNAME] = localFileName
					sentMap[SDFSNAME] = sdfsFileName
					sentMap[OVERWRITE] = TRUE

					msgContent, _ := json.Marshal(sentMap)
					msgSent := MakeMessage(WRITEREQ, string(msgContent), strconv.Itoa(selfID))

					senderID, _ := strconv.Atoi(msgMap[SENDER])
					sendRequest(senderID, msgSent)

					logMsg := fmt.Sprintf("Sending overwrite SDFS File %v request to master node: %s\n", sdfsFileName, domain)
					fmt.Print(logMsg)
					WriteLog(logFile, logMsg, false)
				}
			}(&msgMap)

			///////////////////////////
			// WRITE message handler //
			///////////////////////////
		} else if msgMap[MSGTYPE] == WRITE {
			go func(msgPointer *map[int]string) {
				receiverMap := make(map[string]string)
				err = json.Unmarshal([]byte((*msgPointer)[CONTENT]), &receiverMap)
				ErrorHandler("read message error: ", err, false)
				senderName := receiverMap[SENDERNAME]
				receiverName := receiverMap[RECEIVERNAME]
				senderType := receiverMap[SENDERTYPE]
				receiverType := receiverMap[RECEIVERTYPE]

				logMsg := fmt.Sprintf("Receive write request from: %s\n", domain)
				fmt.Print(logMsg)
				WriteLog(logFile, logMsg, false)

				for key, idStr := range receiverMap {
					if key != REPLICAONE && key != REPLICATWO && key != REPLICATHREE && key != REPLICAFOUR {
						continue
					}
					id, _ := strconv.Atoi(idStr)
					// if current node does not have local replica, then senderName and receiverName are the same
					go WriteToNode(senderName, senderType, receiverName, receiverType, id)
				}
			}(&msgMap)

			/////////////////////////////
			// READREQ message handler //
			/////////////////////////////
		} else if msgMap[MSGTYPE] == READREQ {
			// check if current node is the master
			if !isMaster {
				WriteLog(logFile, "Trying to send read request to non master node\n", false)
				continue
			}

			go func(msgPointer *map[int]string) {
				fileNames := make(map[string]string)
				err = json.Unmarshal([]byte((*msgPointer)[CONTENT]), &fileNames)
				ErrorHandler("Unmarshal error", err, false)
				sdfsFileName := fileNames[SDFSNAME]
				localFileName := fileNames[LOCALNAME]
				receiverType := fileNames[RECEIVERTYPE]
				receiverID := fileNames[RECEIVERID]
				var localExist bool
				if fileNames[LOCALEXIST] == TRUE {
					localExist = true
				} else {
					localExist = false
				}

				logMsg := fmt.Sprintf("Receive read SDFS File %v request from: %s\n", sdfsFileName, domain)
				fmt.Print(logMsg)
				WriteLog(logFile, logMsg, false)

				get(localFileName, sdfsFileName, receiverID, receiverType, localExist)

			}(&msgMap)

			///////////////////////////////
			// ERRORREAD message handler //
			///////////////////////////////
		} else if msgMap[MSGTYPE] == ERRORREAD {
			fmt.Printf("SDFS File: %v doesn't exist\n", msgMap[CONTENT])

			///////////////////////////////
			// DELETEREQ message handler //
			///////////////////////////////
		} else if msgMap[MSGTYPE] == DELETEREQ {
			if !isMaster {
				WriteLog(logFile, "Trying to send delete request to non master node\n", false)
				continue
			}

			go func(msgPointer *map[int]string) {
				sdfsFileName := (*msgPointer)[CONTENT]

				logMsg := fmt.Sprintf("Receive delete SDFS File %v request from: %s\n", sdfsFileName, domain)
				fmt.Print(logMsg)
				WriteLog(logFile, logMsg, false)

				if !deleteSDFS(sdfsFileName) {
					msgSent := MakeMessage(ERRORREAD, sdfsFileName, strconv.Itoa(selfID))
					senderID, _ := strconv.Atoi((*msgPointer)[SENDER])
					sendRequest(senderID, msgSent)
				}
			}(&msgMap)

			////////////////////////////
			// DELETE message handler //
			////////////////////////////
		} else if msgMap[MSGTYPE] == DELETE {
			go func(msgPointer *map[int]string) {
				sdfsFileName := (*msgPointer)[CONTENT]
				err := os.Remove(SDFSFILEPATH + sdfsFileName)
				errMsg := fmt.Sprintf("Can't delete sdfs file %v. File does not exist!\n", sdfsFileName)
				ErrorHandler(errMsg, err, false)

				logMsg := fmt.Sprintf("SDFS File %v deleted\n", sdfsFileName)
				fmt.Print(logMsg)
				WriteLog(logFile, logMsg, false)
			} (&msgMap)

			/////////////////////////////////
			// COORDINATOR message handler //
			/////////////////////////////////
		} else if msgMap[MSGTYPE] == COORDINATOR {
			go func(msgPointer *map[int]string) {
				// election ends
				electionLock.Lock()
				isElecting = false
				electionLock.Unlock()

				chanLock.Lock()
				for i := 0; i < okChanNum; i++ {
					okWaitChan <- true
				}
				okChanNum = 0
				for i := 0; i < coChanNum; i++ {
					coWaitChan <- true
				}
				coChanNum = 0
				chanLock.Unlock()

				// set new master
				masterID, _ = strconv.Atoi((*msgPointer)[SENDER])
				logMsg := fmt.Sprintf("New master is node %v: %v\n", masterID, memberHost[masterID])
				fmt.Print(logMsg)
				WriteLog(logFile, logMsg, false)
			}(&msgMap)

			/////////////////////////////////
			// ELECTION message handler //
			/////////////////////////////////
		} else if msgMap[MSGTYPE] == ELECTION {
			go func(msgPointer *map[int]string) {
				// send ok message to sender
				senderID, _ := strconv.Atoi((*msgPointer)[SENDER])
				failNodeID, _ := strconv.Atoi((*msgPointer)[CONTENT])
				msgContent := MakeMessage(OK, "", strconv.Itoa(selfID))
				sendRequest(senderID, msgContent)
				time.Sleep(5 * time.Millisecond)
				// start self election protocol
				newElection(failNodeID)
			}(&msgMap)

			/////////////////////////////
			// MSGTYPE message handler //
			/////////////////////////////
		} else if msgMap[MSGTYPE] == OK {
			// signal stop waiting for ok message
			go func() {
				chanLock.Lock()
				if okChanNum <= 0 {
					chanLock.Unlock()
					return
				}
				okWaitChan <- true
				okChanNum -= 1
				chanLock.Unlock()
			}()

			/////////////////////////////////
			// NEWELECTION message handler //
			/////////////////////////////////
		} else if msgMap[MSGTYPE] == NEWELECTION {
			senderID, _ := strconv.Atoi(msgMap[SENDER])
			go newElection(senderID)

			//////////////////////////////
			// MAPLEREQ message handler //
			//////////////////////////////
		} else if msgMap[MSGTYPE] == MAPLEREQ {
			go func(msgPointer *map[int]string) {
				logMsg := fmt.Sprintf("Receive maple request from: %s\n", domain)
				fmt.Print(logMsg)
				WriteLog(logFile, logMsg, false)

				var jobContent map[string]string
				_ = json.Unmarshal([]byte(msgMap[CONTENT]), &jobContent)
				var newJob JobDescriptor
				newJob.executable = jobContent["exe"]
				newJob.srcDir = jobContent["src"]
				numTasks, _ := strconv.Atoi(jobContent["num"])
				newJob.numTasksMaple = numTasks
				newJob.partition = jobContent["par"]
				newJob.prefix = jobContent["pre"]

				jobLock.Lock()
				jobQueueMaple = append(jobQueueMaple, newJob)
				jobLock.Unlock()
			}(&msgMap)

			//////////////////////////////
			// JUICEREQ message handler //
			//////////////////////////////
		} else if msgMap[MSGTYPE] == JUICEREQ {
			go func(msgPointer *map[int]string) {
				logMsg := fmt.Sprintf("Receive juice request from: %s\n", domain)
				fmt.Print(logMsg)
				WriteLog(logFile, logMsg, false)

				var jobContent map[string]string
				_ = json.Unmarshal([]byte(msgMap[CONTENT]), &jobContent)
				var newJob JobDescriptor
				newJob.executable = jobContent["exe"]
				newJob.destDir = jobContent["des"]
				numTasks, _ := strconv.Atoi(jobContent["num"])
				deletion, _ := strconv.Atoi(jobContent["del"])
				newJob.numTasksJuice = numTasks
				newJob.deletion = deletion
				newJob.partition = jobContent["par"]
				newJob.prefix = jobContent["pre"]

				jobLock.Lock()
				jobQueueJuice = append(jobQueueJuice, newJob)
				jobLock.Unlock()
			}(&msgMap)

			////////////////////////////////
			// MAPLEERROR message handler //
			////////////////////////////////
		} else if msgMap[MSGTYPE] == MAPLEERROR {
			go func(msgPointer *map[int]string) {
				logMsg := fmt.Sprintf("Receive maple error from: %s\n", domain)
				fmt.Print(logMsg)
				WriteLog(logFile, logMsg, false)

				failId, _ := strconv.Atoi(msgMap[SENDER])

				tasksToAllocateMaple = append(tasksToAllocateMaple, taskAssignMaple[failId])
				taskAssignMaple[failId] = -1
			}(&msgMap)

			////////////////////////////////
			// JUICEERROR message handler //
			////////////////////////////////
		} else if msgMap[MSGTYPE] == JUICEERROR {
			go func(msgPointer *map[int]string) {
				logMsg := fmt.Sprintf("Receive juice error from: %s\n", domain)
				fmt.Print(logMsg)
				WriteLog(logFile, logMsg, false)

				failId, _ := strconv.Atoi(msgMap[SENDER])

				tasksToAllocateJuice = append(tasksToAllocateJuice, taskAssignJuice[failId])
				taskAssignJuice[failId] = -1
			}(&msgMap)
		}
 	}
}


