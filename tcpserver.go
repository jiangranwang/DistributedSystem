package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"strconv"
)

func tcpHandler() {

	listen, err := net.Listen("tcp", ":" + TCPPORT)
	ErrorHandler("TCP Listening Error", err, false)

	for {
		conn, err := listen.Accept()

		if err != nil {
			fmt.Println("*************AcceptERROR")
			fmt.Println(err.Error())
			continue
		}
		// Decode the received message
		msgMap := make(map[int]string)
		newMsg, err := ioutil.ReadAll(conn)
		// fmt.Println(string(newMsg))
		err = json.Unmarshal(newMsg, &msgMap)
		if err != nil {
			fmt.Println("*************DecodeError")
			fmt.Println(err.Error())
			continue
		}

		if msgMap[MSGTYPE] != REPLICALIST {
			fmt.Println("*********************new connect comming in")
		}

		if msgMap[MSGTYPE] == JUICECOM {
			fmt.Println("juice complete message received")
			sender, _ := strconv.Atoi(msgMap[SENDER])

			resultLockJuice.Lock()
			result.WriteString(msgMap[CONTENT])
			completionMap[taskAssignJuice[sender]] = true
			resultLockJuice.Unlock()

			taskAssignJuice[sender] = -1

		} else if msgMap[MSGTYPE] == JUICE {
			fmt.Println("juice message received")
			go func(msgPointer *map[int]string) {

				var jobContent map[int]string
				_ = json.Unmarshal([]byte(msgMap[CONTENT]), &jobContent)

				exe := jobContent[EXECUTABLE]
				deletion, _ := strconv.Atoi(jobContent[DEL])
				var fileList []string
				var fileSize []int64
				_ = json.Unmarshal([]byte(jobContent[FILELIST]), &fileList)
				_ = json.Unmarshal([]byte(jobContent[FILESIZE]), &fileSize)
				JuiceExe(LOCALFILEPATH + exe, fileList, fileSize, deletion)

			}(&msgMap)

		} else if msgMap[MSGTYPE] == MAPLECOM {
			fmt.Println("maple complete message received")
			sender, _ := strconv.Atoi(msgMap[SENDER])

			resultLockMaple.Lock()
			result.WriteString(msgMap[CONTENT])
			completionMap[taskAssignMaple[sender]] = true
			resultLockMaple.Unlock()

			taskAssignMaple[sender] = -1

		} else if msgMap[MSGTYPE] == MAPLE {
			fmt.Println("maple message received")
			go func(msgPointer *map[int]string) {

				var jobContent map[int]string
				_ = json.Unmarshal([]byte(msgMap[CONTENT]), &jobContent)

				exe := jobContent[EXECUTABLE]
				var fileList []string
				var fileSize []int64
				_ = json.Unmarshal([]byte(jobContent[FILELIST]), &fileList)
				_ = json.Unmarshal([]byte(jobContent[FILESIZE]), &fileSize)
				MapleExe(LOCALFILEPATH + exe, fileList, fileSize)

			}(&msgMap)

		} else if msgMap[MSGTYPE] == WRITEBATCH {
			go func(msgPointer *map[int]string) {
				// extract message information
				receiverMap := make(map[string]string)
				fileNameMap := make(map[int]string)
				err = json.Unmarshal([]byte((*msgPointer)[CONTENT]), &receiverMap)
				ErrorHandler("write batch message error: ", err, false)
				senderDir := receiverMap[SENDERDIR]
				receiverDir := receiverMap[RECEIVERDIR]
				senderType := receiverMap[SENDERTYPE]
				receiverType := receiverMap[RECEIVERTYPE]
				receiverID, _ := strconv.Atoi(receiverMap[REPLICAONE])
				err = json.Unmarshal([]byte(receiverMap[SENDERNAME]), &fileNameMap)
				ErrorHandler("unmarshal file name map error: ", err, false)

				// log message
				logMsg := fmt.Sprintf("write batch message received\n")
				fmt.Print(logMsg)
				WriteLog(logFile, logMsg, false)

				// write batch
				for _, fileName := range fileNameMap {
					WriteToNode(senderDir + fileName, senderType, receiverDir + fileName, receiverType, receiverID)
				}
			}(&msgMap)

		} else if msgMap[MSGTYPE] == REPLICALIST {
			masterID, _ = strconv.Atoi(msgMap[SENDER])
			if masterID == selfID {
				continue
			}

			isMaster = false
			receiverMap := make(map[string]string)
			err = json.Unmarshal([]byte(msgMap[CONTENT]), &receiverMap)
			ErrorHandler("Unmarshal error all", err, false)
			newCounter := make(map[string]int)
			err = json.Unmarshal([]byte(receiverMap[SDFSCOUNT]), &newCounter)
			ErrorHandler("Unmarshal error count", err, false)
			newList := make(map[string]map[string]string)
			err = json.Unmarshal([]byte(receiverMap[SDFSLIST]), &newList)
			ErrorHandler("Unmarshal error list", err, false)
			replicateList = newList
			replicateCounter = newCounter
			go checkList()
		}
	}
}