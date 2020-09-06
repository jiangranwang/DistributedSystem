package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"time"
)

// func getCommand()
// ------------------------------------------------------------------
// Description: This function runs a separate thread that takes command from user input
// Input: None
// Output: None
func getCommand() {
	reader := bufio.NewReader(os.Stdin)
	// constantly checking for input command
	for {
		fmt.Printf("%c[%d;%d;%dm%sEnter Command: %c[0m", 0x1B, 37, 42, 1, "", 0x1B)
		cmd, err := reader.ReadString('\n')
		ErrorHandler("Input error: ", err, false)

		if cmd == "\n" {
			continue
		}

		trimS := strings.TrimSuffix(cmd, "\n")
		trim := strings.TrimSpace(trimS)
		split := strings.Split(trim, " ")

		if split[0] == "membership" {
			PrintMemberList()
		} else if split[0] == "selfid" {
			fmt.Printf("%c[%d;%d;%dm%sSelfID is:%c[0m",0x1B, 37, 42, 1, "", 0x1B)
			fmt.Print(selfID, "\n")
		} else if split[0] == "leave" {
			handleLeave()
		} else if split[0] == "localhost" {
			fmt.Printf("%c[%d;%d;%dm%sLocalhost address is: %c[0m",0x1B, 37, 42, 1, "", 0x1B)
			fmt.Print(localHost, localAddr, "\n")
		} else if split[0] == "master" {
			if isMaster {
				fmt.Printf("%c[%d;%d;%dm%sMaster is current node. Address is: %c[0m",0x1B, 37, 42, 1, "", 0x1B)
				fmt.Print(localHost, "\n")
			} else {
				fmt.Printf("%c[%d;%d;%dm%sMaster address is: %c[0m",0x1B, 37, 42, 1, "", 0x1B)
				fmt.Print(memberHost[masterID], "\n")
			}
		} else if split[0] == "query" {
			if len(split) == 2 {
				ClientMP1(split[1], "-n", memberHost)
			} else if len(split) == 3 {
				ClientMP1(split[1], split[2], memberHost)
			} else {
				fmt.Println("Please enter as: query <pattern> <flag>")
			}
		} else if split[0] == "put" {
			if len(split) == 3 {
				localFileName := split[1]
				sdfsFileName := split[2]

				logMsg := fmt.Sprintf("Executing put request: put %v %v\n", localFileName, sdfsFileName)
				WriteLog(logFile, logMsg, false)

				handlePut(localFileName, sdfsFileName, false)
			} else {
				fmt.Println("Please enter as: put <localfilename> <sdfsfilename>")
			}
		} else if split[0] == "putdir" {
			if len(split) == 3 {
				localDir := split[1]
				sdfsPrefix := split[2]

				logMsg := fmt.Sprintf("Executing put request: putdir %v %v\n", localDir, sdfsPrefix)
				WriteLog(logFile, logMsg, false)

				PutWithPrefix(localDir, sdfsPrefix, true)
			} else {
				fmt.Println("Please enter as: putdir <localDir> <sdfsPrefix>")
			}
		} else if split[0] == "get"{
			if len(split) == 3 {
				localFileName := split[2]
				sdfsFileName := split[1]

				logMsg := fmt.Sprintf("Executing get request: get %v %v\n", sdfsFileName, localFileName)
				WriteLog(logFile, logMsg, false)

				handleGet(localFileName, sdfsFileName, true)
			} else {
				fmt.Println("Please enter as: get <sdfsfilename> <localfilename>")
			}
		} else if split[0] == "delete" {
			if len(split) == 2 {
				sdfsFileName := split[1]

				logMsg := fmt.Sprintf("Executing delete request: delete %v\n", sdfsFileName)
				WriteLog(logFile, logMsg, false)

				handleDelete(sdfsFileName)
			} else {
				fmt.Println("Please enter as: delete <sdfsfilename>")
			}
		} else if split[0] == "deletedir" {
			if len(split) == 2 {
				sdfsPrefix := split[1]

				logMsg := fmt.Sprintf("Executing delete request: delete %v\n", sdfsPrefix)
				WriteLog(logFile, logMsg, false)

				DeleteWithPrefix(sdfsPrefix)
			} else {
				fmt.Println("Please enter as: deletedir <sdfspre>")
			}
		}else if split[0] == "ls" {
			if len(split) == 2 {
				sdfsFileName := split[1]
				printSDFSFile(sdfsFileName)
			} else {
				fmt.Println("Please enter as: ls <sdfsfilename>")
			}
		} else if split[0] == "store" {
			if len(split) == 1 {
				printLocalFile()
			} else {
				fmt.Println("Please enter store with no argument")
			}
		} else if split[0] == "maple" {
			if len(split) == 5 {
				mapleDispatch(split[1], split[2], split[3], split[4], "range")
			} else {
				fmt.Println("Please enter as: maple <maple_exe> <num_maples> <sdfs_intermediate_filename_prefix> <sdfs_src_directory>")
			}
		} else if split[0] == "juice" {
			if len(split) == 6 {
				juiceDispatch(split[1], split[2], split[3], split[4], split[5], "range")
			} else {
				fmt.Println("Please enter as: juice <juice_exe> <num_juices> <sdfs_intermediate_filename_prefix> <sdfs_dest_filename> delete_input={0,1}")
			}
		} else if split[0] == "count" {
			fmt.Printf("Counter map: %v\n", replicateCounter)
		} else {
			fmt.Println("No such command!")
			fmt.Println("Available commands: membership, master, leave, query, put, putdir, get, delete, deletedir, ls, store, maple, juice")
		}
		time.Sleep(time.Duration(50) * time.Millisecond)
	}
}

// func service()
// ------------------------------------------------------------------
// Description: The function that integrates four service routines
// Input:   None
// Output:  None
func service() {
	// Thread to keep listening to message
	go ListeningToMessages()

	// Thread that send fail message if detects and node failure
	go FailDetector()

	// Thread that get command line inputs
	go getCommand()

	// Thread that handle query requests
	go ServerMP1()
	go tcpHandler()

	// Thread that send heartbeat message to heartbeat targets
	go HeartBeating()

	// scheduler for maple and juice
	go juiceJobSchedule()
	go MapleJobSchedule()

	// Thread that master send replica list to other nodes periodically
	sendReplicaList()

	// This function should never return
}

// func start()
// ------------------------------------------------------------------
// Description: initialization procedures
// Input:   None
// Output:  None
func start() {
	// initialize log file
	lastLogTime = time.Now().Add(-LOGTIME)
	fLog, _ = os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	_, _ = fLog.Write([]byte("\n\n\n\n\n.......................INITIALIZING....................\n"))
	isElecting = false

	// remove all files in sdfs file directory
	err := os.RemoveAll(SDFSFILEPATH)
	ErrorHandler("Fail to remove sdfs files: ", err, false)
	_ = os.Mkdir(SDFSFILEPATH, os.ModePerm)

	// Thread for receiving new files into sdfs directory
	go FileTransferServerSdfs()

	// Thread for receiving new files into local file directory
	go FileTransferServerLocal()

	// Initialization procedure
	if CheckHostName() {

		// If the node is contact node, proceed contact node initialization procedure
		replicateCounter["0"] = 0
		fmt.Print("-->> Initializing Contact ...\n")
		InitContact()
		fmt.Print("-->> Initialization Completed! \n")
		service()

	} else {

		// If the node is not contact node, proceed non-contact node initialization procedure
		fmt.Print("-->> Request Joining\n")
		if !ReqJoin() {
			fmt.Print("-->> Contact Node is not running, service halt!\n")
			os.Exit(1)
		} else {
			fmt.Print("-->> Service running!\n")
			service()
		}
	}
}



// func main()
// -------------------------------------------------------
// Description: The service procedure
// Input: None
// Output: None
func main() {
	start()

	// Prevent the service end
	for {
		time.Sleep(time.Hour)
	}
}


