package main

import (
	"encoding/json"
	"fmt"
	"math"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)


// structure that stores relevant information
type grepData struct {
	resMap map[string]string
	latency time.Duration
	id int
	server string
}

// func ClientMP1(pattern string, port string, flag string)
// -----------------------------------------------------------
// Description: 	This function is the main function for the client program.
// 				 	It sends the grep instruction to the server, receive the grep
//					result, print the line count to terminal output and save the actual
//					line output to result.log
// Parameters:		pattern string: the pattern that grep command executes on
// 					port string: the port that client will connect to
//					flag string: the flag that grep command executes on
// Return: 			None
func ClientMP1(pattern string, flag string, servers map[int]string) {
	resChannel := make(chan grepData)
	totalLatency := 0.0
	summaryMap := make(map[string]string)
	timeMap := make(map[string]time.Duration)
	numConnectedVMs := 0
	connArr := make(map[int]*net.TCPConn)

	allTime := time.Now()

	// tries to connect all the ten VMs
	for id, server := range servers {
		timeStart := time.Now()
		// attempt to connect to the current VM
		addr, err := net.ResolveTCPAddr("tcp", server + ":" + PORT)
		ErrorHandler("Cannot Resolve Contact Address", err, false)
		connArr[id], err = net.DialTCP("tcp", nil, addr)
		// check if the current server is running
		if err != nil {
			continue
		}
		numConnectedVMs += 1

		// create a new thread to handle the current connection
		go func(i int, flag string, conn *net.TCPConn, server string) {
			// Variables to be used
			var logLine = make(map[string]string)

			// Convert the message to json string and send to server
			msgSend := make(map[string]string)
			msgSend["pattern"] = pattern
			msgSend["flag"] = flag
			msg, err := json.Marshal(msgSend)
			ErrorHandler("Fail to marshal message\n", err, false)

			WriteLog(logFile, "Prepare to send " + pattern + " " + flag + " to server" + server + "\n", false)
			n, err := conn.Write(msg)
			ErrorHandler("Fail to send grep request\n", err, false)
			WriteLog(logFile, "Send " + strconv.Itoa(n) + " bytes\n", false)

			// Push received data to channel
			err = json.NewDecoder(conn).Decode(&logLine)
			ErrorHandler("Fail to unmarshal grep result\n", err, false)
			latency := time.Since(timeStart)
			timeLapse := time.Since(allTime)
			totalLatency = math.Max(totalLatency, float64(timeLapse))
			logData := new(grepData)
			logData.latency = latency
			logData.resMap = logLine
			logData.id = i
			logData.server = server
			resChannel <- *logData
			return
		}(id, flag, connArr[id], server)
	}

	// create variable to store the output to local disk
	var str strings.Builder
	// traverse through all the contents in the residual channel
	for i := 0; i < numConnectedVMs; i++ {
		var hostID string
		res, ok := <- resChannel
		// check if we are trying to get data from empty channel
		if !ok {
			fmt.Printf("Channel Closed!\n")
		}

		str.WriteString("\n-------Start of Server " + res.server + " Query-------\n")
		for line, content := range res.resMap {
			if line == "ERROR" {
				continue
			}
			hostID = strings.Split(line, "/")[0]

			if strings.Split(line, "/")[1] == "lc" {
				summaryMap[line] = content
				continue
			}

			message := "Server (" + hostID + "): " + content + "\n"
			str.WriteString(message)
		}
		timeMap[hostID] = res.latency
		str.WriteString("\n-------END of Server " + res.server + " Query-------\n")
	}

	// print query summary
	msg := fmt.Sprintf("\n----------------Query Summary----------------\n")
	msg += fmt.Sprintf("-> Pattern:     %s\n", pattern)
	msg += fmt.Sprintf("-> grep option: %s\n", flag)
	msg += fmt.Sprintf("-> Line counts:\n")
	ttlLines := 0
	for key, value := range summaryMap {
		msg += fmt.Sprintf("->-> Server %s: %s lines\n", strings.Split(key, "/")[0], value)
		lCount, _ := strconv.Atoi(strings.Split(value, "\n")[0])
		ttlLines += lCount
	}
	msg += fmt.Sprintf("-> Total Lines: %d\n", ttlLines)
	msg += fmt.Sprintf("-> Query Latency:\n")
	for key, value := range timeMap {
		msg += fmt.Sprintf("->-> Server %s: %s \n", key, value)
	}
	msg += fmt.Sprintf("Total Latency: %.4f ms\n", totalLatency/1000000)
	msg += fmt.Sprintf("\n-> The Content returned by grep is in " + queryFile + "\n")
	msg += fmt.Sprintf("\n----------------End of Summary---------------\n")
	fmt.Println(msg)

	WriteLog(queryFile, str.String() + msg, true)
	WriteLog(logFile, msg, false)
}


// func ServerMP1(port string, filename string)
// -----------------------------------------------------------
// Description: 	This function serves as an intermediate stage to handle the server program.
// 					It constantly wait for connection from clients, and it calls
// 					HandleConnectionMP1 if a client tries to connect
// Parameters:		port string: the port that client will connect to
//					filename string: name of the file that grep executes on
// Return: 			None
func ServerMP1() {
	// Listening to fixed port and receives message
	tcpAddr, err := net.ResolveTCPAddr("tcp", ":" + PORT)
	ErrorHandler("TCP Listening Error", err, false)
	listen, err := net.ListenTCP("tcp", tcpAddr)
	ErrorHandler("TCP Listening Error 2", err, false)
	// wait for client to connect
	for {
		// accepts connection
		conn, err := listen.Accept()
		if err != nil {
			continue
		}
		// handle current connection
		HandleConnectionMP1(conn, logFile)
	}
}


// func HandleConnectionMP1(conn net.Conn, filename string)
// -----------------------------------------------------------
// Description: 	This function is the main function of the server program.
//					It reads the grep argument from the client, runs the grep
//					command, and returns the grep result to the client
// Parameters:		conn net.Conn: the Conn object that stores client's information
//					filename string: name of the file that grep executes on
// Return: 			None
func HandleConnectionMP1(conn net.Conn, filename string) {

	// creates a new thread to handle the connection
	go func (conn net.Conn, filename string) {
		// Receiving pattern and flag from client
		var argsMap map[string]string
		err := json.NewDecoder(conn).Decode(&argsMap)
		ErrorHandler("Pattern Not Received\n", err, false)
		pattern := argsMap["pattern"]
		flag := argsMap["flag"]

		fmt.Printf("Received Query Pattern: >> %v <<\n", pattern)
		fmt.Printf("Received Query Flag: >> %v <<\n", flag)

		// Executing grep
		commandResult := exec.Command("grep", flag, pattern, filename)
		grepResult, err := commandResult.CombinedOutput()
		ErrorHandler("Fail to grep\n", err, false)
		grepString := string(grepResult)

		commandResultLine := exec.Command("grep", "-c", pattern, filename)
		grepResultLine, err := commandResultLine.CombinedOutput()
		ErrorHandler("Fail to grep\n", err, false)
		grepStringLine := string(grepResultLine)
		grepStringLine = strings.TrimSuffix(grepStringLine, "\n")

		grepArray := strings.Split(grepString, "\n")
		grepArray = grepArray[:len(grepArray) - 1]
		grepMap := make(map[string]string)

		i := 1

		// convert log lines to dictionary
		host, _ := os.Hostname()
		hostName := strings.Split(host, ".")[0]
		for _, line := range grepArray {
			grepMap[hostName+"/"+string(i)] = line
			i ++
		}
		grepMap[hostName+"/lc"] = grepStringLine

		WriteLog(logFile, "Received Query Pattern: >>" + pattern + "<<\n", false)
		WriteLog(logFile, "Received Query Flag: >>" + flag + "<<\n", false)

		// convert log lines to json object
		msg, err := json.Marshal(grepMap)
		ErrorHandler("Fail to marshal grep result\n", err, false)

		// send the result back to client
		n, err := conn.Write(msg)
		ErrorHandler("Fail to send grep result back\n", err, false)
		WriteLog(logFile, "Grep send " +strconv.Itoa(n)+ " bytes back\n", false)

		_ = conn.Close()
	} (conn, filename)
}
