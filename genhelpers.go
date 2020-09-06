package main

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"time"
)

///////////////////////////////////////////////////
/////////                     /////////////////////
/////////  Helper Functions   /////////////////////
/////////                     /////////////////////
///////////////////////////////////////////////////


// func geneUniqueID() string
// ------------------------------------------------------------------
// Description: This function generates unique keys for each message
// Input: None
// Output: A string in format "xxxx-xx-xx-xx-xxxxx"
// source: https://yourbasic.org/golang/generate-uuid-guid/
func geneUniqueID() string {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		log.Fatal(err)
	}
	uuid := fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
	return uuid
}


// func ErrorHandler (description string, err error, exit bool)
// ------------------------------------------------------------------
// Description: This function is the default error handler, it will log
//              error information into service.log
// Input:   description string: error description;
// 	        err error: the error object;
//          exit bool: true to exit the program
// Output: None
func ErrorHandler (description string, err error, exit bool) {
	if err == nil {
		return
	}
	WriteLog(logFile, description + ": " + err.Error() + "\n", false)
	if exit {
		os.Exit(1)
	}
}


// func WriteLog (filename string, content string, clear bool)
// ------------------------------------------------------------------
// Description: This is a helper function to help you writing to file
// Input:   filename string: the file you want to write;
//          content string: the content you want to write to file;
//          clear bool: true to clear the file before writing, false to append
// Output: None
func WriteLog (filename string, content string, clear bool) {
	if filename != logFile {
		if clear {
			_ = os.Remove(filename)
			f, _ := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0644)
			_, err := f.Write([]byte(content + "\n"))
			if err != nil {
				fmt.Print("Cannot Write to File\n")
			}
			err = f.Close()
			if err != nil {
				fmt.Print("Cannot Close File\n")
			}

		} else {
			f, _ := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			var err error
			// update time in log every LOGTIME
			if lastLogTime.Add(LOGTIME).Before(time.Now()) {
				lastLogTime = time.Now()
				currTime := time.Now().Format("2006-01-02T15:04:05.000Z")
				_, err = f.Write([]byte("\n---------->>CURRENT TIME IS: " + currTime + " MESSAGE BELOW<<---------\n"))
			}
			_, err = f.Write([]byte(content))
			if err != nil {
				fmt.Print("Cannot Write to File\n")
			}
			err = f.Close()
			if err != nil {
				fmt.Print("Cannot Close File\n")
			}
		}
	} else {
		if clear {
			_ = fLog.Close()
			_ = os.Remove(filename)
			fLog, _ = os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0644)
			_, err := fLog.Write([]byte(content + "\n"))
			if err != nil {
				fmt.Print("Cannot Write to File\n")
			}

		} else {
			var err error
			// update time in log every LOGTIME
			if lastLogTime.Add(LOGTIME).Before(time.Now()) {
				lastLogTime = time.Now()
				currTime := time.Now().Format("2006-01-02T15:04:05.000Z")
				_, err = fLog.Write([]byte("\n---------->>CURRENT TIME IS: " + currTime + " MESSAGE BELOW<<---------\n"))
			}
			_, err = fLog.Write([]byte(content))
			if err != nil {
				fmt.Print("Cannot Write to File\n")
			}
		}
	}
}


func writeCritical() {
	criticalMsg := make(map[int]string)
	for key := range memberHost {
		criticalMsg[key] = memberHost[key] + "\n" + memberAddr[key]
	}
	msgWrite, _ := json.Marshal(criticalMsg)
	WriteLog(criticalFile, string(msgWrite), true)
}


// func ReadFromFile(filename string, clear bool) string
// ------------------------------------------------------------------
// Description: This is a helper function to help read content from file
// Input:   filename string: the file you want to read from;
// 	        clear string: true to clear the file after reading
// Output:  string type: the content read from the file
func ReadFromFile(filename string, clear bool) string {
	content, _ := ioutil.ReadFile(filename)
	if clear {
		f, _ := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0644)
		_ = f.Close()
	}
	return string(content)
}

// func sendRequest(receiverID int, msgSent string)
// ------------------------------------------------------------------
// Description: A helper function helps to send message to some other node
// Input:   receiverID int: receiver's node ID
//			msgSent string: the content of the message to be sent
// Output:  None
func sendRequest(receiverID int, msgSent string) {
	conn1, err := net.Dial("udp", memberAddr[receiverID] + ":"+ PORT)
	ErrorHandler("Cannot Dial to Contact Address", err, false)
	if err != nil {
		return
	}

	_, err = conn1.Write([]byte(msgSent))
	ErrorHandler("Send request to receiver address fails", err, false)

	err = conn1.Close()
	ErrorHandler("Closing connection fails: ", err, false)
}


func sendTCPRequest(receiverID int, msgSent string) {
	conn2, err := net.Dial("tcp",  memberAddr[receiverID] + ":" + TCPPORT)
	ErrorHandler("Cannot Dial to TCP Contact Address", err, false)
	if err != nil {
		return
	}

	_, err = conn2.Write([]byte(msgSent))
	ErrorHandler("Send request to TCP receiver address fails", err, false)

	err = conn2.Close()
	ErrorHandler("Closing TCP connection fails: ", err, false)
}


// func CheckHostName() bool
// ------------------------------------------------------------------
// Description: A helper function that decide whether the node is the
//              contact node
// Input:   None
// Output:  true if the node is, false if the node isn't
func CheckHostName() bool {
	localHostName, _ := os.Hostname()
	if localHostName == contactAddress {
		return true
	}
	return false
}

func isDir(fileName string) bool {
	fileInfo, err := os.Stat(fileName)
	if err != nil {
		return false
	}
	return fileInfo.IsDir()
}