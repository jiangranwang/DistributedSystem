package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
)

// This portion of code is responsible for reliable file transfer from
// one virtual machine to another virtual machine. It supports all of
// the following operations:
// 		1. local directory -> sdfs directory
//		2. sdfs directory -> local directory
//		3. one sdfs directory -> another sdfs directory
// The implementation is modified from Mr.Waggel's following blog:
// 		Golang transfer a file over a TCP socket
// Source: https://mrwaggel.be/post/golang-transfer-a-file-over-a-tcp-socket/

const (
	BUFFERSIZE = 4096
)

// func Exist(filename string) bool
// ----------------------------------------------------------
// Description: A helper function that checks whether a file exists
// 				in the given directory
// Input: filename (string): the file we are looking for
// Output: (bool): true if the file exists, false otherwise
func Exist(filename string) bool {
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		return false
	} else {
		return true
	}
}


// func fillString(returnString string, toLength int) string
// ---------------------------------------------------------
// Description: This function will append (toLength - len(returnString)) ":"
//				Characters to a given string and return the modified string
//				in order to facilitate the data transfer
// Input: 		returnString (string): The original string
// 				toLength (int): The length of the string we want to get
// Output:		returnString (string)
func fillString(returnString string, toLength int) string {
	for {
		lengthString := len(returnString)
		if lengthString < toLength {
			returnString = returnString + ":"
			continue
		}
		break
	}
	return returnString
}


// func SendFileToServer(connection net.Conn, filename string, filename2 string)
// -----------------------------------------------------------------------------
// Description: This function will send the local file with path filename1 given
//				to another process that the connection given is connecting to
// Input: 		connection (net.Conn): The TCP connection that passed by FileTransferClient
// 				filename (string): The path of the file we want to send
//				filename2 (string): The path where the file will be stored in the receiving process
// Output:		None
func SendFileToServer(connection net.Conn, filename string, filename2 string) {
	//fmt.Printf("Connected to file transfer server!\n")
	defer connection.Close()
	//fmt.Printf(filename + "\n")
	file, err := os.Open(filename)
	if err != nil {
		fmt.Printf("Fail to open the file!\n")
		return
	}
	fileInfo, _ := file.Stat()
	fileSize := fillString(strconv.FormatInt(fileInfo.Size(), 10), 10)
	fileName2 := fillString(filename2, 64)
	//fmt.Println("Sending fileName and fileSize! ")
	_, _ = connection.Write([]byte(fileSize))
	_, _ = connection.Write([]byte(fileName2))
	sendBuffer := make([]byte, BUFFERSIZE)
	//fmt.Println("Start sending file! ")
	for {
		_, err = file.Read(sendBuffer)
		if err == io.EOF {
			break
		}
		_, _ = connection.Write(sendBuffer)
	}
	//fmt.Println("File has been sent, closing connection!")
	return
}

// func FileTransferClient(ip string, type1 string, filename string, type2 string, filename2 string)
// -----------------------------------------------------------------------------
// Description: This function will try to establish a connection with another process
//				given an ip address. Then, it will send the file we want to transfer
//				to the corresponding receiving thread running in the receiving process
// Input: 		ip (string): The ip address of the receiving process
// 				type1 (string): "local/" or "sdfs/", which is the directory where the file is located
//				filename (string): The file that we want to send
// 				type2 (string): "local/" or "sdfs/", which is the directory where the file will received by the receiving process
//				filename2 (string): The name that the file will be saved as
// Output:		None
func FileTransferClient(ip string, type1 string, filename string, type2 string, filename2 string) {
	if type2 == LOCALFILEPATH {
		connection, err := net.Dial("tcp", ip + ":" + LOCALPORT)
		if err != nil {
			fmt.Printf("LOCAL File Cannot dial to server with error: %v\n", err.Error())
			return
		}
		SendFileToServer(connection, type1 + filename, filename2)
	} else if type2 == SDFSFILEPATH {
		connection, err := net.Dial("tcp", ip + ":" + SDFSPORT)
		if err != nil {
			fmt.Printf("SDFS File Cannot dial to server with error: %v\n", err.Error())
			return
		}
		SendFileToServer(connection, type1 + filename, filename2)
	}

}

// func ReceiveFileFromClient(connection net.Conn, filetype string)
// -----------------------------------------------------------------------------
// Description: A helper routine that will retrieve the file from the connection buffer and store
//				the file to the corresponding directory
// Input: 		connection (net.Conn): The TCP connection that passed by one of the receiving server
//				filetype (string): "local/" or "sdfs/", which is the directory where the file is going to be stored
// Output:		None
func ReceiveFileFromClient(connection net.Conn, filetype string) {
	defer connection.Close()

	bufferFileName := make([]byte, 64)
	bufferFileSize := make([]byte, 10)
	_, _ = connection.Read(bufferFileSize)
	fileSize, _ := strconv.ParseInt(strings.Trim(string(bufferFileSize), ":"), 10, 64)

	_, _ = connection.Read(bufferFileName)
	fileName2 := strings.Trim(string(bufferFileName), ":")

	if Exist(filetype + fileName2) {
		_ = os.Remove(filetype + fileName2)
	}

	newFile, _ := os.Create(filetype + fileName2)

	fmt.Print("Receive file <" + fileName2 + "> stores in path <" + filetype + ">\n")

	defer newFile.Close()

	var receivedBytes int64

	for {
		if (fileSize - receivedBytes) < BUFFERSIZE {
			_, _ = io.CopyN(newFile, connection, fileSize - receivedBytes)
			_, _ = connection.Read(make([]byte, (receivedBytes+BUFFERSIZE)-fileSize))
			break
		}
		_, _ = io.CopyN(newFile, connection, BUFFERSIZE)
		receivedBytes += BUFFERSIZE
	}
}

// func FileTransferServerLocal()
// -----------------------------------------------------------------------------
// Description: One of the routine that will be running in the backend. Receiving connections
//				that transfer files to the local directory of this process
// Input: 		None
// Output:		None
func FileTransferServerLocal() {
	serverConn, _ := net.Listen("tcp", ":" + LOCALPORT)
	for {
		conn, err := serverConn.Accept()
		if err != nil {
			fmt.Printf("Cannot accept connection")
			return
		}
		go ReceiveFileFromClient(conn, LOCALFILEPATH)
	}
}

// func FileTransferServerSdfs()
// -----------------------------------------------------------------------------
// Description: One of the routine that will be running in the backend. Receiving connections
//				that transfer files to the sdfs directory of this process
// Input: 		None
// Output:		None
func FileTransferServerSdfs() {
	serverConn, _ := net.Listen("tcp", ":" + SDFSPORT)
	for {
		conn, err := serverConn.Accept()
		if err != nil {
			fmt.Printf("Cannot accept connection")
			return
		}
		go ReceiveFileFromClient(conn, SDFSFILEPATH)
	}
}

