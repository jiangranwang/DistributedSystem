package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ##################################
// ##  global variables for juice  ##
// ##################################


var jobQueueMaple = make([]JobDescriptor, 0)
var jobRunning = false

var fileMapMaple = make(map[int]string)		// the array stores all file name, only used by master
var tasksToAllocateMaple = make([]int, 0)	// map that maps each task to a member id, only used by master
var taskAssignMaple = make(map[int]int)		// task assignment

var FileQueueMaple = make([]string, 0)		// the task for current node
var FileQueueSizeMaple = make([]int64, 0)
var jobLock sync.Mutex
var resultLockMaple sync.Mutex


// ###########################
// ##  functions for maple  ##
// ###########################

// func mapleDispatch(executable string, numMapleStr string, prefix string, srcDir string, partition string)
// --------------------------------------------------------------------------------------------------------------
// @description: receive a new submitted job, perform sanity check first
//               If the node is master, append the new job to the queue, else send to master
// @input: none
// @return: none
func mapleDispatch(executable string, numMapleStr string, prefix string, srcDir string, partition string) {

	// 1. sanity check
	numTasksMaple, err := strconv.Atoi(numMapleStr)
	if err != nil {
		fmt.Printf("*MapleERROR!! The <num_maples> parameter is not a integer! Abort\n")
		return
	}
	if !Exist(LOCALFILEPATH + executable) {
		fmt.Printf("*MapleERROR!! The executable %s does not exist in the SDFS system! Abort\n", executable)
		return
	}
	if partition != "hash" && partition != "range" {
		fmt.Printf("*JuiceERROR!! The <partition> can be either 'hash' or 'range'! Abort\n")
		return
	}

	// 2. if it is master, add the job to current list, otherwise, send the info to master
	if isMaster {
		var newMaple JobDescriptor
		newMaple.executable = executable
		newMaple.srcDir = srcDir
		newMaple.prefix = prefix
		newMaple.partition = partition
		newMaple.numTasksMaple = numTasksMaple

		jobQueueMaple = append(jobQueueMaple, newMaple)
	} else {
		content := map[string]string{
			"exe" : executable,
			"num" : numMapleStr,
			"pre" : prefix,
			"src" : srcDir,
			"par" : partition,
		}
		contentStr, _ := json.Marshal(content)
		msgSent := MakeMessage(MAPLEREQ, string(contentStr), strconv.Itoa(selfID))
		sendRequest(masterID, msgSent)
	}
}


// func MapleJobSchedule()
// --------------------------------------------------------------------------------------------------------------
// @description: A thread will keep running at backend to schedule juice job
// @input: none
// @return: none
func MapleJobSchedule() {
	for {
		if isMaster {
			if !jobRunning && len(jobQueueMaple) != 0 {
				jobLock.Lock()
				jobRunning = true
				jobLock.Unlock()
				curJob := jobQueueMaple[0]
				jobQueueMaple = jobQueueMaple[1:]
				go MapleMaster(curJob)
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// func MapleMaster(descriptor JobDescriptor)
// --------------------------------------------------------------------------------------------------------------
// @description: Initialization procedure for the master of juice
// @input: none
// @return: none
func MapleMaster(descriptor JobDescriptor) {
	fileMapMaple = make(map[int]string)
	eachTaskFiles = make(map[int][]string)
	eachTaskFileSize = make(map[int][]int64)
	updateList = make(map[int]int)			// the key is ID of worker(node), value is task, indicating which node need to be sent with new messsage
	tasksToAllocateMaple = make([]int, 0)	// map that maps each task to a member id, only used by master
	taskAssignMaple = make(map[int]int)		// task assignment
	result = strings.Builder{}
	completionMap = make(map[int]bool)

	numTasksMaple := descriptor.numTasksMaple
	executable := descriptor.executable
	prefix := descriptor.prefix
	srcDir := descriptor.srcDir
	partition := descriptor.partition

	// 0. set the master flag and sanity check
	fmt.Printf("---------Maple is running!---------\n")
	fmt.Printf("*Executable:       %s\n", executable)
	fmt.Printf("*SDFS File Prefix: %s\n", prefix)
	fmt.Printf("*SDFS File srcPre: %s\n", srcDir)
	fmt.Printf("*Number of tasks:  %d\n", numTasksMaple)
	fmt.Printf("-----------------------------------\n")

	// 1. find all the files with the prefix of srcDir given and append them to fileMapMaple
	i := 0
	for fileName := range replicateList {
		if strings.HasPrefix(fileName, srcDir) {
			fileMapMaple[i] = fileName
			i += 1
		}
	}

	if len(fileMapMaple) == 0 {
		// if 0 file found, aborting
		fmt.Printf("0 source files found in sdfs directory, abort Maple ...\n")
		jobLock.Lock()
		jobRunning = false
		jobLock.Unlock()
		return
	} else {
		fmt.Printf("There are %d files found, partitioning ...\n", i)
	}

	start := time.Now()
	// 2. Partition the files tasks
	if partition == "hash" {
		hashPartitionMaple(numTasksMaple)
	} else {
		rangePartitionMaple(numTasksMaple)
	}

	for taskID, fileName := range eachTaskFiles {
		fmt.Printf("---> TaskID: %d; NumFiles: %d\nFiles: %s\n", taskID, len(fileName), fileName)
	}

	// 3. arrange tasks to workers
	for i := 0; i < numTasksMaple; i++ {
		// first, add all tasks to tasksToAllocateJuice array
		tasksToAllocateMaple = append(tasksToAllocateMaple, i)
		completionMap[i] = false
	}

	// 4. initializing the server
	for {
		arrangeTasksMaple()
		MapleInfoPassing(executable)
		if checkForCompletionMaple() {
			fmt.Println("Task Completed")
			break
		} else {
			fmt.Printf("Remaining num of task: %d ", len(tasksToAllocateMaple))
			for memberID, curTask := range taskAssignMaple {
				fmt.Printf("(%d, %d) ", memberID, curTask)
			}
			fmt.Printf("\n")
			time.Sleep(100 * time.Millisecond)
		}
	}

	// 7. output maple files
	FinalizeOutputMaple(prefix)

	duration := time.Since(start)
	fmt.Printf("Maple job completed in %v. Intemediate files with prefix <%v> are in file in SDFS system\n", duration, prefix)

	jobLock.Lock()
	jobRunning = false
	jobLock.Unlock()
}

func sendBatch(fileList []string, senderType string, receiverType string, receiverID int) {
	distributeMap := make(map[int]map[int]string)
	local := make(map[int]string)
	counterMap := make(map[int]int)
	for id := range memberHost {
		counterMap[id] = 0
	}

	// assign senders evenly
	for i, fileName := range fileList {
		minNum := 1000000
		minNode := -1

		for key, replica := range replicateList[fileName] {
			if key != LASTUPDATE && replicateList[fileName][key] != "" {
				replicaNode, _ := strconv.Atoi(replica)
				if replicaNode == receiverID {
					// local copy possible
					local[i] = fileName
					minNode = -1
					break
				} else {
					if counterMap[replicaNode] < minNum {
						minNum = counterMap[replicaNode]
						minNode = replicaNode
					}
				}
			}
		}

		if minNode != -1 {
			// local copy not possible
			counterMap[minNode] += 1
			if _, ok := distributeMap[minNode]; !ok {
				newMap := make(map[int]string)
				newMap[i] = fileName
				distributeMap[minNode] = newMap
			} else {
				distributeMap[minNode][i] = fileName
			}
		}
	}

	senderMap := make(map[string]string)
	senderMap[SENDERDIR] = ""
	senderMap[RECEIVERDIR] = localTempFilePrefix
	senderMap[SENDERTYPE] = senderType
	senderMap[RECEIVERTYPE] = receiverType
	senderMap[REPLICAONE] = strconv.Itoa(receiverID)

	// send other file requests to node
	for nodeID, fileMap := range distributeMap {
		time.Sleep(5 * time.Millisecond)
		if nodeID == selfID {
			for _, fileName := range fileMap {
				WriteToNode(fileName, senderType, localTempFilePrefix + fileName, receiverType, receiverID)
			}
		} else {
			fileName, _ := json.Marshal(fileMap)
			senderMap[SENDERNAME] = string(fileName)
			msg, _ := json.Marshal(senderMap)
			msgSent := MakeMessage(WRITEBATCH, string(msg), strconv.Itoa(selfID))
			sendTCPRequest(nodeID, msgSent)
		}
	}

	// send local copy request to node
	if receiverID == selfID {
		// send file to master node (self node: local copying)
		for _, fileName := range local {
			WriteToNode(fileName, senderType, localTempFilePrefix + fileName, receiverType, receiverID)
		}
	} else {
		fileName, _ := json.Marshal(local)
		senderMap[SENDERNAME] = string(fileName)
		msg, _ := json.Marshal(senderMap)
		msgSent := MakeMessage(WRITEBATCH, string(msg), strconv.Itoa(selfID))
		sendTCPRequest(receiverID, msgSent)
	}
}

// func MapleInfoPassing(exe string)
// --------------------------------------------------------------------------------------------------------------
// @description: Helper function that send necessary information from master to non-master workers
// @input: none
// @return: none
func MapleInfoPassing(exe string) {
	var msgSent = map[int]string {
		EXECUTABLE: exe,
		FILELIST: 	"",
		FILESIZE:	"",
	}

	for memberID, taskID := range updateList {
		fmt.Printf("Allocating task %v to node %v\n", taskID, memberID)
		sendBatch(eachTaskFiles[taskID], SDFSNAME, LOCALNAME, memberID)
		if memberID == masterID {
			FileQueueMaple = eachTaskFiles[taskID]
			FileQueueSizeMaple = eachTaskFileSize[taskID]
			go MapleExeMaster(LOCALFILEPATH + exe)
		} else {
			fileList, _ := json.Marshal(eachTaskFiles[taskID])
			fileSize, _ := json.Marshal(eachTaskFileSize[taskID])
			msgSent[FILELIST] = string(fileList)
			msgSent[FILESIZE] = string(fileSize)
			content, _ := json.Marshal(msgSent)
			msg := MakeMessage(MAPLE, string(content), strconv.Itoa(selfID))
			sendTCPRequest(memberID, msg)
		}
		updateLock.Lock()
		delete(updateList, memberID)
		updateLock.Unlock()
	}
}

func checkFile(fileName string, fileSize int64) {
	for {
		for i := 0; i < 5; i++ {
			// check if file exists
			if Exist(LOCALFILEPATH + localTempFilePrefix + fileName) {
				// check if file size match
				file, _ := os.Stat(LOCALFILEPATH + localTempFilePrefix + fileName)
				if file.Size() == fileSize {
					return
				}
				// fmt.Printf("File %v has inconsistent size: %v != %v\n", fileName, file.Size(), fileSize)
			}
			// fmt.Printf("File %v does not exist!\n", fileName)
			time.Sleep(CHECKTIME)
		}

		// file does not exist, request it
		fmt.Printf("File %v does not exist. Getting copies...\n", LOCALFILEPATH+localTempFilePrefix+fileName)
		handleGet(localTempFilePrefix+fileName, fileName, false)
	}
}

func jobError(errorType string) {
	if !isMaster {
		// non-master node send error message to master node
		msgSent := MakeMessage(errorType, "", strconv.Itoa(selfID))
		sendRequest(masterID, msgSent)
	} else {
		// master node reschedule current task
		if errorType == MAPLEERROR {
			tasksToAllocateMaple = append(tasksToAllocateMaple, taskAssignMaple[selfID])
			taskAssignMaple[selfID] = -1
		} else {
			tasksToAllocateJuice = append(tasksToAllocateJuice, taskAssignJuice[selfID])
			taskAssignJuice[selfID] = -1
		}
	}
}

func localCombiner(result string) string {
	combiner := make(map[string]int)
	lineArr := strings.Split(result, "\n")
	for _, line := range lineArr {
		key := strings.Split(line, ",")[0]
		if _, ok := combiner[key]; !ok {
			combiner[key] = 1
		} else {
			combiner[key]++
		}
	}

	combined := ""
	for key, count := range combiner {
		combined += key + "," + strconv.Itoa(count) + "\n"
	}
	return combined
}

// func MapleExe(exe string, fileList []string, fileSize []int64) {
// --------------------------------------------------------------------------------------------------------------
// @description: 1. This function runs on every non-master node
//               2. It will execute the executable for every single file in its task and send the result back to master
//               3. After everything is done, it will send JUICECOM message to indicate the completion of the task
// @input: exe (string): the filename of the executable
//         fileList([]string): the list contains every file generated by maple for this task
// @return: none
func MapleExe(exe string, fileList []string, fileSize []int64) {
	// check executable
	if !Exist(exe) {
		jobError(MAPLEERROR)
		fmt.Printf("Executable %v does not exist on local machine.\n", exe)
		return
	}

	res := strings.Builder{}

	// run the executable for every single file
	for i, file := range fileList {
		checkFile(file, fileSize[i])
		fmt.Println("MAPLE Now dealing with " + file)
		execSingleFile := exec.Command(exe, LOCALFILEPATH + localTempFilePrefix + file)
		execOutput, err := execSingleFile.CombinedOutput()
		if err != nil {
			jobError(MAPLEERROR)
			fmt.Printf("maple execution error")
			fmt.Print(err.Error() + "\n")
			return
		}
		//combined := localCombiner(string(execOutput))
		//res.Write([]byte(combined))
		res.Write(execOutput)
		_ = os.Remove(LOCALFILEPATH + localTempFilePrefix + file)
	}
	fmt.Println("Task completed!")

	msgSent := MakeMessage(MAPLECOM, res.String(), strconv.Itoa(selfID))
	sendTCPRequest(masterID, msgSent)
}

// func MapleExeMaster(exe string)
// --------------------------------------------------------------------------------------------------------------
// @description: 1. This function runs on master node
//               2. It will execute the executable for every single file in its task and append the output to result channel
//               3. After everything is done, clear its entry in taskAssignJuice map to -1(no task assigned)
// @input: exe (string): the filename of the executable
// @return: none
func MapleExeMaster(exe string) {
	time.Sleep(5 * time.Millisecond)

	// check executable
	if !Exist(exe) {
		jobError(MAPLEERROR)
		fmt.Printf("Executable %v does not exist on local machine.\n", exe)
		return
	}

	tmpRes := strings.Builder{}
	for i, file := range FileQueueMaple {
		checkFile(file, FileQueueSizeMaple[i])
		fmt.Println("MAPLE Now dealing with " + file)
		execSingleFile := exec.Command(exe, LOCALFILEPATH + localTempFilePrefix + file)
		execOutput, err := execSingleFile.CombinedOutput()
		if err != nil {
			jobError(MAPLEERROR)
			fmt.Printf("maple execution error")
			return
		}
		//combined := localCombiner(string(execOutput))
		//tmpRes.Write([]byte(combined))
		tmpRes.Write(execOutput)
		_ = os.Remove(LOCALFILEPATH + localTempFilePrefix + file)
	}
	fmt.Println("Task completed!")

	// indicating the worker is now free for new task
	resultLockMaple.Lock()
	result.WriteString(tmpRes.String())
	completionMap[taskAssignMaple[masterID]] = true
	resultLockMaple.Unlock()

	taskAssignMaple[masterID] = -1
}

// func FinalizeOutputMaple(destFilePrefix string)
// --------------------------------------------------------------------------------------------------------------
// @description: helper function that will append new received content in the channel to the destFile
// @input: none
// @return: none
func FinalizeOutputMaple(destFilePrefix string) {
	resultStr := result.String()
	fmt.Println("Sorting...")
	resultStrArr := strings.Split(resultStr, "\n")
	sort.Strings(resultStrArr)
	i := 0
	curKey := strings.Split(resultStrArr[0], ",")[0]
	nextKey := ""
	breakflag := false
	fmt.Println("Distributing keys...")
	for {
		if curKey == "" && i + 1 < len(resultStrArr) {
			nextKey = strings.Split(resultStrArr[i + 1], ",")[0]
			i++
			curKey = nextKey
			continue
		}

		if !breakflag {
			filePath, _ := filepath.Abs(LOCALFILEPATH + destFilePrefix + curKey)
			fd, _ := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			for {
				if i + 1 < len(resultStrArr) {
					nextKey = strings.Split(resultStrArr[i + 1], ",")[0]
				} else {
					nextKey = ""
					breakflag = true
				}
				_, _ = fd.WriteString(resultStrArr[i] + "\n")
				i++
				if curKey != nextKey {
					break
				}
			}
			_ = fd.Close()
			handlePut(destFilePrefix + curKey, destFilePrefix + curKey, true)
			_ = os.Remove(filePath)
			curKey = nextKey
		} else {
			break
		}
	}
}

// func hashPartitionMaple()
// --------------------------------------------------------------------------------------------------------------
// @description: This function allocate files output by maple to different juice tasks by using hash partitioning
// @input: none
// @return: none
func hashPartitionMaple(numTasksMaple int) {
	for fileKey, fileName := range fileMapMaple {
		eachTaskFiles[fileKey % numTasksMaple] = append(eachTaskFiles[fileKey % numTasksMaple], fileName)
		file, _ := os.Stat(SDFSFILEPATH + fileName)
		eachTaskFileSize[fileKey % numTasksMaple] = append(eachTaskFileSize[fileKey % numTasksMaple], file.Size())
	}
}

// func rangePartitionMaple()
// --------------------------------------------------------------------------------------------------------------
// @description: This function allocate files output by maple to different juice tasks by using range partitioning
// @input: none
// @return: none
func rangePartitionMaple(numTasksMaple int) {
	fileNames := make([]string, 0, len(fileMapMaple))
	for _, fileName := range fileMapMaple {
		fileNames = append(fileNames, fileName)
	}
	sort.Strings(fileNames)
	numFilesEachTask := len(fileMapMaple) / numTasksMaple + 1
	for i := 0; i < numTasksMaple; i++ {
		for j := 0; j < numFilesEachTask; j++ {
			if i * numFilesEachTask + j >= len(fileNames) {
				break
			} else {
				fileName := fileNames[i * numFilesEachTask + j]
				file, _ := os.Stat(SDFSFILEPATH + fileName)
				eachTaskFiles[i] = append(eachTaskFiles[i], fileName)
				eachTaskFileSize[i] = append(eachTaskFileSize[i], file.Size())
			}
		}
	}
}

// func arrangeTasksMaple()
// --------------------------------------------------------------------------------------------------------------
// @description: This function will run periodically to arrange tasks
//               1. If some node fails (not juice master), delete it from task assignment and reallocate the task
//               2. If some node joins allocate it with a new job if possible
//               3. If some node finishes its job, allocate it with a new job if possible
// @input: none
// @return: none
func arrangeTasksMaple() {
	// Case1: If some node fails, delete it from task assignment and reallocate the task
	var toDelete []int
	for memberID := range taskAssignMaple {
		_, ok := memberHost[memberID]
		if !ok && memberID != selfID{
			toDelete = append(toDelete, memberID)
		}
	}
	for _, memberID := range toDelete {
		tasksToAllocateMaple = append(tasksToAllocateMaple, taskAssignMaple[memberID])
		delete(taskAssignMaple, memberID)
	}

	// Case2: If some node joins allocate it with a new job if possible
	var toInsert []int
	for memberID := range memberHost {
		_, ok := taskAssignMaple[memberID]
		if !ok {
			toInsert = append(toInsert, memberID)
		}
	}

	_, ok := taskAssignMaple[selfID]
	if !ok {
		toInsert = append(toInsert, selfID)
	}

	for _, memberID := range toInsert {
		if len(tasksToAllocateMaple) != 0 {
			taskAssignMaple[memberID], tasksToAllocateMaple = tasksToAllocateMaple[0], tasksToAllocateMaple[1:]
			updateLock.Lock()
			updateList[memberID] = taskAssignMaple[memberID]
			updateLock.Unlock()
		} else {
			taskAssignMaple[memberID] = -1
		}
	}

	// Case3: If some node finishes its job, allocate it with a new job if possible
	for memberID, currentTask := range taskAssignMaple {
		if len(tasksToAllocateMaple) != 0 && currentTask == -1 {
			taskAssignMaple[memberID], tasksToAllocateMaple = tasksToAllocateMaple[0], tasksToAllocateMaple[1:]
			updateLock.Lock()
			updateList[memberID] = taskAssignMaple[memberID]
			updateLock.Unlock()
		}
	}
}

// func checkForCompletionMaple() bool
// --------------------------------------------------------------------------------------------------------------
// @description: A helper function checks whether the juice job completed.
// @input: none
// @return: true if all have been completed, false otherwise
func checkForCompletionMaple() bool {
	if len(tasksToAllocateMaple) != 0 {
		return false
	}
	for _, curTask := range taskAssignMaple {
		if curTask != -1 {
			return false
		}
	}
	for _, completeTask := range completionMap {
		if !completeTask {
			return false
		}
	}
	return true
}
