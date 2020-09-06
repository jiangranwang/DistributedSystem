package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type JobDescriptor struct {
	executable string
	numTasksJuice int
	numTasksMaple int
	prefix string
	destDir string
	srcDir string
	deletion int
	partition string
}

// ##################################
// ##  global variables for juice  ##
// ##################################

const(
	EXECUTABLE 	= 1
	FILELIST	= 2
	FILESIZE	= 3
	DEL 		= 4
)

var jobQueueJuice = make([]JobDescriptor, 0)

var fileMapJuice = make(map[int]string)		// the array stores all file name, only used by master
var eachTaskFiles = make(map[int][]string)	// Files that each task needs to dealing with
var eachTaskFileSize = make(map[int][]int64) // File size that used to check file integrity
var updateList = make(map[int]int)			// the key is ID of worker(node), value is task, indicating which node need to be sent with new messsage
var tasksToAllocateJuice = make([]int, 0)	// map that maps each task to a member id, only used by master
var taskAssignJuice = make(map[int]int)		// task assignment
var completionMap = make(map[int]bool)

var FileQueueJuice = make([]string, 0)		// the task for current node
var FileQueueSizeJuice = make([]int64, 0)
var result = strings.Builder{}
var updateLock sync.Mutex					// lock for modifying updateList
var resultLockJuice sync.Mutex

var localTempFilePrefix = "localExeInput_"


// ###########################
// ##  functions for juice  ##
// ###########################

// func juiceDispatch(executable string, numJuicesStr string, prefix string, destDir string, del string, partition string)
// --------------------------------------------------------------------------------------------------------------
// @description: receive a new submitted job, perform sanity check first
//               If the node is master, append the new job to the queue, else send to master
// @input: none
// @return: none
func juiceDispatch(executable string, numJuicesStr string, prefix string, destDir string, del string, partition string) {

	// 1. sanity check
	var deletion int
	numTasksJuice, err := strconv.Atoi(numJuicesStr)
	if err != nil {
		fmt.Printf("*JuiceERROR!! The <num_juices> parameter is not a integer! Abort\n")
		return
	}
	if !Exist(LOCALFILEPATH + executable) {
		fmt.Printf("*JuiceERROR!! The executable %s does not exist in the SDFS system! Abort\n", executable)
		return
	}
	deletion, err = strconv.Atoi(del)
	if err != nil || (deletion != 1 && deletion != 0) {
		fmt.Printf("*JuiceERROR!! The <delete_input> can be either 0 or 1! Abort\n")
		return
	}
	if partition != "hash" && partition != "range" {
		fmt.Printf("*JuiceERROR!! The <partition> can be either 'hash' or 'range'! Abort\n")
		return
	}

	// 2. if it is master, add the job to current list, otherwise, send the info to master
	if isMaster {
		var newJuice JobDescriptor
		newJuice.executable = executable
		newJuice.destDir = destDir
		newJuice.prefix = prefix
		newJuice.partition = partition
		newJuice.numTasksJuice = numTasksJuice
		newJuice.deletion = deletion

		jobLock.Lock()
		jobQueueJuice = append(jobQueueJuice, newJuice)
		jobLock.Unlock()
	} else {
		content := map[string]string{
			"exe" : executable,
			"num" : numJuicesStr,
			"pre" : prefix,
			"des" : destDir,
			"del" : del,
			"par" : partition,
		}
		contentStr, _ := json.Marshal(content)
		msgSent := MakeMessage(JUICEREQ, string(contentStr), strconv.Itoa(selfID))
		sendRequest(masterID, msgSent)
	}
}

// func juiceJobSchedule()
// --------------------------------------------------------------------------------------------------------------
// @description: A thread will keep running at backend to schedule juice job
// @input: none
// @return: none
func juiceJobSchedule() {
	for {
		if isMaster {
			if !jobRunning && len(jobQueueJuice) != 0 {
				jobLock.Lock()
				jobRunning = true
				jobLock.Unlock()
				curJob := jobQueueJuice[0]
				jobQueueJuice = jobQueueJuice[1:]
				go JuiceMaster(curJob)
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// func JuiceMaster(descriptor JobDescriptor)
// --------------------------------------------------------------------------------------------------------------
// @description: Initialization procedure for the master of juice
// @input: none
// @return: none
func JuiceMaster(descriptor JobDescriptor) {
	fileMapJuice = make(map[int]string)
	eachTaskFiles = make(map[int][]string)
	eachTaskFileSize = make(map[int][]int64)
	updateList = make(map[int]int)			// the key is ID of worker(node), value is task, indicating which node need to be sent with new messsage
	tasksToAllocateJuice = make([]int, 0)	// map that maps each task to a member id, only used by master
	taskAssignJuice = make(map[int]int)		// task assignment
	result = strings.Builder{}
	completionMap = make(map[int]bool)

	delete_ := descriptor.deletion
	numTasksJuice := descriptor.numTasksJuice
	executable := descriptor.executable
	prefix := descriptor.prefix
	destDir := descriptor.destDir
	partition := descriptor.partition

	// 0. set the master flag and sanity check

	fmt.Printf("-----Juice is running!-----\n")
	fmt.Printf("*Executable:       %s\n", executable)
	fmt.Printf("*SDFS File Prefix: %s\n", prefix)
	fmt.Printf("*Number of tasks:  %d\n", numTasksJuice)

	// 1. find all the files with the prefix given and append them to taskMapJuice
	i := 0
	for fileName := range replicateList {
		if strings.HasPrefix(fileName, prefix) {
			fileMapJuice[i] = fileName
			i += 1
		}
	}

	if len(fileMapJuice) == 0 {
		// if 0 file found, aborting
		fmt.Printf("0 Maple files found, abort Juice ...\n")
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
		hashPartitionJuice(numTasksJuice)
	} else {
		rangePartitionJuice(numTasksJuice)
	}

	for taskID, fileName := range eachTaskFiles {
		fmt.Printf("---> TaskID: %d; NumFiles: %d\nFiles: %s\n", taskID, len(fileName), fileName)
	}

	// 3. arrange tasks to workers
	for i := 0; i < numTasksJuice; i++ {
		// first, add all tasks to tasksToAllocateJuice array
		tasksToAllocateJuice = append(tasksToAllocateJuice, i)
		completionMap[i] = false
	}

	// 4. initializing the server
	for {
		arrangeTasksJuice()
		JuiceInfoPassing(executable, delete_)
		if checkForCompletionJuice() {
			fmt.Println("Task Completed")
			break
		} else {
			fmt.Printf("Remaining num of task: %d ", len(tasksToAllocateJuice))
			for memberID, curTask := range taskAssignJuice {
				fmt.Printf("(%d, %d) ", memberID, curTask)
			}
			fmt.Printf("\n")
			time.Sleep(100 * time.Millisecond)
		}
	}
	FinalizeOutputJuice(destDir)

	duration := time.Since(start)
	fmt.Printf("Juice job completed in %v. Final result is in <%s> file in SDFS system\n", duration, destDir)

	jobLock.Lock()
	jobRunning = false
	jobLock.Unlock()
}

// func JuiceInfoPassing(exe string, pre string)
// --------------------------------------------------------------------------------------------------------------
// @description: Helper function that send necessary information from master to non-master workers
// @input: none
// @return: none
func JuiceInfoPassing(exe string, deletion int) {
	var msgSent = map[int]string {
		EXECUTABLE: exe,
		FILELIST: 	"",
		FILESIZE:	"",
		DEL:		strconv.Itoa(deletion),
	}

	for memberID, taskID := range updateList {
		sendBatch(eachTaskFiles[taskID], SDFSNAME, LOCALNAME, memberID)
		if memberID == masterID {
			FileQueueJuice = eachTaskFiles[taskID]
			FileQueueSizeJuice = eachTaskFileSize[taskID]
			go JuiceExeMaster(LOCALFILEPATH + exe, deletion)
		} else {
			fileList, _ := json.Marshal(eachTaskFiles[taskID])
			fileSize, _ := json.Marshal(eachTaskFileSize[taskID])
			msgSent[FILELIST] = string(fileList)
			msgSent[FILESIZE] = string(fileSize)
			content, _ := json.Marshal(msgSent)
			msg := MakeMessage(JUICE, string(content), strconv.Itoa(selfID))
			sendTCPRequest(memberID, msg)
		}
		updateLock.Lock()
		delete(updateList, memberID)
		updateLock.Unlock()
	}
}

// func JuiceExe(exe string, fileList []string)
// --------------------------------------------------------------------------------------------------------------
// @description: 1. This function runs on every non-master node
//               2. It will execute the executable for every single file in its task and send the result back to master
//               3. After everything is done, it will send JUICECOM message to indicate the completion of the task
// @input: exe (string): the filename of the executable
//         fileList([]string): the list contains every file generated by maple for this task
// @return: none
func JuiceExe(exe string, fileList []string, fileSize []int64, deletion int) {
	// check executable
	if !Exist(exe) {
		jobError(JUICEERROR)
		fmt.Printf("Executable %v does not exist on local machine.\n", exe)
		return
	}

	res := strings.Builder{}

	// run the executable for every single file
	for i, file := range fileList {
		checkFile(file, fileSize[i])
		fmt.Println("JUICE Now dealing with " + file)
		execSingleFile := exec.Command(exe, LOCALFILEPATH + localTempFilePrefix + file)
		execOutput, err := execSingleFile.CombinedOutput()
		if err != nil {
			jobError(JUICEERROR)
			fmt.Printf("juice execution error")
			fmt.Print(err.Error() + "\n")
			return
		}
		res.Write(execOutput)
		_ = os.Remove(LOCALFILEPATH + localTempFilePrefix + file)
		if deletion == 1 {
			go handleDelete(file)
		}
	}
	fmt.Println("Task completed!")

	// fmt.Println(res)
	msgSent := MakeMessage(JUICECOM, res.String(), strconv.Itoa(selfID))
	sendTCPRequest(masterID, msgSent)
}

// func JuiceExeMaster(exe string)
// --------------------------------------------------------------------------------------------------------------
// @description: 1. This function runs on master node
//               2. It will execute the executable for every single file in its task and append the output to result channel
//               3. After everything is done, clear its entry in taskAssignJuice map to -1(no task assigned)
// @input: exe (string): the filename of the executable
// @return: none
func JuiceExeMaster(exe string, deletion int) {
	time.Sleep(5 * time.Millisecond)

	// check executable
	if !Exist(exe) {
		jobError(JUICEERROR)
		fmt.Printf("Executable %v does not exist on local machine.\n", exe)
		return
	}

	tmpRes := strings.Builder{}
	for i, file := range FileQueueJuice {
		checkFile(file, FileQueueSizeJuice[i])
		fmt.Println("JUICE Now dealing with " + file)
		execSingleFile := exec.Command(exe, LOCALFILEPATH + localTempFilePrefix + file)
		execOutput, err := execSingleFile.CombinedOutput()
		if err != nil {
			jobError(JUICEERROR)
			fmt.Printf("juice execution error")
			return
		}
		tmpRes.Write(execOutput)
		_ = os.Remove(LOCALFILEPATH + localTempFilePrefix + file)

		if deletion == 1 {
			go handleDelete(file)
		}
	}
	fmt.Println("Task completed!")

	// indicating the worker is now free for new task
	resultLockJuice.Lock()
	result.WriteString(tmpRes.String())
	completionMap[taskAssignJuice[masterID]] = true
	resultLockJuice.Unlock()

	taskAssignJuice[masterID] = -1
}


// func FinalizeOutputJuice(destFile string)
// --------------------------------------------------------------------------------------------------------------
// @description: helper function that will append new received content in the channel to the destFile
// @input: none
// @return: none
func FinalizeOutputJuice(destFile string) {
	resultStrArr := strings.Split(result.String(), "\n")
	fmt.Println("Sorting...")
	sort.Strings(resultStrArr)
	resultStr := ""
	for _, str := range resultStrArr {
		if len(str) == 0 {
			continue
		}
		resultStr += str + "\n"
	}
	filePath, _ := filepath.Abs(LOCALFILEPATH + destFile)
	_ = os.Remove(filePath)
	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
	}
	// fmt.Println(resultStr)
	_, err = f.Write([]byte(resultStr))
	if err != nil {
		log.Println(err)
	}
	_ = f.Close()

	handlePut(destFile, destFile, true)
}

// func hashPartitionJuice()
// --------------------------------------------------------------------------------------------------------------
// @description: This function allocate files output by maple to different juice tasks by using hash partitioning
// @input: none
// @return: none
func hashPartitionJuice(numTasksJuice int) {
	for fileKey, fileName := range fileMapJuice {
		eachTaskFiles[fileKey % numTasksJuice] = append(eachTaskFiles[fileKey % numTasksJuice], fileName)
		file, _ := os.Stat(SDFSFILEPATH + fileName)
		eachTaskFileSize[fileKey % numTasksJuice] = append(eachTaskFileSize[fileKey % numTasksJuice], file.Size())
	}
}

// func rangePartitionJuice()
// --------------------------------------------------------------------------------------------------------------
// @description: This function allocate files output by maple to different juice tasks by using range partitioning
// @input: none
// @return: none
func rangePartitionJuice(numTasksJuice int) {
	fileNames := make([]string, 0, len(fileMapJuice))
	for _, fileName := range fileMapJuice {
		fileNames = append(fileNames, fileName)
	}
	sort.Strings(fileNames)
	numFilesEachTask := len(fileMapJuice) / numTasksJuice + 1
	for i := 0; i < numTasksJuice; i++ {
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

// func arrangeTasksJuice()
// --------------------------------------------------------------------------------------------------------------
// @description: This function will run periodically to arrange tasks
//               1. If some node fails (not juice master), delete it from task assignment and reallocate the task
//               2. If some node joins allocate it with a new job if possible
//               3. If some node finishes its job, allocate it with a new job if possible
// @input: none
// @return: none
func arrangeTasksJuice() {
	// Case1: If some node fails, delete it from task assignment and reallocate the task
	var toDelete []int
	for memberID := range taskAssignJuice {
		_, ok := memberHost[memberID]
		if !ok && memberID != selfID{
			toDelete = append(toDelete, memberID)
		}
	}
	for _, memberID := range toDelete {
		if taskAssignJuice[memberID] != -1 {
			tasksToAllocateJuice = append(tasksToAllocateJuice, taskAssignJuice[memberID])
		}
		delete(taskAssignJuice, memberID)
	}

	// Case2: If some node joins allocate it with a new job if possible
	var toInsert []int
	for memberID := range memberHost {
		_, ok := taskAssignJuice[memberID]
		if !ok {
			toInsert = append(toInsert, memberID)
		}
	}

	_, ok := taskAssignJuice[selfID]
	if !ok {
		toInsert = append(toInsert, selfID)
	}

	for _, memberID := range toInsert {
		if len(tasksToAllocateJuice) != 0 {
			taskAssignJuice[memberID], tasksToAllocateJuice = tasksToAllocateJuice[0], tasksToAllocateJuice[1:]
			updateLock.Lock()
			updateList[memberID] = taskAssignJuice[memberID]
			updateLock.Unlock()
		} else {
			taskAssignJuice[memberID] = -1
		}
	}

	// Case3: If some node finishes its job, allocate it with a new job if possible
	for memberID, currentTask := range taskAssignJuice {
		if len(tasksToAllocateJuice) != 0 && currentTask == -1 {
			taskAssignJuice[memberID], tasksToAllocateJuice = tasksToAllocateJuice[0], tasksToAllocateJuice[1:]
			updateLock.Lock()
			updateList[memberID] = taskAssignJuice[memberID]
			updateLock.Unlock()
		}
	}
}

// func checkForCompletionJuice() bool
// --------------------------------------------------------------------------------------------------------------
// @description: A helper function checks whether the juice job completed.
// @input: none
// @return: true if all have been completed, false otherwise
func checkForCompletionJuice() bool {
	if len(tasksToAllocateJuice) != 0 {
		return false
	}
	for _, curTask := range taskAssignJuice {
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
