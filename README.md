# Overview
This repository implements a simplified map reduce system that is similar to Hadoop. 

Every server maintains a membership table that keep tracks of the current running servers in the system. Every server randomly picks three other targets in the system and constantly checks to see if any of the three servers is failed. Each server will send heartbeat pings to the servers that are monitoring it so that the current server indicates it is still alive. When a node fails, at least one of the servers will detect the failure within a prespecified time bound and this failure message is propagted to all other servers in the system in O(log(N)) time where N is the number of servers in the system.

It has a simplified version of Hadoop Distributed File System (HDFS) that can upload files, retrieve files, and delete files from the servers. When running on multiple servers, every file uploaded to any of the server will be replicated three times on other servers to avoid lost files due to server failures. When retrieving files, it first checks if the current server has the file requested. If not, the current server will randomly pick another server that has the file requested and return the file back to the user. On each server, there are two directories local/ and sdfs/, where local/ contains the files stored locally and sdfs/ contains the file stored on the distributed system.

We also implemented a simplified map-reduce system, and we name it to maple-juice system. The maple function can be initiated through the command line interface, and this function performs the same task as the map function in the Hadoop system. Similarly, the juice function does the same thing as the reduce functino in the Hadoop system. To perform a map reduce task, the respective map and reduce functions has to be stored in the exe_src folder and the files that are used in the task should be stored in the distributed file system. Once a maple or a juice command is initiated on a server (call it master), the system will evenly dispatch the jobs to all the servers avaible in the system, and the result will be send to back to the master at the end. Detailed implementation is described in the Design section below.

## Structure of this project
```
DistributedSystem
│
│   README.md               // specification
|   genhelpers.go           // general helper functions
|   initialization.go       // initialization functions
|   macros.go               // global constants and variables
|   membershiphelpers.go    // helper functions for membership protocols
|   membershiproutines.go   // failure detection and heartbeating routines
|   msghandler.go           // main function that accepts incoming messages
|   query.go                // functions that support query request
|   sdfshelper.go           // helper functions for distributed file system
|   sdfsroutines.go         // main routines for distributed file system
|   service.go              // main services
|   filetransfer.go         // functions that handle file transfer
|   election.go             // functions that handle election protocol
|   maple.go                // functions and variables for map tasks
|   juice.go                // functions and variables for reduce tasks
│   tcpserver.go            // a tcp server responsible for reliable communication
|
```

## How to run this program
* clone the repository
* make the executable for applications
```
cd exe_src
make all
# all the executables should be under local/ now
cd ..
```
* set socket upper bound 
```
ulimit -n 4096
```
* build the project and run
```
make clean && make service
./service

```
The data files should be in <src_dir/> under local/ directory
* Put all data files to simple distributed file system
```
putdir <src_dir/> <sdfs_prefix>

```
* Execute map
```
maple <maple_exe> <num_maples> <sdfs_intermediate_filename_prefix> <sdfs_prefix>
 
```
* Execute reduce
```
juice <juice_exe> <num_juices> <sdfs_intermediate_filename_prefix> <sdfs_dest_filename> delete_input={0,1}
 
```
* get the result
```
get <sdfs_dest_filename> <local_file_name>
 
```

# Design
## Map & Reduce Implementation
### I. Map/Reduce Dispatcher:
The Map and Reduce masters have been set to the same node as the master in SDFS. When a user submits a job from master node, the dispatcher will first run a sanity check and then handed the job to the job scheduler of Map or Reduce. When the job is submitted from non-master node, the dispatcher will send the job information to master and then the master can handle the job to job scheduler.
### II. Map/Reduce job scheduler:
The job scheduler of Map/Reduce only runs on the master node. When the Map/Reduce scheduler receives a new job, the scheduler will append the job information to a list that stores all the queueing jobs. There will be a flag indicating whether a task is currently running. When there is no task running (the flag will be set to false) and the size of queue is not zero, the scheduler will dequeue the first job and handle the job to task scheduler. 
### III. Map/Reduce task scheduler:
The task scheduler of Map/Reduce only runs on the master node. When the task scheduler receives a new job, it will partition the job to multiple tasks each with a unique task ID according to the user’s demand. 
For Map, the task scheduler will first find all files with the given <sdfs_src_directory> prefix in the SDFS. For Reduce, the task scheduler will first find all files with the given <sdfs_interme-diate_filename_prefix> in the SDFS. 
Then the scheduler will partition those files to different tasks and store the information in a map that maps task ID to files the task needs to deal with. The scheduler will maintain a queue for all unassigned task ID. After that, the scheduler will dequeue unassigned tasks and assigns them to different workers (number of tasks can be larger than number of workers). Then, the master will send tasks (including filename partitioned to the task and the file name of the executable) to the corresponding worker. When the worker receives the task info, it will first fetch all the files it needed from SDFS and start working. 
As the job is being processed, whenever the master receives a MapCOM/ReduceCOM message along with the result output of the task from a worker (which means the worker has completed the current assigned task), the master will delete the task ID and assign the worker a new task. If there are no more new tasks to assign and all the tasks assigned have been completed, the master will finalize the Map/Reduce output.
## Failure Toleration
This implementation can tolerate workers’ (not master) failure since the master will maintain the information about all tasks assignment and currently being processed. When the master receives failure message about a worker and it finds out the worker is processing a task and the master haven’t received its completion message, it will enqueue the task ID to the unassigned tasks queue. The task will be assigned to another available worker in the future.
