# Overview
This repository implements a simplified map-reduce system that is similar to Hadoop. 

Every server maintains a membership table that keeps track of the currently running servers in the system. Every server randomly picks three other targets in the system and constantly checks to see if any of the three servers is failed. Each server will send heartbeat pings to the servers that are monitoring it so that the current server indicates it is still alive. When a node fails, at least one of the servers will detect the failure within a prespecified time-bound and this failure message is propagated to all other servers in the system in O(log(N)) time where N is the number of servers in the system.

It has a simplified version of the Hadoop Distributed File System (HDFS) that can upload files, retrieve files, and delete files from the servers. When running on multiple servers, every file uploaded to any of the servers will be replicated three times on other servers to avoid lost files due to server failures. When retrieving files, it first checks if the current server has the file requested. If not, the current server will randomly pick another server that has the file requested and return the file to the user. On each server, there are two directories local/ and sdfs/, where local/ contains the files stored locally and sdfs/ contains the file stored on the distributed system.

We also implemented a simplified map-reduce system, and we name it to maple-juice system. The maple function can be initiated through the command-line interface, and this function performs the same task as the map function in the Hadoop system. Similarly, the juice function does the same thing as the reduce function in the Hadoop system. To perform a map-reduce task, the respective map and reduce functions have to be stored in the exe_src folder and the files that are used in the task should be stored in the distributed file system. Once a maple or a juice command is initiated on a server (call it master), the system will evenly dispatch the jobs to all the servers available in the system, and the result will be sent to back to the master at the end. 

Detailed implementation is described in the Design section below.

|Member1|Email|Member2|Email|
|:---:|:---:|:---:|:---:|
|Jiangran Wang|<a href="mailto:jw22@illinois.edu">jw22</a>|Jiayuan Li |<a href="mailto:jiayuan8@illinois.edu">jiayuan8</a>

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
## Membership Protocol Implementation
* Since the worst case is that 3 nodes will simultaneously fail, so we will let every node heartbeats to other 3 nodes. Therefore, we can guarantee that the failure of each node can always be detected by one of its heartbeat target since the 3 of its heartbeat targets will only have 2 failures at maximum.
* When joining the group, the contact node will allocate a unique ID to the node (Integer type). So, the IDs of all nodes in the group can be sorted as an increasing sequence. For each node i, the node will choose the 3 nodes that have the largest IDs among all the nodes that have IDs less than node i's ID as its heartbeat target. Similarly, it will choose 3 nodes that have the least IDs among all the nodes that have IDs greater than node i's ID as its heartbeat target. 

### The contact node
* Hard coded to VM1 (fa19-cs425-g03-01.cs.illinois.edu)
#### 1. Allocate IDs for newly joining node
* Since every new node must join the group through the contact node, the contact node will key a maxID variable (integer type start with 1). Whenever a new node joins, it will allocate the maxID as the ID for the new node and increment maxID by 1. Thus, we can guarantee each node in the group has a unique ID. The contact node will always have ID 0.
#### 2. Contact node rejoining
* Since every new node must join the group through the contact node, the contact node will have a list of all members (both online or failed but not yet reported to the contact node). The contact node will write its member list to a file (critical.log). Whenever the contact node failed and rejoins, it will try to connect the nodes in member list stored in the file and thusly guarantee the contact node can always be aware of each node in the group.

### Failure detection
* A separate thread runs the failure detector in the background, and it continuously updates the heartbeat receive time from each node it monitors. When a heartbeat from a specific node is not seen after 2.5s, the current node will mark this node as fail and send the failure message to the nodes it monitors. 

### Message Struct

| Unique ID | Time Stamp | Sender | Message Type | Message Content |
|:---:|:---:|:---:|:---:|:---:|
* Unique ID: The program will generate a unique ID for each message, which can be an indentifier for each unique message
* Time Stamp: The time when the sender sends the message
* Sender: The ID allocated by the contact node to each machine when the machine joins the group
* Message Type: Specified below
* Message Content

### Message Type

#### 0: heartbeat message
* The message that heartbeat to its heartbeat target
* The message content should contain nothing

#### 1: failure message
* The message that report some node fails. The failure node ID is specified in content part
* The message content shoud report which node fails

#### 2: leave message
* The message that report some node leaves. The failure node ID is specified in content part
* The message content shoud report which node leaves

#### 3: join ack message
* The message that send by the contact node and send back to new joining nodes, specifying their ID and give them group member list
* The message content should contain a map (in json string format) that maps the group member id to group member hostname and maps [0xFFFFFFFF] to its newly allocated ID by the contact node

#### 4: join request message
* The message that send by the new joining node and send to the contact node to request joining the group
* The message content should contain its hostname

#### 5: update list message
* after the new node joins, the contact node will send update list message to all node on its member list to update their member list
* The message content should contain a map (in json string format) that maps the new node's id to the new node's hostname










## File System Implementation
Every operation on SDFS file, including read, write, delete, and overwrite, 
must go through the master node. In this way, the master node always has the
most recent information about the file system. It also ensures that all other
nodes will get the most updated file information. 

### Replica List
Master node owns the most recent replica list at all the time, and it sends the list
to all other nodes periodically. The list is stored in global variable replicateList.
Each sdfs file entry in the replica list has the following structure:

| SDFS Name | Local Name | Replica 1-4 | Last Update |
|:---:|:---:|:---:|:---:|

* SDFS Name: name of the sdfs replica file
* Local Name: name of the local replica file
* Replica 1-4: stores the node id of where does the sdf copy exists. The
    fourth replica field is used only when the local replica fails
* Last Update: the time of the last write instruction to this sdfs file

### Master Election Protocol
We use the bully algorithm to do master election.

### Message Type

#### Write Request
* This message is sent when user executes the put instruction and want to
    write a sdfs file.
* Message content contains the local replica name and the sdfs replica name
* It should only be received by the master node

#### Write
* This message is only sent by the master node to let the receiver of this
    message to write a file to some other node
* Message content contains the file type and name of the sender and the receiver

#### Read Request
* This message is sent when user executes the get instruction and want to
    get a file from the system. It's also sent when any node detects file
    inconsistency and requests a file transfer
* Message content contains the sdfs replica name and the local name
* It should only be received by the master node

#### Error Read
* This message is only sent by the master node upon receiving a read request 
    to indicate that the sdfs file does not exist

#### Delete Request
* This message is sent when user executes the delete instruction and want to
    delete a sdfs file.
* Message content contains the sdfs file name we want to delete
* It should only be received by the master node

#### Delete
* This message is only sent by the master node upon receiving a delete request.

#### Overwrite
* This message is only sent by the master node when two write operation to
    the same file is within one minute
* User should indicate whether to overwrite the file

#### Replica List
* This message is only sent by the master node periodically and it sends the
    the most recent replica list to all other node
* The message contains the full replica list
* After receiving this message, every node should check whether the files that
    the current node has is consistent with the replica list

#### Election
* This message is sent to all nodes with id higher than the current node when the
    master node fails
* The message contains the failed node id

#### OK
* Node who receives the Election message respond OK message

#### Coordinator
* Node that has the highest id or time out waiting for ok message send this 
    message to indicate it's the new master node
    
#### New Election
* This message is sent by the master node when it leaves










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
