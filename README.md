# CS425MP4

* The last mp for CS 425 distributed systems in fall 2019
* Map-Juice

|Member1|Email|Member2|Email|
|:---:|:---:|:---:|:---:|
|Jiangran Wang|<a href="mailto:jw22@illinois.edu">jw22</a>|Jiayuan Li |<a href="mailto:jiayuan8@illinois.edu">jiayuan8</a>

## Structure of this project
```
425MP4
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
|   maple.go                // functions and variables for maple
|   juice.go                // functions and variables for juice
│   tcpserver.go            // a tcp server responsible for reliable communication
|
```

## How to run this program
* clone the repository
```
git clone https://gitlab.engr.illinois.edu/jw22/cs425mp4.git
cd cs425mp4
```
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
* Execute maple
```
maple <maple_exe> <num_maples> <sdfs_intermediate_filename_prefix> <sdfs_prefix>
 
```
* Execute juice
```
juice <juice_exe> <num_juices> <sdfs_intermediate_filename_prefix> <sdfs_dest_filename> delete_input={0,1}
 
```
* get the result
```
get <sdfs_dest_filename> <local_file_name>
 
```

# Design
## Maple & Juice Implementation
### I. Maple/Juice Dispatcher:
The Maple and Juice masters have been set to the same node as the master in SDFS. When a user submits a job from master node, the dispatcher will first run a sanity check and then handed the job to the job scheduler of Maple or Juice. When the job is submitted from non-master node, the dispatcher will send the job information to master and then the master can handle the job to job scheduler.
### II. Maple/Juice job scheduler:
The job scheduler of Maple/Juice only runs on the master node. When the Maple/Juice scheduler receives a new job, the scheduler will append the job information to a list that stores all the queueing jobs. There will be a flag indicating whether a task is currently running. When there is no task running (the flag will be set to false) and the size of queue is not zero, the scheduler will dequeue the first job and handle the job to task scheduler. 
### III. Maple/ Juice task scheduler:
The task scheduler of Maple/Juice only runs on the master node. When the task scheduler receives a new job, it will partition the job to multiple tasks each with a unique task ID according to the user’s demand. 
For Maple, the task scheduler will first find all files with the given <sdfs_src_directory> prefix in the SDFS. For Juice, the task scheduler will first find all files with the given <sdfs_interme-diate_filename_prefix> in the SDFS. 
Then the scheduler will partition those files to different tasks and store the information in a map that maps task ID to files the task needs to deal with. The scheduler will maintain a queue for all unassigned task ID. After that, the scheduler will dequeue unassigned tasks and assigns them to different workers (number of tasks can be larger than number of workers). Then, the master will send tasks (including filename partitioned to the task and the file name of the executable) to the corresponding worker. When the worker receives the task info, it will first fetch all the files it needed from SDFS and start working. 
As the job is being processed, whenever the master receives a MAPLECOM/JUICECOM message along with the result output of the task from a worker (which means the worker has completed the current assigned task), the master will delete the task ID and assign the worker a new task. If there are no more new tasks to assign and all the tasks assigned have been completed, the master will finalize the Maple/Juice output.
## Failure Toleration
This implementation can tolerate workers’ (not master) failure since the master will maintain the information about all tasks assignment and currently being processed. When the master receives failure message about a worker and it finds out the worker is processing a task and the master haven’t received its completion message, it will enqueue the task ID to the unassigned tasks queue. The task will be assigned to another available worker in the future.
