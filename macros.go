package main

import (
	"os"
	"sync"
	"time"
)

///////////////////////////////////////////////////
/////////                  ////////////////////////
/////////  Magic Numbers   ////////////////////////
/////////                  ////////////////////////
///////////////////////////////////////////////////

const(
	// URL for contact machine
	contactAddress 		= "fa19-cs425-g03-01.cs.illinois.edu"
	PORT				= "7000"
	LOCALPORT			= "8000"
	SDFSPORT			= "9000"
	TCPPORT				= "10001"

	// Markers for message type
	// membership messages
	HEARTBEAT string 	= "0"
	FAIL string 		= "1"
	LEAVE string 		= "2"
	JOINACK string		= "3"
	JOINREQ string		= "4"
	UPDATELIST string	= "5"
	// file system messages
	WRITEREQ string		= "6"
	WRITE string		= "7"
	WRITEBATCH string 	= "8"
	READREQ string		= "9"
	ERRORREAD string	= "10"
	DELETEREQ string 	= "11"
	DELETE string 		= "12"
	OVERWRITE string	= "13"
	REPLICALIST string 	= "14"
	// master election messages
	ELECTION string 	= "15"
	OK string 			= "16"
	COORDINATOR string 	= "17"
	NEWELECTION string	= "18"
	// map reduce protocols
	MAPLE string 		= "19"
	MAPLEREQ string 	= "20"
	MAPLECOM string 	= "21"
	MAPLEERROR string	= "22"
	JUICE string 		= "23"
	JUICEREQ string 	= "24"
	JUICECOM string 	= "25"
	JUICEERROR string 	= "26"

	// Keys in message struct
	UNIQUEID int 		= 0
	TIMESTAMP int		= 1
	SENDER int			= 2
	MSGTYPE int			= 3
	CONTENT int			= 4
	SELFKEY int			= -1

	// Keys in master's replica list
	LOCALNAME string 	= "local"
	SDFSNAME string		= "sdfs"
	REPLICAONE string	= "1"
	REPLICATWO string	= "2"
	REPLICATHREE string = "3"
	REPLICAFOUR string	= "4"
	LASTUPDATE string	= "6"

	// Keys in receiverMap
	SENDERTYPE string	= "7"
	RECEIVERTYPE string = "8"
	SENDERNAME string	= "9"
	RECEIVERNAME string = "10"
	RECEIVERID string 	= "11"
	SENDERDIR string 	= "12"
	RECEIVERDIR string 	= "13"
	LOCALEXIST string   = "14"

	// Keys in replica list map
	SDFSLIST string 	= "0"
	SDFSCOUNT string 	= "1"

	// global boolean value
	TRUE string			= "true"
	FALSE string 		= "false"

	// Size of global arrays
	SIZERECENTMSG int	= 60

	// Log name
	logFile string 		= "service.log"
	criticalFile string = "critical.log"
	queryFile string 	= "query.log"

	// file distribution
	SDFSFILEPATH string = "sdfs/"
	LOCALFILEPATH string= "local/"

	// time constants
	FAILTIME  			= 2 * time.Second
	LOGTIME 			= 10 * time.Second
	UPDATETIME 			= 2 * time.Second
	OKWAITTIME			= 2 * time.Second
	COWAITTIME			= 5 * time.Second
	CHECKTIME 			= 100 * time.Millisecond
)

///////////////////////////////////////////////////
/////////                     /////////////////////
/////////  Global Variables   /////////////////////
/////////                     /////////////////////
///////////////////////////////////////////////////

// The array that keeps recent messages
var recentMessages = make([]string, SIZERECENTMSG)

// Number of nodes that this service should heartbeat to or monitoring
var targetMonitorNum int

// Arrays to monitor peer's latest heartbeat
var monitorList = make(map[int]string)
var lastUpdate = make(map[int]time.Time)
var lastUpdateLocal = make(map[int]time.Time)

// Heartbeat target list
var targetList = make(map[int]int)
var targetAddr = make(map[int]string)

// replica list
var replicateList = make(map[string]map[string]string)
var replicateCounter = make(map[string]int)

// Arrays of all members
var memberHost = make(map[int]string)
var memberAddr = make(map[int]string)

var masterID int
var selfID int
var maxID int
var isContact bool
var isMaster bool
var isElecting bool

// channel that receives add or deleted keys
var localHost string
var localAddr string
var lastLogTime time.Time

// master election channels
var okWaitChan = make(chan bool)
var coWaitChan = make(chan bool)
var okChanNum int
var coChanNum int

// Semaphores
var memberLock sync.RWMutex
var fileLock sync.RWMutex
var electionLock sync.Mutex
var chanLock sync.Mutex

var fLog *os.File

// Initialize helper map
var helperMap = map[string]string {
	HEARTBEAT : "HEARTBEAT",
	FAIL : "FAIL",
	LEAVE : "LEAVE",
	JOINACK : "JOINACK",
	JOINREQ : "JOINREQ",
	UPDATELIST : "UPDATELIST",
	WRITEREQ : "WRITEREQ",
	WRITE : "WRITE",
	READREQ : "READREQ",
	ERRORREAD : "ERRREAD",
	DELETEREQ : "DELETEREQ",
	DELETE : "DELETE",
	OVERWRITE : "OVERWRITE",
	REPLICALIST : "REPLICALIST",
}

var replicaMap = map[string]string{
	REPLICAONE : REPLICAONE,
	REPLICATWO : REPLICATWO,
	REPLICATHREE : REPLICATHREE,
	REPLICAFOUR : REPLICAFOUR,
}