service:
	go clean
	go build -o service service.go tcpserver.go initialization.go election.go msghandler.go sdfsroutines.go filetransfer.go \
	    memshiproutines.go sdfshelper.go memshiphelpers.go genhelpers.go query.go macros.go maple.go juice.go
clean:
	go clean