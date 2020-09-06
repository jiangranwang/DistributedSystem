package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Printf("*WebLinkMapperError!!! No source file\n")
		return
	}

	filename := os.Args[1]
	fd, err := os.Open(filename)
	if err != nil {
		fmt.Printf("Cannot read file %v! With error: %v\n", os.Args[1], err.Error())
		return
	}

	scanner := bufio.NewScanner(fd)
	for scanner.Scan() {
		words := strings.Split(scanner.Text(), " ")
		fmt.Printf("%s,%s\n", words[1], words[0])
	}
}
