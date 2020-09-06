package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Printf("*WebLinkReduceError!!! No source file\n")
		return
	}

	filename := os.Args[1]
	fd, err := os.Open(filename)
	if err != nil {
		fmt.Printf("Cannot read file %v! With error: %v\n", os.Args[1], err.Error())
		return
	}

	linkMap := make(map[string]bool)
	scanner := bufio.NewScanner(fd)
	for scanner.Scan() {
		text := scanner.Text()
		links := strings.Split(text, ",")
		if len(links) != 2 {
			continue
		}
		if _, ok := linkMap[links[1]]; ok {
			continue
		}
		linkMap[links[1]] = true
		fmt.Printf("%s\n", text)
	}
}
