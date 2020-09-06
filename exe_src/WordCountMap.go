package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Printf("*WordMapperError!!! No source file\n")
		return
	}

	filename := os.Args[1]
	dat, err := ioutil.ReadFile(filename)
	if err != nil {
		fmt.Printf("*WordMapperError!!! File %v cannot be read\n", filename)
	}

	words := strings.Split(string(dat), " ")
	for _, word := range words {
		if len(word) == 0 {
			continue
		}
		fmt.Printf("%s,1\n", word)
	}
}
