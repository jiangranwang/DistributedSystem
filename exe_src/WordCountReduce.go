package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Printf("*WebLinkReduceError!!! No source file\n")
		return
	}

	dat, err := ioutil.ReadFile(os.Args[1])
	if err != nil {
		fmt.Printf("Cannot read file %v! With error: %v\n", os.Args[1], err.Error())
		return
	}

	content := string(dat)
	lineArr := strings.Split(content, "\n")
	valueFrequency := len(lineArr) - 1
	word := strings.Split(lineArr[0], ",")[0]
	if len(word) == 0 {
		return
	}
	fmt.Printf("%s,%d\n", word, valueFrequency)
}