package main

import (
	"fmt"
	"github.com/bxcodec/dbresolver/v2"
)

func main() {
	fmt.Printf("Version: %s\n", dbresolver.Version)
	fmt.Printf("CommitSha: %s\n", dbresolver.Commit)
}
