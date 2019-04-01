package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/peterwilliams97/pdf-search/utils"
)

const usage = `Usage: go run geoff_index_search.go [OPTIONS] Adobe PDF
Performs a full text search for "Adobe PDF" in Bleve index "store.position" that was created with
simple_index.go`

func main() {
	var filePath string
	var inMemory bool
	maxResults := 10
	// utils.Debug = true // -d command line option doesn't work for this command line program !@#$

	flag.StringVar(&filePath, "p", filePath, "PDF file to index.")
	flag.BoolVar(&inMemory, "m", inMemory, "In-memory store.")
	flag.IntVar(&maxResults, "n", maxResults, "Max number of results to return.")
	utils.MakeUsage(usage)
	utils.SetLogging()
	flag.Parse()
	if utils.ShowHelp {
		flag.Usage()
		os.Exit(0)
	}

	if len(flag.Args()) < 1 {
		flag.Usage()
		os.Exit(1)
	}

	pathList := []string{filePath}
	term := strings.Join(flag.Args(), " ")
	var results string

	// common.Log.Debug("doPersist=%t", doPersist)

	if !inMemory {
		persistDir := "yyy"
		_, index, err := utils.IndexPdfs(pathList, persistDir, true, false)
		if err != nil {
			panic(err)
		}
		index.Close()
		results, err = utils.SearchPdfIndex(persistDir, term, maxResults)
		if err != nil {
			panic(err)
		}
	} else {
		lState, index, err := utils.IndexPdfs(pathList, "", true, false)
		if err != nil {
			panic(err)
		}
		results, err = utils.SearchIndex(lState, index, term, maxResults)
		if err != nil {
			panic(err)
		}
	}

	fmt.Println("=================+++=====================")
	fmt.Printf("%s\n", results)
}
