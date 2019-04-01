package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/peterwilliams97/pdf-search/utils"
)

/*
	go run geoff_index_search.go -p ~/testdata/adobe/PDF32000_2008.pdf  Type 1
	Duration=72.4 sec (memory=false)

	go run geoff_index_search.go -p ~/testdata/adobe/PDF32000_2008.pdf -m Type 1
	Duration=22.7 sec (memory=true) 1 files [/Users/pcadmin/testdata/adobe/PDF32000_2008.pdf]
*/

const usage = `Usage: go run geoff_index_search.go [OPTIONS] Adobe PDF
Performs a full text search for "Adobe PDF" in Bleve index "store.position" that was created with
simple_index.go`

func main() {
	var pathPattern string
	var inMemory = false
	maxResults := 10
	// utils.Debug = true // -d command line option doesn't work for this command line program !@#$

	flag.StringVar(&pathPattern, "p", pathPattern, "PDF file to index.")
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

	pathList, err := utils.PatternsToPaths([]string{pathPattern}, true)
	if err != nil {
		fmt.Fprintf(os.Stderr, "PatternsToPaths failed. args=%#q err=%v\n", flag.Args(), err)
		os.Exit(1)
	}
	// const n0 = 6000
	// const nTot = 300
	// if len(pathList) > n0+nTot {
	// 	pathList = pathList[n0 : n0+nTot]
	// }
	term := strings.Join(flag.Args(), " ")
	var results string

	// common.Log.Debug("doPersist=%t", doPersist)

	numPages := 0
	t0 := time.Now()
	if !inMemory {
		persistDir := "yyy"
		_, index, nPages, err := utils.IndexPdfs(pathList, persistDir, true, false)
		if err != nil {
			panic(err)
		}
		index.Close()
		numPages = nPages
		results, err = utils.SearchPdfIndex(persistDir, term, maxResults)
		if err != nil {
			panic(err)
		}
	} else {
		lState, index, nPages, err := utils.IndexPdfs(pathList, "", true, false)
		if err != nil {
			panic(err)
		}
		numPages = nPages
		results, err = utils.SearchIndex(lState, index, term, maxResults)
		if err != nil {
			panic(err)
		}
	}
	dt := time.Since(t0)
	fmt.Println("=================+++=====================")
	fmt.Printf("%s\n", results)
	fmt.Println("=================xxx=====================")
	pagesSec := 0.0
	if dt.Seconds() >= 0.01 {
		pagesSec = float64(numPages) / dt.Seconds()
	}
	showList := pathList
	if len(showList) > 10 {
		showList = showList[:10]
	}

	fmt.Printf("[%s index] Duration=%.1f sec (%.1f pages/sec) %d pages in %d files %+v\n",
		indexKinds[inMemory], dt.Seconds(), pagesSec, numPages, len(pathList), showList)
}

var indexKinds = map[bool]string{
	false: "On-disk",
	true:  "In-memory",
}
