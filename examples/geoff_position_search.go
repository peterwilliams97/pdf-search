package main

import (
	"flag"
	"os"
	"strings"

	"github.com/peterwilliams97/pdf-search/utils"
	"github.com/unidoc/unidoc/common"
)

const usage = `Usage: go run position_search.go [OPTIONS] Adobe PDF
Performs a full text search for "Adobe PDF" in Bleve index "store.position" that was created with
simple_index.go`

var persistDir = "store.position"

func main() {
	maxResults := 10
	flag.StringVar(&persistDir, "s", persistDir, "Bleve store name. This is a directory.")
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

	term := strings.Join(flag.Args(), " ")

	results, err := utils.SearchPdfIndex(persistDir, term, maxResults)
	if err != nil {
		panic(err)
	}
	common.Log.Info("=================+++=====================")
	common.Log.Info("%s", results)
}
