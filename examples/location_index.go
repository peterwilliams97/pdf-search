package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/blevesearch/bleve"
	"github.com/peterwilliams97/pdf-search/utils"
)

const usage = `Usage: go run location_index.go [OPTIONS] PDF32000_2008.pdf
Runs UniDoc PDF text extraction on PDF32000_2008.pdf and writes a Bleve index to store.simple.`

var indexPath = "store.simple"

func main() {
	flag.StringVar(&indexPath, "s", indexPath, "Bleve store name. This is a directory.")
	var forceCreate, allowAppend bool
	flag.BoolVar(&forceCreate, "f", false, "Force creation of a new Bleve index.")
	flag.BoolVar(&allowAppend, "a", false, "Allow existing an Bleve index to be appended to.")

	utils.MakeUsage(usage)
	flag.Parse()
	utils.SetLogging()
	if utils.ShowHelp {
		flag.Usage()
		os.Exit(0)
	}
	if len(flag.Args()) < 1 {
		flag.Usage()
		os.Exit(1)
	}

	// Read the list of PDF files that will be processed.
	pathList, err := utils.PatternsToPaths(flag.Args(), true)
	if err != nil {
		fmt.Fprintf(os.Stderr, "PatternsToPaths failed. args=%#q err=%v\n", flag.Args(), err)
		os.Exit(1)
	}
	pathList = utils.CleanCorpus(pathList)
	fmt.Printf("Indexing %d PDF files.\n", len(pathList))

	// Create a new index.
	index, err := utils.CreateBleveIndex(indexPath, forceCreate, allowAppend)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not create Bleve index %q.\n", indexPath)
		panic(err)
	}

	// Add the pages of all the PDFs in `pathList` to `index`.
	for _, inPath := range pathList {
		err := indexDocPages(index, inPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Could not index %q.\n", inPath)
			panic(err)
		}
		docCount, err := index.DocCount()
		if err != nil {
			panic(err)
		}
		fmt.Printf("Indexed %q. Total %d pages indexed.\n", inPath, docCount)
	}
}

// indexDocPages adds the text of all the pages in PDF file `inPath` to Bleve index `index`.
func indexDocPages(index bleve.Index, inPath string) error {
	docPages, err := utils.ExtractDocPagesLookup(inPath)
	if err != nil {
		fmt.Printf("indexDocPages: Couldn't extract pages from %q err=%v\n", inPath, err)
		return nil
	}
	for _, page := range docPages {
		err = index.Index(page.ID, page)
		if err != nil {
			return err
		}
	}
	return nil
}
