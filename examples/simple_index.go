package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/blevesearch/bleve"
	"github.com/peterwilliams97/pdf-search/doclib"
)

const usage = `Usage: go run simple_index.go [OPTIONS] PDF32000_2008.pdf
Runs UniDoc PDF text extraction on PDF32000_2008.pdf and writes a Bleve index to store.simple.`

var persistDir = "store.simple"

func main() {
	flag.StringVar(&persistDir, "s", persistDir, "Index store directory name.")
	var forceCreate, allowAppend bool
	flag.BoolVar(&forceCreate, "f", false, "Force creation of a new Bleve index.")
	flag.BoolVar(&allowAppend, "a", false, "Allow existing an Bleve index to be appended to.")

	doclib.MakeUsage(usage)
	flag.Parse()
	doclib.SetLogging()
	if len(flag.Args()) < 1 {
		flag.Usage()
		os.Exit(1)
	}

	indexPath := filepath.Join(persistDir, "bleve")
	fmt.Printf("indexPath=%q\n", indexPath)

	// Read the list of PDF files that will be processed.
	pathList, err := doclib.PatternsToPaths(flag.Args(), true)
	if err != nil {
		fmt.Fprintf(os.Stderr, "PatternsToPaths failed. args=%#q err=%v\n", flag.Args(), err)
		os.Exit(1)
	}
	pathList = doclib.CleanCorpus(pathList)
	fmt.Printf("Indexing %d PDF files.\n", len(pathList))

	// Create a new Bleve index.
	index, err := doclib.CreateBleveIndex(indexPath, forceCreate, allowAppend)
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
	fmt.Printf("indexPath=%q\n", indexPath)
}

// indexDocPages adds the text of all the pages in PDF file `inPath` to Bleve index `index`.
func indexDocPages(index bleve.Index, inPath string) error {
	docPages, err := doclib.ExtractDocPages(inPath)
	if err != nil {
		fmt.Printf("indexDocPages: Couldn't extract pages from %q err=%v\n", inPath, err)
		return nil
	}
	fmt.Printf("indexDocPages: inPath=%q docPages=%d\n", inPath, len(docPages))
	t0 := time.Now()
	for i, page := range docPages {
		err = index.Index(page.ID, page)
		dt := time.Since(t0)
		if err != nil {
			return err
		}
		if i%10 == 0 {
			fmt.Printf("\tIndexed %2d of %d pages in %5.1f sec (%.2f sec/page)\n",
				i+1, len(docPages), dt.Seconds(), dt.Seconds()/float64(i+1))
			fmt.Printf("\tid=%q text=%d\n", page.ID, len(page.Contents))
		}
	}
	dt := time.Since(t0)
	fmt.Printf("\tIndexed %d pages in %.1f sec (%.3f sec/page)\n",
		len(docPages), dt.Seconds(), dt.Seconds()/float64(len(docPages)))

	return nil
}
