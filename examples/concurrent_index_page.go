package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"

	"github.com/peterwilliams97/pdf-search/doclib"
)

const usage = `Usage: go run concurrent_inde_page.go [OPTIONS] testdata/*.pdf
Runs UniDoc PDF text extraction on PDF files in testdata and writes a Bleve index to
store.concurrent.`

var indexPath = "store.concurrent.page"

func main() {
	flag.StringVar(&indexPath, "s", indexPath, "Bleve store name. This is a directory.")
	var forceCreate, allowAppend bool
	flag.BoolVar(&forceCreate, "f", false, "Force creation of a new Bleve index.")
	flag.BoolVar(&allowAppend, "a", false, "Allow existing an Bleve index to be appended to.")
	numWorkers := -1
	flag.IntVar(&numWorkers, "w", numWorkers, "Number of worker threads.")
	doclib.MakeUsage(usage)

	fmt.Printf("GOMAXPROCS: %d\n", runtime.GOMAXPROCS(-1))
	fmt.Printf("NumCPU: %d\n\n", runtime.NumCPU())

	flag.Parse()
	doclib.SetLogging()
	if len(flag.Args()) < 1 {
		flag.Usage()
		os.Exit(1)
	}

	// Read the list of PDF files that will be processed.
	pathList, err := doclib.PatternsToPaths(flag.Args(), true)
	if err != nil {
		fmt.Fprintf(os.Stderr, "PatternsToPaths failed. args=%#q err=%v\n", flag.Args(), err)
		os.Exit(1)
	}
	pathList = doclib.CleanCorpus(pathList)
	fmt.Printf("Indexing %d PDF files.\n", len(pathList))

	// Create a new index.
	index, err := doclib.CreateBleveIndex(indexPath, forceCreate, allowAppend)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not create Bleve index %q.\n", indexPath)
		panic(err)
	}

	// Set a number of worker threads that won't overload the host computer.
	if numWorkers < 0 {
		numWorkers = runtime.NumCPU() - 1
	}
	if numWorkers <= 0 {
		numWorkers = 1
	}
	fmt.Printf("%d workers\n", numWorkers)

	// Create the processing queue.
	queue := doclib.NewExtractPageQueue(numWorkers)
	resultChan := make(chan *doclib.ExtractPageResult)

	// Start a go routine to feed the processing queue.
	go func() {
		// Create processing instructions `w` for each file in pathList and add the processing
		// instructions to the queue.
		for i, inPath := range pathList {
			w := doclib.NewExtractPageWork(i, inPath, resultChan)
			queue.Queue(w)
		}
	}()

	completeJob := func(pageResult doclib.ExtractPageResult) error {
		page := pageResult.Page
		err := index.Index(page.ID, page)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Could not index %s.\n", pageResult.DocID)
			panic(err)
		}
		return err
	}

	// Wait for extraction results here in the main thread.
	queue.Complete(len(pathList), completeJob)

	// Shut down the processing queue workers.
	queue.Close()

	docCount, err := index.DocCount()
	if err != nil {
		fmt.Fprintf(os.Stderr, "index.DocCount failed. err=%v\n", err)
		return
	}
	fmt.Printf("Total %d pages.\n", docCount)

	fmt.Println("Finished")
}
