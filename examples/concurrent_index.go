package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"github.com/blevesearch/bleve"
	"github.com/peterwilliams97/pdf-search/utils"
)

var indexPath = "store.concurrent"

func main() {
	numWorkers := -1
	flag.StringVar(&indexPath, "s", indexPath, "Bleve store name. This is a directory.")
	flag.IntVar(&numWorkers, "w", numWorkers, "Number of worker threads.")
	utils.MakeUsage(`Usage: go run concurrent_index.go [OPTIONS] testdata/*.pdf
Runs UniDoc PDF text extraction on PDF files in testdata and writes a Bleve index to
store.concurrent.`)

	fmt.Printf("GOMAXPROCS: %d\n", runtime.GOMAXPROCS(-1))
	fmt.Printf("NumCPU: %d\n\n", runtime.NumCPU())

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
	mapping := bleve.NewIndexMapping()
	index, err := bleve.New(indexPath, mapping)
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
	queue := NewExtractDocQueue(numWorkers)
	resultChan := make(chan *extractDocResult, len(pathList))

	// Start a go routine to feed the processing queue.
	go func() {
		for i, inPath := range pathList {
			w := NewExtractDocWork(i, inPath, resultChan)
			// Put the work on the queue
			queue.Queue(w)
		}
	}()

	// Wait for extraction results here in the main thread.
	for numDone := 0; numDone < len(pathList); numDone++ {
		result := <-resultChan
		for _, page := range result.docPages {
			err = index.Index(page.ID, page)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Could not index %q.\n", result.inPath)
				panic(err)
			}
		}
		docCount, err := index.DocCount()
		if err != nil {
			fmt.Printf("%+v index.DocCount failed err=%v.\n", result.docID, err)
			continue
		}
		fmt.Printf("done=%d i=%d %q Total %d pages.\n",
			numDone, result.idx, filepath.Base(result.inPath), docCount)
	}

	// Shut down the processing queue workers.
	queue.Close()

	fmt.Println("Finished")
}

// NewExtractDocQueue creates a processing queue for document text extraction with `numWorkers`
// worker go routines.
func NewExtractDocQueue(numWorkers int) *extractDocQueue {
	q := extractDocQueue{
		queue: make(chan *extractDocWork),
		done:  make(chan struct{}),
	}
	for i := 0; i < numWorkers; i++ {
		go extractDocWorker(q)
	}
	return &q
}

// Queue enqeues PDF processing instructions `work` in processing queue `q`.
func (q *extractDocQueue) Queue(work *extractDocWork) {
	q.queue <- work
}

// Close shut downs all the workers in `q`
func (q *extractDocQueue) Close() {
	close(q.done)
}

// NewExtractDocWork creates PDF processing instructions for PDF file `inPath`.
// `idx` is an index that the caller knows about, typically order of submistion It is currently used
// by the caller to compare order of completion to order of completion.
// `rc` is a channel for returning results.
func NewExtractDocWork(idx int, inPath string, rc chan *extractDocResult) *extractDocWork {
	id := docID{idx, inPath}
	return &extractDocWork{id, rc}
}

// extractDocQueue is a queue of PDF processing jobs.
type extractDocQueue struct {
	queue chan *extractDocWork
	done  chan struct{}
}

// extractDocWork is a set of instructions for one PDF processing job.
type extractDocWork struct {
	docID                        // Identifies PDF file.
	rc    chan *extractDocResult // Results are via this channel.
}

// extractDocResult are the results of one PDF processing job.
type extractDocResult struct {
	docID              // Identifies PDF file.
	docPages []PdfPage // Information about text on pages.
	err      error
}

// docID identifies a PDF file.
type docID struct {
	idx    int // index into input list
	inPath string
}

// extract runns the PDF processing code for the instructions in `w`.
func (w extractDocWork) extract() *extractDocResult {
	result := extractDocResult{docID: w.docID}
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("Recover: %+v r=%v\n", w.docID, r)
			fmt.Fprintf(os.Stderr, "Recover: %+v r=%v\n", w.docID, r)
			result.err = r.(error)
		}
	}()
	result.docPages, result.err = extractDocPages(w.inPath)
	return &result
}

// extractDocWorker runs in each worker go routine. It reads work off `q` and calls the PDF
// processing code.
func extractDocWorker(q extractDocQueue) {
	for {
		select {
		case <-q.done:
			return
		case w := <-q.queue:
			w.rc <- w.extract()
		}
	}
}

// PdfPage is a simple but inefficient way of encoding a PDF page in bleve index.
// We use it in our first example program because we are just showing how Bleve works and not
// writing production code.
type PdfPage struct {
	ID       string // Unique identifier. <file hash>.<page number>
	Name     string // File name.
	Page     int    // Page number.
	Contents string // Page text.
}

// extractDocPages uses UniDoc to extract the text from all pages in PDF file `inPath` as a slice
// of PdfPage.
func extractDocPages(inPath string) ([]PdfPage, error) {

	hash, err := utils.FileHash(inPath)
	if err != nil {
		return nil, err
	}

	pdfReader, err := utils.PdfOpen(inPath)
	if err != nil {
		fmt.Printf("extractDocPages: Could not open inPath=%q. err=%v\n", inPath, err)
		return nil, err
	}
	numPages, err := pdfReader.GetNumPages()
	if err != nil {
		return nil, err
	}

	var docPages []PdfPage
	for pageNum := 1; pageNum < numPages; pageNum++ {
		page, err := pdfReader.GetPage(pageNum)
		if err != nil {
			return nil, err
		}

		var text string
		text, err = utils.ExtractPageText(page)
		if err != nil {
			if utils.Debug {
				fmt.Printf("extractDocPages: ExtractPageText failed. inPath=%q pageNum=%d err=%v\n",
					inPath, pageNum, err)
			}
			return nil, err
		}
		if text == "" {
			continue
		}

		pdfPage := PdfPage{
			ID:       fmt.Sprintf("%s.%d", hash[:10], pageNum),
			Name:     filepath.Base(inPath),
			Page:     pageNum,
			Contents: text,
		}

		docPages = append(docPages, pdfPage)
		if len(docPages)%100 == 99 {
			fmt.Printf("\tpageNum=%d docPages=%d %q\n", pageNum, len(docPages), inPath)
		}
	}

	return docPages, nil
}
