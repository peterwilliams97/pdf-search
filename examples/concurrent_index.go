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
		docCount := len(result.docPages)
		// docCount, err := index.DocCount()
		// if err != nil {
		// 	fmt.Printf("worker %d) %+v index.DocCount failed err=%v.\n",
		// 		result.workerIdx, result.docID, err)
		// 	continue
		// }
		fmt.Printf("worker %d) done=%d i=%d %q Total %d pages.\n",
			result.workerIdx, numDone, result.idx, filepath.Base(result.inPath), docCount)
	}

	// Shut down the processing queue.
	fmt.Println("Finished")
	queue.Close()
}

// NewExtractDocQueue creates a processing queue for document text extraction with `numWorkers`
// worker go routines.
func NewExtractDocQueue(numWorkers int) *extractDocQueue {
	q := extractDocQueue{
		queue: make(chan *extractDocWork),
		done:  make(chan struct{}),
	}
	for i := 0; i < numWorkers; i++ {
		go extractDocWorker(i, q)
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

type extractDocQueue struct {
	queue chan *extractDocWork
	done  chan struct{}
}

type extractDocWork struct {
	docID
	rc chan *extractDocResult
}

type extractDocResult struct {
	docID
	docPages  []PdfPage
	err       error
	workerIdx int
}

type docID struct {
	idx    int // index into input list
	inPath string
}

func (w extractDocWork) extract(workerIdx int) *extractDocResult {
	fmt.Printf("worker %d) extract %q\n", workerIdx, w.inPath)
	result := extractDocResult{docID: w.docID, workerIdx: workerIdx}
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("worker %d) crashed %+v r=%v\n", workerIdx, w.docID, r)
			fmt.Fprintf(os.Stderr, "worker %d) crashed %+v r=%v\n", workerIdx, w.docID, r)
			result.err = r.(error)
		}
	}()
	result.docPages, result.err = extractDocPages(fmt.Sprintf("worker %d)", workerIdx), w.inPath)
	return &result
}

// extractDocWorker runs in each worker go routine. It reads work off `q` and calls the PDF
// processing code.
// `workerIdx` is a hacky diagnostic to identify the go routine run in !@#$
// ``
func extractDocWorker(workerIdx int, q extractDocQueue) {
	for {
		select {
		case <-q.done:
			return
		case w := <-q.queue:
			r := w.extract(workerIdx)
			w.rc <- r
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
// of PdfPage
func extractDocPages(desc, inPath string) ([]PdfPage, error) {

	hash, err := utils.FileHash(inPath)
	if err != nil {
		return nil, err
	}

	pdfReader, err := utils.PdfOpen(inPath)
	if err != nil {
		fmt.Printf("%s extractDocPages: Could not open inPath=%q. err=%v\n", desc, inPath, err)
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
				fmt.Printf("%s extractDocPages: ExtractPageText failed. inPath=%q pageNum=%d err=%v\n",
					desc, inPath, pageNum, err)
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
			fmt.Printf("\t%s pageNum=%d docPages=%d %q\n", desc, pageNum, len(docPages), inPath)
		}
	}

	return docPages, nil
}
