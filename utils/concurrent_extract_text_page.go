package utils

import "fmt"

type JobCompletion struct {
	DocID // Identifies PDF file.
	Pages []int
	Err   error
}

// extractPageQueue is a queue of PDF processing jobs.
type extractPageQueue struct {
	workQueue     chan *extractPageWork  // Work instructions. 1 per PDF file.
	pageDoneQueue chan ExtractPageResult // Page results. 1 per PDF page succesfully processed.
	docDoneQueue  chan JobCompletion     // Page results. 1 per PDF page succesfully processed.
	done          chan struct{}
}

// NewExtractPageQueue creates a processing queue for document text extraction with `numWorkers`
// worker go routines.
func NewExtractPageQueue(numWorkers int) *extractPageQueue {
	q := extractPageQueue{
		workQueue:     make(chan *extractPageWork),
		pageDoneQueue: make(chan ExtractPageResult),
		docDoneQueue:  make(chan JobCompletion),
		done:          make(chan struct{}),
	}
	for i := 0; i < numWorkers; i++ {
		go extractPageWorker(q)
	}
	return &q
}

// Complete runs `completeJob` on all jobs in `q` and returns when `numJobs` are processed.
func (q *extractPageQueue) Complete(numJobs int, completeJob func(page ExtractPageResult) error) {
	numJobsDone := 0
	numPagesDone := 0
	numPages := 0
	for {
		select {
		case d := <-q.docDoneQueue:
			numJobsDone++
			numPages += len(d.Pages)
			fmt.Printf("done=%d pages=%d -- %s\n", numJobsDone, numPages, d.DocID)
		case p := <-q.pageDoneQueue:
			completeJob(p)
			numPagesDone++
		case <-q.done:
			return
		}
		if numJobsDone == numJobs && numPagesDone == numPages {
			return
		}
	}
}

// Queue enqeues PDF processing instructions `work` in processing queue `q`.
func (q *extractPageQueue) Queue(work *extractPageWork) {
	q.workQueue <- work
}

// Close shut downs all the workers in `q`
func (q *extractPageQueue) Close() {
	close(q.done)
}

// NewExtractPageWork creates PDF processing instructions for PDF file `inPath`.
// `idx` is an index that the caller knows about, typically order of submistion It is currently used
// by the caller to compare order of completion to order of completion.
// `rc` is a channel for returning results.
func NewExtractPageWork(idx int, inPath string, rc chan *ExtractPageResult) *extractPageWork {
	id := DocID{idx, inPath}
	return &extractPageWork{id, rc}
}

// extractPageWork is a set of instructions for one PDF processing job.
type extractPageWork struct {
	DocID                         // Identifies PDF file.
	rc    chan *ExtractPageResult // Results are sent via this channel.
}

// ExtractPageResult are the results of one PDF processing job.
type ExtractPageResult struct {
	DocID         // Identifies PDF file.
	Page  PdfPage // Information about text on pages.
	err   error
}

// extract runs the PDF processing code according to the instructions in work `w` and writes
// results to `results`.
func (w extractPageWork) extract(results chan<- ExtractPageResult) JobCompletion {
	docPages := make(chan PdfPage)
	done := make(chan struct{})
	go func() {
		for {
			select {
			case p := <-docPages:
				r := ExtractPageResult{DocID: w.DocID, Page: p}
				results <- r
			case <-done:
				return
			}
		}
	}()
	pages, err := ExtractDocPagesChan(w.inPath, docPages)
	close(done)
	return JobCompletion{w.DocID, pages, err}
}

// extractPageWorker runs in each worker go routine.
func extractPageWorker(q extractPageQueue) {
	// extractPageWorker reads work `w` off `q`, calls the PDF processing code `extract` with this
	// work, and passes `extract`'s return to the `w.rc` channel.
	for {
		select {
		case w := <-q.workQueue:
			d := w.extract(q.pageDoneQueue)
			q.docDoneQueue <- d
		case <-q.done:
			return
		}
	}
}
