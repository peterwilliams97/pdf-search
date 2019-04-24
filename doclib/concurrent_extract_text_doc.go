package doclib

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

// Close shut downs all the workers in `q`.
func (q *extractDocQueue) Close() {
	close(q.done)
}

// NewExtractDocWork creates PDF processing instructions for PDF file `inPath`.
// `idx` is an index that the caller knows about, typically order of submistion It is currently used
// by the caller to compare order of completion to order of completion.
// `rc` is a channel for returning results.
func NewExtractDocWork(idx int, inPath string, rc chan *ExtractDocResult) *extractDocWork {
	id := DocID{idx, inPath}
	return &extractDocWork{id, rc}
}

// extractDocQueue is a queue of PDF processing jobs.
type extractDocQueue struct {
	queue chan *extractDocWork
	done  chan struct{}
}

// extractDocWork is a set of instructions for one PDF processing job.
type extractDocWork struct {
	DocID                        // Identifies PDF file.
	rc    chan *ExtractDocResult // Results are sent via this channel.
}

// ExtractDocResult are the results of one PDF processing job.
type ExtractDocResult struct {
	DocID              // Identifies PDF file.
	DocPages []PdfPage // Information about text on pages.
	err      error
}

// extract runs the PDF processing code according to the instructions in work `w`.
func (w extractDocWork) extract() *ExtractDocResult {
	result := ExtractDocResult{DocID: w.DocID}
	result.DocPages, result.err = ExtractDocPages(w.inPath)
	return &result
}

// extractDocWorker runs in each worker go routine.
func extractDocWorker(q extractDocQueue) {
	// extractDocWorker reads work `w` off `q`, calls the PDF processing code `extract` with this
	// work, and passes extract's return to the `w.rc` channel.
	for {
		select {
		case w := <-q.queue:
			r := w.extract()
			w.rc <- r
		case <-q.done:
			return
		}
	}
}
