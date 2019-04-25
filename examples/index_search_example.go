package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	psearch "github.com/peterwilliams97/pdf-search"
	"github.com/peterwilliams97/pdf-search/doclib"
)

/*
	This program shows how to use functions in the pdf-search library.

	1) Create a bleve+PDF index with psearch.IndexPdfFiles() or psearch.IndexPdfMem()
	2) Optionally reuse an existing on-disk bleve+PDF index with psearch.ReuseIndex
	3) Search the bleve+PDF index with Search() or SearchMem()
*/
/*
	Test results from Peter's MacBook Pro.
	-------------------------------------
	./index_search_example -p -f ~/testdata/adobe/PDF32000_2008.pdf  Type 1
	[On-disk index] Duration=72.4 sec

	./index_search_example -f ~/testdata/adobe/PDF32000_2008.pdf  Type 1
	[In-memory index] Duration=22.7 sec

	Timings from Peter's Mac Book Pro.
	./index_search_example -f ~/testdata/other/pcng/docs/target/output/pcng-manual.pdf  PaperCut NG
	[In-memory index] Duration=87.3 sec (87.220 index + 0.055 search) (454.4 pages/min)
	[In-memory index] Duration=91.9 sec (91.886 index + 0.060 search) (431.3 pages/min)
	[In-memory index] Duration=83.1 sec (83.027 index + 0.068 search) (477.3 pages/min)
	[On-disk index] Duration=126.2 sec (126.039 index + 0.152 search) (314.3 pages/min)
	[Reused index] Duration=0.2 sec (0.000 index + 0.159 search) (0.0 pages/min) 0 pages in 0 files []
	661 pages in 1 files [/Users/pcadmin/testdata/other/pcng/docs/target/output/pcng-manual.pdf]
*/

const usage = `Usage: go run index_search_example.go [OPTIONS] -f "PDF32000*.pdf"  Adobe PDF
Performs a full text search for "Adobe PDF" in PDF files that match "PDF32000*.pdf".`

func main() {
	var pathPattern string
	var persistDir string
	var memory bool
	var persist bool
	var reuse bool
	var nameOnly bool
	var useReaderSeeker bool
	maxResults := 10
	outPath := "search.results.pdf"

	flag.StringVar(&pathPattern, "f", pathPattern, "PDF file(s) to index.")
	flag.StringVar(&outPath, "o", outPath, "Name of PDF file that will show marked up results.")
	flag.StringVar(&persistDir, "s", psearch.DefaultPersistDir, "The on-disk index is stored here.")
	flag.BoolVar(&memory, "m", memory, "Serialize buffers to memory.")
	flag.BoolVar(&persist, "p", persist, "Store index on disk (slower but allows more PDF files).")
	flag.BoolVar(&reuse, "r", reuse, "Reused stored index on disk for the last -p run.")
	flag.BoolVar(&nameOnly, "l", nameOnly, "Return matching file names only.")
	flag.IntVar(&maxResults, "n", maxResults, "Max number of results to return.")
	flag.BoolVar(&useReaderSeeker, "j", useReaderSeeker, "Exercise the io.ReaderSeeker API.")

	doclib.MakeUsage(usage)
	flag.Parse()
	doclib.SetLogging()

	if len(flag.Args()) < 1 {
		flag.Usage()
		os.Exit(1)
	}

	if reuse {
		if memory {
			fmt.Fprintf(os.Stderr,
				"Memory-serialized stores cannot be reused. Using unserialized store.")
			memory = false
		}
	}

	maxSearchResults := maxResults
	if nameOnly {
		maxSearchResults = 1e9
	}

	var err error
	var pathList []string
	if !reuse {
		pathList, err = doclib.PatternsToPaths([]string{pathPattern}, true)
		if err != nil {
			fmt.Fprintf(os.Stderr, "PatternsToPaths failed. args=%#q err=%v\n", flag.Args(), err)
			os.Exit(1)
		}
		pathList = doclib.CleanCorpus(pathList)
		if len(pathList) < 1 {
			fmt.Fprintf(os.Stderr, "No files matching %q.\n", pathPattern)
			os.Exit(1)
		}
	}

	term := strings.Join(flag.Args(), " ")

	t0 := time.Now()
	var pdfIndex psearch.PdfIndex
	var data []byte
	if reuse {
		pdfIndex = psearch.ReuseIndex(persistDir)
	} else if memory {
		var rsList []io.ReadSeeker
		for _, inPath := range pathList {
			rs, err := os.Open(inPath)
			if err != nil {
				panic(err)
			}
			defer rs.Close()
			rsList = append(rsList, rs)
		}
		data, err = psearch.IndexPdfMem(pathList, rsList, report)
		if err != nil {
			panic(err)
		}
	} else {
		pdfIndex, err = psearch.IndexPdfFiles(pathList, persist, persistDir, report, useReaderSeeker)
		if err != nil {
			panic(err)
		}
	}

	var results doclib.PdfMatchSet
	dtIndex := time.Since(t0)
	if memory {
		results, err = psearch.SearchMem(data, term, maxSearchResults)
		if err != nil {
			panic(err)
		}
	} else {
		results, err = pdfIndex.Search(term, maxSearchResults)
		if err != nil {
			panic(err)
		}
	}
	dt := time.Since(t0)
	dtSearch := dt - dtIndex

	if nameOnly {
		files := results.Files()
		if len(files) > maxResults {
			files = files[:maxResults]
		}
		for i, fn := range files {
			fmt.Printf("%4d: %q\n", i, fn)
		}
	} else {

		fmt.Println("=================+++=====================")
		fmt.Printf("%s\n", results)
		fmt.Println("=================xxx=====================")
	}

	if err = psearch.MarkupPdfResults(results, outPath); err != nil {
		panic(err)
	}
	fmt.Println("=================+++=====================")

	numPages := pdfIndex.NumPages()
	pagesSec := 0.0
	if dt.Seconds() >= 0.01 {
		pagesSec = float64(numPages) / dt.Seconds()
	}
	showList := pathList
	if len(showList) > 10 {
		showList = showList[:10]
		for i, fn := range showList {
			showList[i] = filepath.Base(fn)
		}
	}

	storage := "SerialMem"
	if !memory {
		storage = pdfIndex.StorageName()
	}

	fmt.Fprintf(os.Stderr, "[%s index] Duration=%.1f sec (%.3f index + %.3f search) (%.1f pages/min) "+
		"%d pages in %d files %+v\n"+
		"Marked up search results in %q\n",
		storage, dt.Seconds(), dtIndex.Seconds(), dtSearch.Seconds(),
		pagesSec*60.0, numPages, len(pathList), showList, outPath)
}

func report(msg string) {
	fmt.Fprintf(os.Stderr, ">> %s\n", msg)
}
