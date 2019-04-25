package main

// 144 884 492
import (
	"flag"
	"fmt"
	"os"

	"github.com/peterwilliams97/pdf-search/doclib"
)

const usage = `Usage: go run position_index.go [OPTIONS] PDF32000_2008.pdf
Runs UniDoc PDF text extraction on PDF32000_2008.pdf and writes a Bleve index to store.position.`

var persistDir = "store.position"

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

	// Read the list of PDF files that will be processed.
	pathList, err := doclib.PatternsToPaths(flag.Args(), true)
	if err != nil {
		fmt.Fprintf(os.Stderr, "PatternsToPaths failed. args=%#q err=%v\n", flag.Args(), err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "Total of %d PDF files.\n", len(pathList))
	pathList = doclib.CleanCorpus(pathList)
	lState, index, totalPages, err := doclib.IndexPdfFiles(pathList, persistDir, forceCreate, allowAppend, report)
	if err != nil {
		panic(err)
	}
	fmt.Fprintf(os.Stderr, "lState=%+v\n", *lState)
	fmt.Fprintf(os.Stderr, "index=%+v\n", index)
	fmt.Fprintf(os.Stderr, "totalPages=%d\n", totalPages)
	fmt.Fprintf(os.Stderr, "persistDir=%q\n", persistDir)
}

func report(msg string) {
	fmt.Fprintf(os.Stderr, ">> %s\n", msg)
}
