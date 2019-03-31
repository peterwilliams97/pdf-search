package main

// 144 884 492
import (
	"flag"
	"fmt"
	"os"

	"github.com/peterwilliams97/pdf-search/utils"
)

const usage = `Usage: go run position_index.go [OPTIONS] PDF32000_2008.pdf
Runs UniDoc PDF text extraction on PDF32000_2008.pdf and writes a Bleve index to store.position.`

var basePath = "store.position"

func main() {
	flag.StringVar(&basePath, "s", basePath, "Index store directory name.")
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
	fmt.Fprintf(os.Stderr, "Total of %d PDF files.\n", len(pathList))
	pathList = utils.CleanCorpus(pathList)
	if len(pathList) > 5860 { // !@#$
		pathList = pathList[5860:]
	}
	lState, index, err := utils.IndexPdfs(pathList, basePath, forceCreate, allowAppend)
	if err != nil {
		panic(err)
	}
	fmt.Fprintf(os.Stderr, "lState=%+v\n", *lState)
	fmt.Fprintf(os.Stderr, "index=%+v\n", index)
	fmt.Fprintf(os.Stderr, "basePath=%q\n", basePath)
}
