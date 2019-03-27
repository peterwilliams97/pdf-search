package main

// 144 884 492
import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/blevesearch/bleve"
	"github.com/peterwilliams97/pdf-search/utils"
)

const usage = `Usage: go run location_index.go [OPTIONS] PDF32000_2008.pdf
Runs UniDoc PDF text extraction on PDF32000_2008.pdf and writes a Bleve index to store.simple.`

var basePath = "store.position"
var minFileNum = -1
var maxFileNum = -1

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

	indexPath := filepath.Join(basePath, "bleve")
	fmt.Printf("indexPath=%q\n", indexPath)

	// Read the list of PDF files that will be processed.
	pathList, err := utils.PatternsToPaths(flag.Args(), true)
	if err != nil {
		fmt.Fprintf(os.Stderr, "PatternsToPaths failed. args=%#q err=%v\n", flag.Args(), err)
		os.Exit(1)
	}
	pathList = utils.CleanCorpus(pathList)
	if len(pathList) > 5860 { // !@#$
		pathList = pathList[5860:]
	}

	fmt.Printf("Indexing %d PDF files.\n", len(pathList))

	lState, err := utils.OpenPositionsState(basePath, forceCreate)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not create positions store %q. err=%v\n", basePath, err)
		panic(err)
	}
	defer lState.Flush()

	// Create a new Bleve index.
	index, err := utils.CreateBleveIndex(indexPath, forceCreate, allowAppend)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not create Bleve index %q.\n", indexPath)
		panic(err)
	}

	// Add the pages of all the PDFs in `pathList` to `index`.
	for i, inPath := range pathList {
		if minFileNum > 0 && i+1 < minFileNum {
			continue
		}
		if maxFileNum > 0 && i+1 > maxFileNum {
			break
		}
		fmt.Fprintf(os.Stderr, ">> %3d of %d: %q\n", i+1, len(pathList), inPath)
		err := indexDocPagesLoc(index, lState, inPath)
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

type IDText struct {
	ID   string
	Text string
}

// indexDocPagesLoc adds the text of all the pages in PDF file `inPath` to Bleve index `index`.
func indexDocPagesLoc(index bleve.Index, lState *utils.PositionsState, inPath string) error {
	docPages, err := lState.ExtractDocPagePositions(inPath)
	if err != nil {
		fmt.Printf("indexDocPagesLoc: Couldn't extract pages from %q err=%v\n", inPath, err)
		return nil
	}
	fmt.Printf("indexDocPagesLoc: inPath=%q docPages=%d\n", inPath, len(docPages))

	// for _, l := range docPages {
	// 	dpl := l.ToDocPageLocations()
	// 	if err := serial.WriteDocPageLocations(locationsFile, dpl); err != nil {
	// 		return err
	// 	}
	// }

	t0 := time.Now()
	for i, l := range docPages {
		// Don't weigh down the Bleve index with the text bounding boxes.
		id := fmt.Sprintf("%04X.%d", l.DocIdx, l.PageIdx)
		idText := IDText{ID: id, Text: l.Text}

		err = index.Index(id, idText)
		dt := time.Since(t0)
		if err != nil {
			return err
		}
		if i%100 == 0 {
			fmt.Printf("\tIndexed %2d of %d pages in %5.1f sec (%.2f sec/page)\n",
				i+1, len(docPages), dt.Seconds(), dt.Seconds()/float64(i+1))
			fmt.Printf("\tid=%q text=%d\n", id, len(idText.Text))
		}
	}
	dt := time.Since(t0)
	fmt.Printf("\tIndexed %d pages in %.1f sec (%.3f sec/page)\n",
		len(docPages), dt.Seconds(), dt.Seconds()/float64(len(docPages)))
	return nil
}
