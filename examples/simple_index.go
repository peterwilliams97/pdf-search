package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/blevesearch/bleve"
	"github.com/peterwilliams97/pdf-search/utils"
)

var store = "store.simple"

func main() {
	flag.StringVar(&store, "s", store, "Bleve store name. This is a directory.")
	utils.MakeUsage(`Usage: go run simple_index.go [OPTIONS] PDF32000_2008.pdf
Runs UniDoc PDF text extraction on PDF32000_2008.pdf and writes a Bleve index to store.simple.`)
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

	pathList, err := utils.PatternsToPaths(flag.Args(), true)
	if err != nil {
		fmt.Fprintf(os.Stderr, "PatternsToPaths failed. args=%#q err=%v\n", flag.Args(), err)
		os.Exit(1)
	}
	fmt.Printf("Indexing %d PDF files.\n", len(pathList))

	// Create a new index.
	mapping := bleve.NewIndexMapping()
	index, err := bleve.New(store, mapping)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not create Bleve index %q.\n", store)
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
}

// indexDocPages adds the text of all the pages in PDF file `inPath` to Bleve index `index`.
func indexDocPages(index bleve.Index, inPath string) error {
	docPages, err := extractDocPages(inPath)
	if err != nil {
		fmt.Printf("indexDocPages: Couldn't extract pages from %q err=%v\n", inPath, err)
		return nil
	}
	for _, page := range docPages {
		err = index.Index(page.ID, page)
		if err != nil {
			return err
		}
	}
	return nil
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
			fmt.Printf("extractDocPages: ExtractPageText failed. inPath=%q pageNum=%d err=%v\n",
				inPath, pageNum, err)
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
