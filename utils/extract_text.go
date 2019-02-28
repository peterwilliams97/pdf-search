package utils

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/unidoc/unidoc/common"
)

// RecoverErrors can be set to true to recover from errors in library functions.
var RecoverErrors bool

func init() {
	flag.BoolVar(&RecoverErrors, "r", false, "Recover from errors in library functions.")
}

// PdfPage is a simple but inefficient way of encoding a PDF page in a Bleve index.
// We use it in our first example program because we are just showing how Bleve works and not
// writing production code.
type PdfPage struct {
	ID       string // Unique identifier. <file hash>.<page number>
	Name     string // File name.
	Page     int    // Page number.
	Contents string // Page text.
}

// ExtractDocPages uses UniDoc to extract the text from all pages in PDF file `inPath` as a slice
// of PdfPage. It can recover from errors in the libraries it calls if RecoverErrors is true.
func ExtractDocPages(inPath string) ([]PdfPage, error) {
	var docPages []PdfPage
	var err error
	if RecoverErrors {
		defer func() {
			if r := recover(); r != nil {
				common.Log.Error("Recover: %q r=%v", inPath, r)
				fmt.Fprintf(os.Stderr, "Recover: %q r=%v\n", inPath)
				err = r.(error)
			}
		}()
	}
	docPages, err = extractDocPages(inPath)
	return docPages, err
}

// extractDocPages uses UniDoc to extract the text from all pages in PDF file `inPath` as a slice
// of PdfPage.
func extractDocPages(inPath string) ([]PdfPage, error) {

	hash, err := FileHash(inPath)
	if err != nil {
		return nil, err
	}

	pdfReader, err := PdfOpen(inPath)
	if err != nil {
		common.Log.Error("ExtractDocPages: Could not open inPath=%q. err=%v", inPath, err)
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

		text, err := ExtractPageText(page)
		if err != nil {
			common.Log.Error("ExtractDocPages: ExtractPageText failed. inPath=%q pageNum=%d err=%v",
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
			common.Log.Debug("\tpageNum=%d docPages=%d %q", pageNum, len(docPages), inPath)
		}
	}

	return docPages, nil
}
