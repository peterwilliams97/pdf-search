package utils

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/unidoc/unidoc/common"
	pdf "github.com/unidoc/unidoc/pdf/model"
)

// RecoverErrors can be set to true to recover from errors in library functions.
var RecoverErrors bool

func init() {
	flag.BoolVar(&RecoverErrors, "r", false, "Recover from errors in library functions.")
}

// DocID identifies a PDF file.
type DocID struct {
	idx    int // index into input list
	inPath string
}

// String returns the view of DocID that users see.
func (id DocID) String() string {
	return fmt.Sprintf("%d: %q", id.idx, id.inPath)
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

	var docPages []PdfPage

	err = ProcessPDFPages(inPath, func(pageNum int, page *pdf.PdfPage) error {
		text, err := ExtractPageText(page)
		if err != nil {
			common.Log.Error("ExtractDocPages: ExtractPageText failed. inPath=%q pageNum=%d err=%v",
				inPath, pageNum, err)
			return err
		}
		if text == "" {
			return nil
		}
		docPages = append(docPages, PdfPage{
			ID:       fmt.Sprintf("%s.%d", hash[:10], pageNum),
			Name:     filepath.Base(inPath),
			Page:     pageNum,
			Contents: text,
		})
		if len(docPages)%100 == 99 {
			common.Log.Debug("\tpageNum=%d docPages=%d %q", pageNum, len(docPages), inPath)
		}
		return nil
	})

	return docPages, nil
}

// ExtractDocPagesChan uses UniDoc to extract the text from all pages in PDF file `inPath`.
// It sends the non-empty pages it successfully extracts to channel `docPages`.
// It returns the page numbers of these pages so that a caller can know pages to check for
// completion in the channe's receiver.
func ExtractDocPagesChan(inPath string, docPages chan<- PdfPage) ([]int, error) {

	hash, err := FileHash(inPath)
	if err != nil {
		return nil, err
	}

	var pagesDone []int
	err = ProcessPDFPages(inPath, func(pageNum int, page *pdf.PdfPage) error {
		text, err := ExtractPageText(page)
		if err != nil {
			common.Log.Error("ExtractDocPages: ExtractPageText failed. inPath=%q pageNum=%d err=%v",
				inPath, pageNum, err)
			return err
		}
		if text == "" {
			return nil
		}
		docPages <-  PdfPage{
			ID:       fmt.Sprintf("%s.%d", hash[:10], pageNum),
			Name:     filepath.Base(inPath),
			Page:     pageNum,
			Contents: text,
		}
		pagesDone = append(pagesDone, pageNum)
		if len(docPages)%100 == 99 {
			common.Log.Debug("\tpageNum=%d docPages=%d %q", pageNum, len(pagesDone), inPath)
		}
		return nil
	})

	return pagesDone, nil
}
