package doclib

import (
	"fmt"
	"path/filepath"

	"github.com/unidoc/unidoc/common"
	"github.com/unidoc/unidoc/pdf/extractor"
	pdf "github.com/unidoc/unidoc/pdf/model"
)

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
	PageNum  uint32 // PageNum number.
	Contents string // PageNum text.
}

type LocPage struct {
	ID        string // Unique identifier. <file hash>.<page number>
	Name      string // File name.
	PageNum   uint32 // PageNum number.
	Contents  string // PageNum text.
	Locations []extractor.TextLocation
}

func (l LocPage) ToPdfPage() PdfPage {
	return PdfPage{ID: l.ID, Name: l.Name, PageNum: l.PageNum, Contents: l.Contents}
}

// ExtractDocPages uses UniDoc to extract the text from all pages in PDF file `inPath` as a slice
// of PdfPage.
func ExtractDocPages(inPath string) ([]PdfPage, error) {
	hash, err := FileHash(inPath)
	if err != nil {
		return nil, err
	}

	var docPages []PdfPage

	return docPages, ProcessPDFPagesFile(inPath, func(pageNum uint32, page *pdf.PdfPage) error {
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
			PageNum:  pageNum,
			Contents: text,
		})
		if len(docPages)%100 == 99 {
			common.Log.Debug("\tpageNum=%d docPages=%d %q", pageNum, len(docPages), inPath)
		}
		return nil
	})
}

// ExtractDocPagesChan uses UniDoc to extract the text from all pages in PDF file `inPath`.
// It sends the non-empty pages it successfully extracts to channel `docPages`.
// It returns the page numbers of these pages so that a caller can know pages to check for
// completion in the channel's receiver.
func ExtractDocPagesChan(inPath string, docPages chan<- PdfPage) ([]uint32, error) {

	hash, err := FileHash(inPath)
	if err != nil {
		return nil, err
	}

	var pagesDone []uint32

	return pagesDone, ProcessPDFPagesFile(inPath, func(pageNum uint32, page *pdf.PdfPage) error {
		text, err := ExtractPageText(page)
		if err != nil {
			common.Log.Error("ExtractDocPages: ExtractPageText failed. inPath=%q pageNum=%d err=%v",
				inPath, pageNum, err)
			return err
		}
		if text == "" {
			return nil
		}
		docPages <- PdfPage{
			ID:       fmt.Sprintf("%s.%d", hash[:10], pageNum),
			Name:     filepath.Base(inPath),
			PageNum:  pageNum,
			Contents: text,
		}
		pagesDone = append(pagesDone, pageNum)
		if len(docPages)%100 == 99 {
			common.Log.Debug("\tpageNum=%d docPages=%d %q", pageNum, len(pagesDone), inPath)
		}
		return nil
	})
}

// ExtractDocPagesLookup extracts pages from PDF including text positions.
func ExtractDocPagesLookup(inPath string) ([]LocPage, error) {

	hash, err := FileHash(inPath)
	if err != nil {
		return nil, err
	}

	var docPages []LocPage

	return docPages, ProcessPDFPagesFile(inPath, func(pageNum uint32, page *pdf.PdfPage) error {
		text, locations, err := ExtractPageTextLocation(page)
		if err != nil {
			common.Log.Error("ExtractDocPagesLookup: ExtractPageTextLocation failed. inPath=%q pageNum=%d err=%v",
				inPath, pageNum, err)
			return err
		}
		if text == "" {
			return nil
		}
		docPages = append(docPages, LocPage{
			ID:        fmt.Sprintf("%s.%d", hash[:10], pageNum),
			Name:      filepath.Base(inPath),
			PageNum:   pageNum,
			Contents:  text,
			Locations: locations,
		})
		if len(docPages)%100 == 99 {
			common.Log.Info("\tpageNum=%d docPages=%d %q", pageNum, len(docPages), inPath)
		}
		return nil
	})
}
