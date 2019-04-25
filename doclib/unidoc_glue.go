package doclib

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/unidoc/unidoc/common"
	"github.com/unidoc/unidoc/common/license"
	"github.com/unidoc/unidoc/pdf/extractor"
	pdf "github.com/unidoc/unidoc/pdf/model"
)

var (
	Debug bool
	Trace bool
	// ExposeErrors can be set to true to not recover from errors in library functions.
	ExposeErrors bool
)

const (
	licenseKey = ``
	company    = "..."
)

// init sets up UniDoc licensing and logging.
func init() {
	err := license.SetLicenseKey(licenseKey, company)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading UniDoc license: %v\n", err)
	}
	pdf.SetPdfCreator("PDF Search")

	flag.BoolVar(&Debug, "d", false, "Print debugging information.")
	flag.BoolVar(&Trace, "e", false, "Print detailed debugging information.")
	if Trace {
		Debug = true
	}
	flag.BoolVar(&ExposeErrors, "x", ExposeErrors, "Don't recover from library panics.")
}

func SetLogging() {
	if Trace {
		common.SetLogger(common.NewConsoleLogger(common.LogLevelTrace))
	} else if Debug {
		common.SetLogger(common.NewConsoleLogger(common.LogLevelDebug))
	} else {
		common.SetLogger(common.NewConsoleLogger(common.LogLevelInfo))
	}
	common.Log.Info("Debug=%t Trace=%t", Debug, Trace)
}

// PdfOpenFile opens PDF file `inPath` and attempts to handle null encryption schemes.
func PdfOpenFile(inPath string, lazy bool) (*pdf.PdfReader, error) {

	f, err := os.Open(inPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return PdfOpenReader(f, lazy)
}

func PdfOpenReader(f io.ReadSeeker, lazy bool) (*pdf.PdfReader, error) {

	var pdfReader *pdf.PdfReader
	var err error
	if lazy {
		pdfReader, err = pdf.NewPdfReaderLazy(f)
	} else {
		pdfReader, err = pdf.NewPdfReader(f)
	}
	if err != nil {
		return nil, err
	}

	isEncrypted, err := pdfReader.IsEncrypted()
	if err != nil {
		panic(err)
		return nil, err
	}
	if isEncrypted {
		_, err = pdfReader.Decrypt([]byte(""))
		if err != nil {
			panic(err)
			return nil, err
		}
	}
	return pdfReader, nil
}

// PdfOpenDescribe returns numPages, width, height for PDF file `inPath`.
// Width and height are in mm.
func PdfOpenDescribe(inPath string) (numPages int, width, height float64, err error) {
	pdfReader, err := PdfOpenFile(inPath, true)
	if err != nil {
		return 0, 0.0, 0.0, err
	}
	return Describe(pdfReader)
}

// Describe returns numPages, width, height for the PDF in `pdfReader`.
// Width and height are in mm.
func Describe(pdfReader *pdf.PdfReader) (numPages int, width, height float64, err error) {
	pageSizes, err := pageSizeListMm(pdfReader)
	if err != nil {
		return
	}
	numPages = len(pageSizes)
	width, height = DocPageSize(pageSizes)
	return
}

// DocPageSize returns the width and height of a document whose page sizes are `pageSizes`.
// This is a single source of truth for our definition of document page size.
// Currently the document width is defined as the longest page width in the document.
func DocPageSize(pageSizes [][2]float64) (w, h float64) {
	for _, wh := range pageSizes {
		if wh[0] > w {
			w = wh[0]
		}
		if wh[1] > h {
			h = wh[1]
		}
	}
	return
}

// pageSizeListMm returns a slice of the pages sizes for the pages `pdfReader`.
// width and height are in mm.
func pageSizeListMm(pdfReader *pdf.PdfReader) (pageSizes [][2]float64, err error) {
	numPages, err := pdfReader.GetNumPages()
	if err != nil {
		return
	}

	for i := 0; i < numPages; i++ {
		pageNum := i + 1
		page := pdfReader.PageList[i]
		common.Log.Trace("==========================================================")
		common.Log.Trace("page %d", pageNum)
		var w, h float64
		w, h, err = PageSizeMm(page)
		if err != nil {
			return
		}
		size := [2]float64{w, h}
		pageSizes = append(pageSizes, size)
	}

	return
}

// PageSizeMm returns the width and height of `page` in mm.
func PageSizeMm(page *pdf.PdfPage) (width, height float64, err error) {
	width, height, err = PageSizePt(page)
	return PointToMM(width), PointToMM(height), err
}

// PageSizePt returns the width and height of `page` in points.
func PageSizePt(page *pdf.PdfPage) (width, height float64, err error) {
	b, err := page.GetMediaBox()
	if err != nil {
		return 0, 0, err
	}
	return b.Urx - b.Llx, b.Ury - b.Lly, nil
}

// ExtractPageText returns the text on page `page`.
func ExtractPageText(page *pdf.PdfPage) (string, error) {
	textList, err := ExtractPageTextObject(page)
	if err != nil {
		return "", err
	}
	return textList.ToText(), nil
}

// ExtractPageTextLocation returns the locations of text on page `page`.
func ExtractPageTextLocation(page *pdf.PdfPage) (string, []extractor.TextLocation, error) {
	textList, err := ExtractPageTextObject(page)
	if err != nil {
		return "", nil, err
	}
	text, locations := textList.ToTextLocation()
	return text, locations, nil
}

// ExtractPageTextObject returns the PageText on page `page`.
// PageText is an opaque UniDoc struct that describes the text marks on a PDF page.
// extractDocPages uses UniDoc to extract the text from all pages in PDF file `inPath` as a slice
// of PdfPage.
func ExtractPageTextObject(page *pdf.PdfPage) (*extractor.PageText, error) {
	ex, err := extractor.New(page)
	if err != nil {
		return nil, err
	}
	pageText, _, _, err := ex.ExtractPageText()
	return pageText, err
}

// ProcessPDFPagesFile runs `processPage` on every page in PDF file `inPath`.
// It can recover from errors in the libraries it calls if RecoverErrors is true.
func ProcessPDFPagesFile(inPath string, processPage func(pageNum uint32, page *pdf.PdfPage) error) error {
	rs, err := os.Open(inPath)
	if err != nil {
		return err
	}
	defer rs.Close()
	return ProcessPDFPagesReader(inPath, rs, processPage)
}

func ProcessPDFPagesReader(inPath string, rs io.ReadSeeker,
	processPage func(pageNum uint32, page *pdf.PdfPage) error) error {

	var err error
	if !ExposeErrors {
		defer func() {
			if r := recover(); r != nil {
				common.Log.Error("Recover: %q r=%#v", inPath, r)
				switch t := r.(type) {
				case error:
					err = t
				case string:
					err = errors.New(t)
				}
			}
		}()
	}

	pdfReader, err := PdfOpenReader(rs, true)
	if err != nil {
		common.Log.Error("processPDFPages: Could not open inPath=%q. err=%v", inPath, err)
		return err
	}

	err = processPDFPages(inPath, pdfReader, processPage)
	return err
}

// processPDFPages runs `processPage` on every page in PDF file `inPath`.
func processPDFPages(inPath string, pdfReader *pdf.PdfReader,
	processPage func(pageNum uint32, page *pdf.PdfPage) error) error {

	numPages, err := pdfReader.GetNumPages()
	if err != nil {
		return err
	}

	common.Log.Debug("processPDFPages: inPath=%q numPages=%d", inPath, numPages)

	for pageNum := uint32(1); pageNum < uint32(numPages); pageNum++ {
		page, err := pdfReader.GetPage(int(pageNum))
		if err != nil {
			return err
		}
		if err = processPage(pageNum, page); err != nil {
			return err
		}
	}
	return nil
}
