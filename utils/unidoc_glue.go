package utils

import (
	"flag"
	"fmt"
	"os"

	"github.com/unidoc/unidoc/common"
	"github.com/unidoc/unidoc/common/license"
	"github.com/unidoc/unidoc/pdf/extractor"
	pdf "github.com/unidoc/unidoc/pdf/model"
)

var (
	ShowHelp bool
	Debug    bool
	Trace    bool
	// RecoverErrors can be set to true to recover from errors in library functions.
	RecoverErrors bool
)

// init sets up UniDoc licensing and logging.
func init() {
	err := license.SetLicenseKey(`
-----BEGIN UNIDOC LICENSE KEY-----
eyJsaWNlbnNlX2lkIjoiMjA0YWIxMjgtZGY5Yy00ZWE3LTdlM2UtNzJiYTk4OWFhNGZmIiwiY3VzdG9tZXJfaWQiOiJlMDRiODNjZC0zOTYzLTQxNDktNjljOC03MDU0MTM0OWUyMWMiLCJjdXN0b21lcl9uYW1lIjoiUGFwZXJjdXQiLCJ0eXBlIjoiY29tbWVyY2lhbCIsInRpZXIiOiJidXNpbmVzc191bmxpbWl0ZWQiLCJmZWF0dXJlcyI6WyJ1bmlkb2MiLCJ1bmlkb2MtY2xpIl0sImNyZWF0ZWRfYXQiOjE0ODU0NzUxOTksImV4cGlyZXNfYXQiOjE1MTcwMTExOTksImNyZWF0b3JfbmFtZSI6IlVuaURvYyBTdXBwb3J0IiwiY3JlYXRvcl9lbWFpbCI6InN1cHBvcnRAdW5pZG9jLmlvIn0=
+
JYUUjfjjpek96Rh2LoPy4LbWEHT5X46PxLyNkMyF74L/eNeLR55vcvvi2MIUtZBamCbay+YjmqZu5n6IJQWVDrImdC3b7OthoSdGMvfNSjOSuQcoV/mFpkMYin34Uwe7KM6EebzCuX2LF/LTPpdL6iYHtiWxTnF3yZwFqSgJLa8NSSSElfVLidbfQHYJSu52FTcqqWaqIjT51YiZB0Pq54YDP/jS10sRDYDe3sOpI1bfFplYkcdxPX1tK0AQKbvYCDcNbbnoKhk0EZAVSmI+kh5TdKzUn3BpQc7MP+koGrAePc3ddZF6pNzaiW1CJiO7/TmRzQioEq3Rp/h1XYkKXw==
-----END UNIDOC LICENSE KEY-----
`,
		"PaperCut")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading UniDoc license: %v\n", err)
	}
	pdf.SetPdfCreator("PDF Search")

	// flag.BoolVar(&ShowHelp, "h", false, "Show this help message.")
	flag.BoolVar(&Debug, "d", false, "Print debugging information.")
	flag.BoolVar(&Trace, "e", false, "Print detailed debugging information.")
	if Trace {
		Debug = true
	}
	flag.BoolVar(&RecoverErrors, "r", false, "Recover from errors in library functions.")
	fmt.Printf("*** ShowHelp=%t Debug=%t Trace=%t\n", ShowHelp, Debug, Trace)
}

func SetLogging() {
	if Trace {
		common.SetLogger(common.NewConsoleLogger(common.LogLevelTrace))
	} else if Debug {
		common.SetLogger(common.NewConsoleLogger(common.LogLevelDebug))
	} else {
		common.SetLogger(common.NewConsoleLogger(common.LogLevelInfo))
	}
	// common.Log.Error("Error")
	common.Log.Info("ShowHelp=%t Debug=%t Trace=%t", ShowHelp, Debug, Trace)
	// common.Log.Debug("Debug")
	// common.Log.Trace("Trace")
}

// PdfOpen opens PDF file `inPath` and attempts to handle null encryption schemes.
func PdfOpen(inPath string) (*pdf.PdfReader, error) {

	f, err := os.Open(inPath)
	if err != nil {
		panic(err)
		return nil, err
	}
	defer f.Close()

	pdfReader, err := pdf.NewPdfReader(f)
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

// ProcessPDFPages runs `processPage` on every page in PDF file `inPath`.
// It can recover from errors in the libraries it calls if RecoverErrors is true.
func ProcessPDFPages(inPath string, processPage func(pageNum int, page *pdf.PdfPage) error) error {
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
	err = processPDFPages(inPath, processPage)
	return err
}

// processPDFPages runs `processPage` on every page in PDF file `inPath`.
func processPDFPages(inPath string, processPage func(pageNum int, page *pdf.PdfPage) error) error {
	pdfReader, err := PdfOpen(inPath)
	if err != nil {
		common.Log.Error("processPDFPages: Could not open inPath=%q. err=%v", inPath, err)
		return err
	}
	numPages, err := pdfReader.GetNumPages()
	if err != nil {
		return err
	}

	common.Log.Info("processPDFPages: inPath=%q numPages=%d", inPath, numPages)

	for pageNum := 1; pageNum < numPages; pageNum++ {
		page, err := pdfReader.GetPage(pageNum)
		if err != nil {
			return err
		}
		if err = processPage(pageNum, page); err != nil {
			return err
		}
	}
	return nil
}
