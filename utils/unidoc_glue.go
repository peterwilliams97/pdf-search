package utils

import (
	"flag"
	"fmt"
	"os"

	"github.com/unidoc/unidoc/common"
	"github.com/unidoc/unidoc/pdf/extractor"
	pdf "github.com/unidoc/unidoc/pdf/model"
)

var (
	ShowHelp bool
	Debug    bool
	Trace    bool
)

// init sets up UniDoc licensing and logging.
func init() {
	// Make sure to enter a valid license key.
	// Otherwise text is truncated and a watermark added to the text.
	// License keys are available via: https://unidoc.io
	// 	err := license.SetLicenseKey(`
	// -----BEGIN UNIDOC LICENSE KEY-----
	// ...
	// -----END UNIDOC LICENSE KEY-----
	// `, "Customer nae")
	// 	if err != nil {
	// 		fmt.Fprintf(os.Stderr, "Error loading UniDoc license: %v\n", err)
	// 	}
	pdf.SetPdfCreator("PDF Search")

	flag.BoolVar(&ShowHelp, "h", false, "Show this help message.")
	flag.BoolVar(&Debug, "d", false, "Print debugging information.")
	flag.BoolVar(&Trace, "e", false, "Print detailed debugging information.")
	if Trace {
		Debug = true
	}
}

func SetLogging() {
	if Trace {
		common.SetLogger(common.NewConsoleLogger(common.LogLevelTrace))
	} else if Debug {
		common.SetLogger(common.NewConsoleLogger(common.LogLevelDebug))
	} else {
		common.SetLogger(common.NewConsoleLogger(common.LogLevelError))
	}
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
	textList, err := ExtractPageTextList(page)
	if err != nil {
		return "", err
	}
	return textList.ToText(), err
}

// ExtractPageTextList returns the PageText on page `page`.
// PageText is an opaque UniDoc struct that describes the text marks on a PDF page.
func ExtractPageTextList(page *pdf.PdfPage) (*extractor.PageText, error) {
	ex, err := extractor.New(page)
	if err != nil {
		fmt.Printf("ExtractPageTextList: extractor.New failed. err=%v\n", err)
		return nil, err
	}
	pageText, _, _, err := ex.ExtractPageText()
	if err != nil {
		fmt.Printf("ExtractPageTextList: ExtractPageText failed. err=%v\n", err)
	}
	return pageText, err
}
