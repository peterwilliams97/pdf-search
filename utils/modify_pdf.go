package utils

import (
	"fmt"
	"math"
	"path/filepath"
	"strings"

	"github.com/unidoc/unidoc/common"
	"github.com/unidoc/unidoc/pdf/creator"
	pdf "github.com/unidoc/unidoc/pdf/model"
)

// ExtractList is a list of document:page inputs that are to be combined in a specified order
type ExtractList struct {
	maxPages  int
	sources   []Extract // Source pages in order they will be combined
	sourceSet map[string]bool
	contents  map[string]map[int]pageContent // Pages for each document
	// documentIndex map[string]int
}

func (l ExtractList) String() string {
	parts := []string{fmt.Sprintf("maxPages: %d", l.maxPages)}
	for i, src := range l.sources {
		parts = append(parts, fmt.Sprintf("%6d: %20q:%d", i, filepath.Base(src.inPath), src.pageNum))
	}
	return strings.Join(parts, "\n")
}

type Extract struct {
	inPath  string // Path of PDF that page comes from.
	pageNum int    // Page number (1-offset) of page in source document
}

type pageContent struct {
	// pageNum                 // page number (1-offset) of page in source document
	rects []pdf.PdfRectangle // the rectangles to be drawn on the PDF page
	page  *pdf.PdfPage       // the UniDoc PDF page. Created as needed.
}

// type DocContents struct {
// 	pageNums []int          // page number (1-offset) of page in source document
// 	pages    []*pdf.PdfPage // pages
// }

func (l *ExtractList) AddRect(inPath string, pageNum int, llx, lly, urx, ury float64) {
	pathPage := fmt.Sprintf("%s.%d", inPath, pageNum)
	if !l.sourceSet[pathPage] {
		if len(l.sourceSet) >= l.maxPages {
			common.Log.Info("AddRect: %q:%d len=%d MAX PAGES EXCEEDED", inPath, pageNum)
			return
		}
		l.sourceSet[pathPage] = true
		l.sources = append(l.sources, Extract{inPath, pageNum})
		common.Log.Info("AddRect: %q:%d len=%d", inPath, pageNum, len(l.sourceSet))
	}

	docContent, ok := l.contents[inPath]
	if !ok {
		docContent = map[int]pageContent{}
		l.contents[inPath] = docContent
	}
	pageContent := docContent[pageNum]
	if len(pageContent.rects) >= 3 {
		return
	}
	r := pdf.PdfRectangle{llx, lly, urx, ury}
	pageContent.rects = append(pageContent.rects, r)
	if pageNum == 0 {
		common.Log.Error("inPath=%q pageNum=%d", inPath, pageNum)
		panic("xxxx")
	}
	docContent[pageNum] = pageContent
}

func CreateExtractList(maxPages int) *ExtractList {
	return &ExtractList{
		maxPages:  maxPages,
		contents:  map[string]map[int]pageContent{},
		sourceSet: map[string]bool{},
	}
}

func (l *ExtractList) NumPages() int {
	return len(l.sources)
}

// func (l *ExtractList) AddPage(inPath string, pageNum int) {
// 	// idx, ok := l.documentIndex[inPath]
// 	// if !ok {
// 	// 	idx = len(l.documentIndex)
// 	// 	l.documentIndex[inPath] = idx
// 	// 	// l.docPages[inPath] = []
// 	// }
// 	l.sources = append(l.sources, Extract{inPath, pageNum})
// 	l.docPages[inPath].pageNums = append(l.docPages[inPath].pageNums, pageNum)
// }

func (l *ExtractList) SaveOutputPdf(outPath string) error {
	common.Log.Info("l=%s", *l)
	for inPath, docContents := range l.contents {
		pdfReader, err := PdfOpen(inPath)
		if err != nil {
			common.Log.Error("SaveOutputPdf: Could not open inPath=%q. err=%v", inPath, err)
			panic(err)
			return err
		}
		for pageNum := range docContents {
			page, err := pdfReader.GetPage(pageNum)
			if err != nil {
				common.Log.Error("SaveOutputPdf: Could not get page inPath=%q pageNum=%d. err=%v",
					inPath, pageNum, err)

				panic(err)
				return err
			}
			pageContent := l.contents[inPath][pageNum]
			pageContent.page = page
			l.contents[inPath][pageNum] = pageContent
		}
	}

	common.Log.Info("SaveOutputPdf: outPath=%q", outPath)

	// Make a new PDF creator.
	c := creator.New()

	for i, src := range l.sources {
		docContent, ok := l.contents[src.inPath]
		if !ok {
			panic(fmt.Errorf("%d: %+v", i, src))
		}
		pageContent, ok := docContent[src.pageNum]
		if !ok {
			panic(fmt.Errorf("%d: %+v", i, src))
		}
		if pageContent.page == nil {
			panic(fmt.Errorf("%d: %+v", i, src))
		}
		if err := c.AddPage(pageContent.page); err != nil {
			common.Log.Error("%d: %+v ", i, src)
			return err
		}
		// h := pageContent.page.MediaBox.Ury
		for _, r := range pageContent.rects[:1] {
			common.Log.Info("@@@@ %q:%d %s", filepath.Base(src.inPath), src.pageNum, rectString(r))
			// rect := c.NewRectangle(r.Llx, h-r.Lly, r.Urx-r.Llx, -(r.Ury - r.Lly))
			rect := c.NewRectangle(r.Llx, r.Lly, r.Urx-r.Llx, r.Ury-r.Lly)
			bbox := r
			if math.Abs(bbox.Urx-bbox.Llx) < 1.0 || math.Abs(bbox.Ury-bbox.Lly) < 1.0 {
				panic(fmt.Errorf("bbox=%+v", bbox))
			}
			rect.SetBorderColor(creator.ColorRGBFromHex("#0000ff")) // Red border
			rect.SetBorderWidth(1.0)
			if err := c.Draw(rect); err != nil {
				panic(err)
				return err
			}
		}
	}

	err := c.WriteToFile(outPath)
	if err != nil {
		panic(err)
	}
	return err
}

func rectString(r pdf.PdfRectangle) string {
	return fmt.Sprintf("{llx: %4.1f lly: %4.1f urx: %4.1f ury: %4.1f} %.1f x %.1f",
		r.Llx, r.Lly, r.Urx, r.Ury, r.Urx-r.Llx, r.Ury-r.Lly)
}

// func DrawPdfRectangle(inPath, outPath string, pageNumber int, llx, lly, urx, ury float64) error {
// 	return ModifyPdfPage(inPath, outPath, pageNumber,
// 		func(c *creator.Creator) error {
// 			rect := c.NewRectangle(llx, lly, urx-llx, ury-lly)
// 			return c.Draw(rect)
// 		})
// }

// func ModifyPdfPage(inPath, outPath string, pageNumber int,
// 	processPage func(c *creator.Creator) error) error {

// 	pdfReader, err := PdfOpen(inPath)
// 	if err != nil {
// 		common.Log.Error("ModifyPdfPage: Could not open inPath=%q. err=%v", inPath, err)
// 		return err
// 	}
// 	numPages, err := pdfReader.GetNumPages()
// 	if err != nil {
// 		return err
// 	}

// 	common.Log.Info("ModifyPdfPage: inPath=%q numPages=%d", inPath, numPages)

// 	// Make a new PDF creator.
// 	c := creator.New()

// 	for pageNum := 1; pageNum < numPages; pageNum++ {
// 		page, err := pdfReader.GetPage(pageNum)
// 		if err != nil {
// 			return err
// 		}
// 		err = c.AddPage(page)
// 		if err != nil {
// 			return err
// 		}
// 		if pageNum == pageNumber {
// 			if err = processPage(c); err != nil {
// 				return err
// 			}
// 		}
// 	}

// 	return c.WriteToFile(outPath)
// }
