package utils

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/peterwilliams97/pdf-search/serial"
	"github.com/unidoc/unidoc/common"
	"github.com/unidoc/unidoc/pdf/extractor"
)

// byteSpan is the location of the bytes of a DocPageLocations in a data file.
// The span is over [Offset, Offset+Size).
// There is one byteSpan (corresponding to a DocPageLocations) per page.
type byteSpan struct {
	Offset  uint32 // Offset in the data file for the DocPageLocations for a page.
	Size    uint32 // Size of the DocPageLocations in the data file.
	Check   uint32 // CRC checksum for the DocPageLocations data.
	PageNum uint32 // PDF page number.
}

// DocPositions tracks the data that is used to index a PDF file.
type DocPositions struct {
	lState *PositionsState // State of whole store.
	inPath string          // Path of input PDF file.
	docIdx uint64          // Index into lState.fileList.
	*docPersist
	*docData
}

// docPersist tracks the info for indexing a PDF file on disk.
type docPersist struct {
	dataFile    *os.File                        // Positions are stored in this file.
	spans       []byteSpan                      // Indexes into `dataFile`. These is a byteSpan per page.
	dataPath    string                          // Path of `dataFile`.
	spansPath   string                          // Path where `spans` is saved.
	textDir     string                          // !@#$ Debugging
	pageDpl     map[int]serial.DocPageLocations // !@#$ Debugging
	pageDplPath string
}

// docData is the data for indexing a PDF file in memory.
type docData struct {
	loc      serial.DocPageLocations
	textList []string
}

func (d DocPositions) String() string {
	parts := []string{fmt.Sprintf("%q docIdx=%d", filepath.Base(d.inPath), d.docIdx)}
	for i, span := range d.spans {
		parts = append(parts, fmt.Sprintf("\t%2d: %v", i+1, span))
	}
	return fmt.Sprintf("DocPositions{%s}", strings.Join(parts, "\n"))
}

func (d DocPositions) isMem() bool {
	persist := d.docPersist != nil
	mem := d.docData != nil
	if persist == mem {
		panic(fmt.Errorf("d=%s", d))
	}
	return mem
}

func (lDoc *DocPositions) Save() error {
	b, err := json.MarshalIndent(lDoc.spans, "", "\t")
	if err != nil {
		return err
	}
	return ioutil.WriteFile(lDoc.spansPath, b, 0666)
}

func (lDoc *DocPositions) Close() error {
	if err := lDoc.saveJsonDebug(); err != nil {
		return err
	}
	if err := lDoc.Save(); err != nil {
		return err
	}
	return lDoc.dataFile.Close()
}

func (lDoc *DocPositions) saveJsonDebug() error {
	common.Log.Debug("saveJsonDebug: pageDpl=%d pageDplPath=%q",
		len(lDoc.pageDpl), lDoc.pageDplPath)
	var pageNums []int
	for p := range lDoc.pageDpl {
		pageNums = append(pageNums, p)
	}
	sort.Ints(pageNums)
	common.Log.Debug("saveJsonDebug: pageNums=%+v", pageNums)
	var data []byte
	for _, p := range pageNums {
		dpl := lDoc.pageDpl[p]
		dpl.Doc = uint64(lDoc.docIdx)
		dpl.Page = uint32(p)
		b, err := json.MarshalIndent(dpl, "", "\t")
		if err != nil {
			return err
		}
		common.Log.Debug("saveJsonDebug: page %d: %d bytes", p, len(b))
		data = append(data, b...)
		// panic("2")
	}
	// panic("3")
	return ioutil.WriteFile(lDoc.pageDplPath, data, 0666)
}

// AddDocPage adds a page (with page number `pageNum` and contents `dpl`) to `lDoc`.
// !@#$ Remove `text` param.
func (lDoc *DocPositions) AddDocPage(pageNum int, dpl serial.DocPageLocations, text string) (uint32, error) {
	if pageNum == 0 {
		panic("0000")
	}

	lDoc.pageDpl[pageNum] = dpl // !@#$

	b := flatbuffers.NewBuilder(0)
	buf := serial.MakeDocPageLocations(b, dpl)
	check := crc32.ChecksumIEEE(buf) // uint32
	offset, err := lDoc.dataFile.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}

	span := byteSpan{
		Offset:  uint32(offset),
		Size:    uint32(len(buf)),
		Check:   check,
		PageNum: uint32(pageNum),
	}

	if _, err := lDoc.dataFile.Write(buf); err != nil {
		return 0, err
	}

	lDoc.spans = append(lDoc.spans, span)
	pageIdx := uint32(len(lDoc.spans) - 1)

	// pref := text
	// if len(pref) > 40 {
	// 	pref = pref[:40]
	// }
	// fmt.Printf("text=%d %q\n", len(text), pref)

	filename := lDoc.GetTextPath(pageIdx)
	err = ioutil.WriteFile(filename, []byte(text), 0644)
	return pageIdx, err
}

// ReadPagePositions returns the DocPageLocations of the text on the `pageIdx` (0-offset)
// returned text in document `lDoc`.
func (lDoc *DocPositions) ReadPagePositions(pageIdx uint32) (uint32, serial.DocPageLocations, error) {
	if !lDoc.isMem() {
		return lDoc.readPersistedPagePositions(pageIdx)
	}
	return 0, lDoc.docDat.loc, nil
}

func (lDoc *DocPositions) readPersistedPagePositions(pageIdx uint32) (uint32, serial.DocPageLocations, error) {
	e := lDoc.spans[pageIdx]
	if e.PageNum == 0 {
		panic("jjjjj")
	}
	offset, err := lDoc.dataFile.Seek(int64(e.Offset), io.SeekStart)
	if err != nil || uint32(offset) != e.Offset {
		common.Log.Error("ReadPagePositions: Seek failed e=%+v offset=%d err=%v",
			e, offset, err)
		panic("wtf")
	}
	buf := make([]byte, e.Size)
	if _, err := lDoc.dataFile.Read(buf); err != nil {
		return 0, serial.DocPageLocations{}, err
	}
	size := len(buf)
	check := crc32.ChecksumIEEE(buf)
	if check != e.Check {
		common.Log.Error("ReadPagePositions: e=%+v size=%d check=%d", e, size, check)
		panic(errors.New("bad checksum"))
		return 0, serial.DocPageLocations{}, errors.New("bad checksum")
	}
	dpl, err := serial.ReadDocPageLocations(buf)
	return e.PageNum, dpl, err
}

func (lDoc *DocPositions) GetTextPath(pageIdx uint32) string {
	return filepath.Join(lDoc.textDir, fmt.Sprintf("%03d.txt", pageIdx))
}

func CreateFileDesc(inPath string) (FileDesc, error) {
	hash, err := FileHash(inPath)
	if err != nil {
		return FileDesc{}, err
	}
	size := FileSize(inPath)
	return FileDesc{
		InPath: inPath,
		Hash:   hash,
		SizeMB: float64(size) / 1024.0 / 1024.0,
	}, nil
}

// func loadFileList(filename string) ([]FileDesc, error) {
// 	b, err := ioutil.ReadFile(filename)
// 	if err != nil {
// 		if !Exists(filename) {
// 			return nil, nil
// 		}
// 		return nil, err
// 	}
// 	var fileList []FileDesc
// 	err = json.Unmarshal(b, &fileList)
// 	return fileList, err
// }

// func saveFileList(filename string, fileList []FileDesc) error {
// 	b, err := json.MarshalIndent(fileList, "", "\t")
// 	if err != nil {
// 		return err
// 	}
// 	return ioutil.WriteFile(filename, b, 0666)
// }

// type PdfPage struct {
// 	ID       string // Unique identifier. <file hash>.<page number>
// 	Name     string // File name.
// 	Page     int    // Page number.
// 	Contents string // Page text.
// }

// DocPageText contains doc,page indexes, the PDF page number and the text extracted from from a PDF
// page.
type DocPageText struct {
	DocIdx  uint64 // Doc index (0-offset) into PositionsState.fileList .
	PageIdx uint32 // Page index (0-offset) into DocPositions.index .
	PageNum int    // Page number in PDF file (1-offset)
	Text    string // Extracted page text.
	// Name    string // File name. !@#$
}

// ToSerialTextLocation converts extractor.TextLocation `loc` to a more compact serial.TextLocation.
func ToSerialTextLocation(loc extractor.TextLocation) serial.TextLocation {
	b := loc.BBox
	bbox := b
	if loc.Text != "" && loc.Text != " " {
		dx := bbox.Urx - bbox.Llx
		dy := bbox.Ury - bbox.Lly
		if math.Abs(dx) < extractor.MinBBox || math.Abs(dy) < extractor.MinBBox {
			common.Log.Error("bbox=%+v\nloc=%#v", bbox, loc)
			panic(fmt.Errorf("bbox=%+v dx,dy=%.2f,%.2f", bbox, dx, dy))
		}
	}
	return serial.TextLocation{
		Start: uint32(loc.Offset),
		Llx:   float32(b.Llx),
		Lly:   float32(b.Lly),
		Urx:   float32(b.Urx),
		Ury:   float32(b.Ury),
	}
}
