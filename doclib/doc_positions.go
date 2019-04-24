package doclib

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/peterwilliams97/pdf-search/serial"
	"github.com/unidoc/unidoc/common"
	"github.com/unidoc/unidoc/pdf/extractor"
)

// DocPositions tracks the data that is used to index a PDF file.
type DocPositions struct {
	lState  *PositionsState                    // State of whole store.
	inPath  string                             // Path of input PDF file.
	docIdx  uint64                             // Index into lState.fileList.
	pageDpl map[uint32]serial.DocPageLocations // !@#$ Debugging
	*docPersist
	*docData
}

// docPersist tracks the info for indexing a PDF file on disk.
type docPersist struct {
	dataFile    *os.File   // Positions are stored in this file.
	spans       []byteSpan // Indexes into `dataFile`. These is a byteSpan per page.
	dataPath    string     // Path of `dataFile`.
	spansPath   string     // Path where `spans` is saved.
	textDir     string     // !@#$ Debugging
	pageDplPath string
}

// docData is the data for indexing a PDF file in memory.
type docData struct {
	// loc       serial.DocPageLocations
	pageNums  []uint32
	pageTexts []string
}

// byteSpan is the location of the bytes of a DocPageLocations in a data file.
// The span is over [Offset, Offset+Size).
// There is one byteSpan (corresponding to a DocPageLocations) per page.
type byteSpan struct {
	Offset  uint32 // Offset in the data file for the DocPageLocations for a page.
	Size    uint32 // Size of the DocPageLocations in the data file.
	Check   uint32 // CRC checksum for the DocPageLocations data.
	PageNum uint32 // PDF page number.
}

func (d DocPositions) String() string {
	parts := []string{fmt.Sprintf("%q docIdx=%d mem=%t",
		filepath.Base(d.inPath), d.docIdx, d.docData != nil)}
	if d.docPersist != nil {
		parts = append(parts, d.docPersist.String())
	}
	if d.docData != nil {
		parts = append(parts, d.docData.String())
	}
	if (d.docPersist != nil) == (d.docData != nil) {
		parts = append(parts, "<BAD>")
	}
	return fmt.Sprintf("DocPositions{%s}", strings.Join(parts, "\n"))
}

func (d DocPositions) Len() int {
	return len(d.pageNums)
}

func (d docPersist) String() string {
	var parts []string
	for i, span := range d.spans {
		parts = append(parts, fmt.Sprintf("\t%2d: %v", i+1, span))
	}
	return fmt.Sprintf("docPersist{%s}", strings.Join(parts, "\n"))
}

func (d docData) String() string {
	np := len(d.pageNums)
	nt := len(d.pageTexts)
	bad := ""
	if np != nt {
		bad = " [BAD]"
	}
	return fmt.Sprintf("docData{pageNums=%d pageTexts=%d%s}", np, nt, bad)
}

func (d DocPositions) isMem() bool {
	persist := d.docPersist != nil
	mem := d.docData != nil
	if persist == mem {
		panic(fmt.Errorf("d=%s should not happen\n%#v", d, d))
	}
	return mem
}

// openDoc() opens `lDoc` for reading. In a persistent `lDoc`, necessary files are opened.
func (lDoc *DocPositions) openDoc() error {
	if lDoc.isMem() {
		return nil
	}

	// Persistent case.
	f, err := os.Open(lDoc.dataPath)
	if err != nil {
		return err
	}
	lDoc.dataFile = f

	b, err := ioutil.ReadFile(lDoc.spansPath)
	if err != nil {
		return err
	}
	var spans []byteSpan
	if err := json.Unmarshal(b, &spans); err != nil {
		return err
	}
	lDoc.spans = spans

	return nil
}

func (lDoc *DocPositions) Save() error {
	if lDoc.isMem() {
		return nil
	}
	b, err := json.MarshalIndent(lDoc.spans, "", "\t")
	if err != nil {
		return err
	}
	return ioutil.WriteFile(lDoc.spansPath, b, 0666)
}

func (lDoc *DocPositions) Close() error {
	if lDoc.isMem() {
		return nil
	}
	// Persistent case.
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
	var pageNums []uint32
	for p := range lDoc.pageDpl {
		pageNums = append(pageNums, uint32(p))
	}
	sort.Slice(pageNums, func(i, j int) bool { return pageNums[i] < pageNums[j] })
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
	}
	return ioutil.WriteFile(lDoc.pageDplPath, data, 0666)
}

// AddDocPage adds a page (with page number `pageNum` and contents `dpl`) to `lDoc`.
// !@#$ Remove `text` param.
func (lDoc *DocPositions) AddDocPage(pageNum uint32, dpl serial.DocPageLocations, text string) (uint32, error) {
	if pageNum == 0 {
		panic("pageNum = 0 should never happen")
	}
	lDoc.pageDpl[pageNum] = dpl // !@#$

	if lDoc.isMem() {
		lDoc.docData.pageTexts = append(lDoc.docData.pageTexts, text)
		lDoc.docData.pageNums = append(lDoc.docData.pageNums, pageNum)
		return uint32(len(lDoc.docData.pageNums)) - 1, nil
	}
	return lDoc.addDocPagePersist(pageNum, dpl, text)
}

func (lDoc *DocPositions) addDocPagePersist(pageNum uint32, dpl serial.DocPageLocations,
	text string) (uint32, error) {

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

	filename := lDoc.GetTextPath(pageIdx)
	err = ioutil.WriteFile(filename, []byte(text), 0644)
	if err != nil {
		return 0, err
	}
	return pageIdx, err
}

func (lDoc *DocPositions) ReadPageText(pageIdx uint32) (string, error) {
	if lDoc.isMem() {
		return lDoc.pageTexts[pageIdx], nil
	}
	return lDoc.readPersistedPageText(pageIdx)
}

func (lDoc *DocPositions) readPersistedPageText(pageIdx uint32) (string, error) {
	filename := lDoc.GetTextPath(pageIdx)
	b, err := ioutil.ReadFile(filename)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// ReadPagePositions returns the DocPageLocations of the text on the `pageIdx` (0-offset)
// returned text in document `lDoc`.
func (lDoc *DocPositions) ReadPagePositions(pageIdx uint32) (uint32, serial.DocPageLocations, error) {
	if lDoc.isMem() {
		if pageIdx >= uint32(len(lDoc.pageNums)) {
			return 0, serial.DocPageLocations{}, fmt.Errorf("Bad pageIdx=%d lDoc=%s", pageIdx, lDoc)
		}
		common.Log.Debug("ReadPagePositions: pageIdx=%d pageNums=%v", pageIdx, lDoc.pageNums)
		pageNum := lDoc.pageNums[pageIdx]
		if pageNum == 0 {
			return 0, serial.DocPageLocations{}, fmt.Errorf("No pageNum. lDoc=%s", lDoc)
		}
		dpl := lDoc.pageDpl[pageNum]
		return pageNum, dpl, nil
	}
	return lDoc.readPersistedPagePositions(pageIdx)
}

func (lDoc *DocPositions) readPersistedPagePositions(pageIdx uint32) (
	uint32, serial.DocPageLocations, error) {

	e := lDoc.spans[pageIdx]
	if e.PageNum == 0 {
		return 0, serial.DocPageLocations{}, fmt.Errorf("Bad span pageIdx=%d e=%+v", pageIdx, e)
	}

	offset, err := lDoc.dataFile.Seek(int64(e.Offset), io.SeekStart)
	if err != nil || uint32(offset) != e.Offset {
		common.Log.Error("ReadPagePositions: Seek failed e=%+v offset=%d err=%v",
			e, offset, err)
		return 0, serial.DocPageLocations{}, err
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

func CreateFileDesc(inPath string, rs io.ReadSeeker) (FileDesc, error) {
	if rs != nil {
		size, hash, err := ReaderSizeHash(rs)
		return FileDesc{
			InPath: inPath,
			Hash:   hash,
			SizeMB: float64(size) / 1024.0 / 1024.0,
		}, err
	}
	hash, err := FileHash(inPath)
	if err != nil {
		return FileDesc{}, err
	}
	size, err := FileSize(inPath)
	if err != nil {
		return FileDesc{}, err
	}
	return FileDesc{
		InPath: inPath,
		Hash:   hash,
		SizeMB: float64(size) / 1024.0 / 1024.0,
	}, nil
}

// DocPageText contains doc:page indexes, the PDF page number and the text extracted from from a PDF
// page.
type DocPageText struct {
	DocIdx  uint64 // Doc index (0-offset) into PositionsState.fileList .
	PageIdx uint32 // Page index (0-offset) into DocPositions.index .
	PageNum int    // Page number in PDF file (1-offset)
	Text    string // Extracted page text.
}

// ToSerialTextLocation converts extractor.TextLocation `loc` to a more compact serial.TextLocation.
func ToSerialTextLocation(loc extractor.TextLocation) serial.TextLocation {
	b := loc.BBox
	return serial.TextLocation{
		Start: uint32(loc.Offset),
		Llx:   float32(b.Llx),
		Lly:   float32(b.Lly),
		Urx:   float32(b.Urx),
		Ury:   float32(b.Ury),
	}
}
