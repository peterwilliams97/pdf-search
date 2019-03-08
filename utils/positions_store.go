package utils

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/peterwilliams97/pdf-search/serial"
	"github.com/unidoc/unidoc/common"
	"github.com/unidoc/unidoc/pdf/extractor"
	pdf "github.com/unidoc/unidoc/pdf/model"
)

/*
   PositionsState is for serializing and accesing DocPageLocations.

   Positions are read from disk a page at a time by ReadPositions which returns the
   []DocPageLocations for the PDF page given by `doc` and `page`.

   func (lState *PositionsState) ReadPositions(doc uint64, page uint32) ([]DocPageLocations, error)

   We use this to allow an efficient look up of DocPageLocation of an offset within a page's text.
   1) Look up []DocPageLocations for the PDF page given by `doc` and `page`
   2) Binary search []DocPageLocations to find location for `offset`.

   Persistent storage
   -----------------
   1 data file + 1 index file per document.
   index file is small and contains offsets of pages in data file. It is made up of
     byteSpan (12 byte data structure)
         offset uint32
         size   uint32
         check  uint32

   <root>/
      file_list.json
      positions/
          <hash1>.dat
          <hash1>.idx
          <hash1>.pages
              <page1>.txt
              <page2>.txt
              ...
          <hash2>.dat
          <hash2>.idx
          <hash2>.pages
              <page1>.txt
              <page2>.txt
              ...
          ...
*/

const storeUpdatePeriodSec = 60.0

// PositionsState is the global state of a writer or reader to the position indexes saved to disk.
type PositionsState struct {
	root       string            // top level directory of the data saved to disk
	fileList   []FileDesc        // list of file entries
	hashIndex  map[string]uint64 // {file hash: index into fileList}
	indexHash  map[uint64]string // {index into fileList: file hash}
	hashPath   map[string]string // {file hash: file path}
	updateTime time.Time         // Time of last Flush()
}

func (lState PositionsState) positionsDir() string {
	return filepath.Join(lState.root, "positions")
}

// OpenPositionsState loads indexes from an existing locations directory `root` or creates one if it
// doesn't exist.
// When opening for writing, do this to ensure final index is written to disk:
//    lState, err := utils.OpenPositionsState(basePath, forceCreate)
//    defer lState.Flush()
func OpenPositionsState(root string, forceCreate bool) (*PositionsState, error) {
	lState := PositionsState{root: root}
	if forceCreate {
		if err := lState.removePositionsState(); err != nil {
			return nil, err
		}
	}

	filename := lState.fileListPath()
	fileList, err := loadFileList(filename)
	if err != nil {
		return nil, err
	}
	hashIndex := map[string]uint64{}
	indexHash := map[uint64]string{}
	hashPath := map[string]string{}
	for i, hip := range fileList {
		hashIndex[hip.Hash] = uint64(i)
		indexHash[uint64(i)] = hip.Hash
		hashPath[hip.Hash] = hip.InPath
	}
	lState.fileList = fileList
	lState.hashIndex = hashIndex
	lState.indexHash = indexHash
	lState.hashPath = hashPath
	lState.updateTime = time.Now()

	fmt.Fprintf(os.Stderr, "lState=%q %d\n", lState.root, len(lState.fileList))

	return &lState, nil
}

func (lState *PositionsState) ExtractDocPagePositions(inPath string) ([]DocPageText, error) {
	fd, err := CreateFileDesc(inPath)
	if err != nil {
		panic(err)
		return nil, err
	}

	lDoc, err := lState.CreatePositionsDoc(fd)
	if err != nil {
		// panic(err)
		return nil, err
	}

	var docPages []DocPageText

	err = ProcessPDFPages(inPath, func(pageNum int, page *pdf.PdfPage) error {
		text, locations, err := ExtractPageTextLocation(page)
		if err != nil {
			common.Log.Error("ExtractDocPagePositions: ExtractPageTextLocation failed. inPath=%q pageNum=%d err=%v",
				inPath, pageNum, err)
			return nil // !@#$ Skip errors for now
			panic(err)
			return err
		}
		if text == "" {
			return nil
		}

		var dpl serial.DocPageLocations
		for _, loc := range locations {
			dpl.Locations = append(dpl.Locations, ToSerialTextLocation(loc))
		}

		pageIdx, err := lDoc.AddPage(dpl, text)
		if err != nil {
			panic(err)
			return err
		}
		docPages = append(docPages, DocPageText{
			DocIdx:  lDoc.docIdx,
			PageIdx: pageIdx,
			PageNum: pageNum,
			Text:    text,
			// Name:    inPath,
		})
		if len(docPages)%100 == 99 {
			common.Log.Info("\tpageNum=%d docPages=%d %q", pageNum, len(docPages), inPath)
		}
		dp := docPages[len(docPages)-1]
		common.Log.Info("ExtractDocPagePositions: Doc=%d Page=%d locs=%d",
			dp.DocIdx, dp.PageIdx, len(dpl.Locations))

		return nil
	})
	if err != nil {
		return docPages, err
	}
	err = lDoc.Close()
	if err != nil {
		panic(err)
	}
	return docPages, err
}

// addFile adds PDF file `fd` to `lState`.fileList.
// returns: docIdx, inPath, exists
//     docIdx: Index of PDF file in `lState`.fileList.
//     inPath: Path to file. This the first path this file was added to the index with.
//     exists: true if `fd` was already in lState`.fileList.
func (lState *PositionsState) addFile(fd FileDesc) (uint64, string, bool) {
	hash := fd.Hash
	docIdx, ok := lState.hashIndex[hash]
	if ok {
		return docIdx, lState.hashPath[hash], true
	}
	lState.fileList = append(lState.fileList, fd)
	docIdx = uint64(len(lState.fileList) - 1)
	lState.hashIndex[hash] = docIdx
	lState.indexHash[docIdx] = hash
	lState.hashPath[hash] = fd.InPath
	dt := time.Since(lState.updateTime)
	// fmt.Fprintf(os.Stderr, "*00 Flush: %s %.1f sec\n", lState.updateTime, dt.Seconds())
	if dt.Seconds() > storeUpdatePeriodSec {
		lState.Flush()
		lState.updateTime = time.Now()
	}
	return docIdx, fd.InPath, false
}

func (lState *PositionsState) Flush() error {
	dt := time.Since(lState.updateTime)
	docIdx := uint64(len(lState.fileList) - 1)
	fmt.Fprintf(os.Stderr, "*** Flush %3d files (%4.1f sec) %s\n",
		docIdx+1, dt.Seconds(), lState.updateTime)
	return saveFileList(lState.fileListPath(), lState.fileList)
}

// fileListPath is the path where lState.fileList is stored on disk.
func (lState *PositionsState) fileListPath() string {
	return filepath.Join(lState.root, "file_list.json")
}

// removePositionsState removes the PositionsState persistent data in the directory tree under
// `root` from disk.
func (lState *PositionsState) removePositionsState() error {
	if !Exists(lState.root) {
		return nil
	}
	flPath := lState.fileListPath()
	if !Exists(flPath) && !strings.HasPrefix(flPath, "store.") {
		common.Log.Error("%q doesn't appear to a be a PositionsState directory. %q doesn't exist.",
			lState.root, flPath)
		return errors.New("not a PositionsState directory")
	}
	err := RemoveDirectory(lState.root)
	if err != nil {
		common.Log.Error("RemoveDirectory(%q) failed. err=%v", lState.root, err)
	}
	return err
}

// docPath returns the file path to the positions files for PDF with hash `hash`.
func (lState *PositionsState) docPath(hash string) string {
	common.Log.Trace("docPath: %q %s", lState.positionsDir(), hash)
	// if lState.positionsDir == "" {
	// 	panic(hash)
	// }
	return filepath.Join(lState.positionsDir(), hash)
}

// createIfNecessary creates `lState`.positionsDir if it doesn't already exist.
// It is called at the start of CreatePositionsDoc() which allows us to avoid creating our directory
// structure until we have successfully extracted the text from a PDF pages.
func (lState *PositionsState) createIfNecessary() error {
	d := lState.positionsDir()
	common.Log.Trace("createIfNecessary: 1 positionsDir=%q", d)
	if Exists(d) {
		return nil
	}
	// lState.positionsDir = filepath.Join(lState.root, "positions")
	// common.Log.Info("createIfNecessary: 2 positionsDir=%q", lState.positionsDir)
	err := MkDir(d)
	common.Log.Trace("createIfNecessary: err=%v", err)
	return err
}

// byteSpan is the location of the bytes of a DocPageLocations in a data file.
// The span is over [Offset, Offset+Size).
// There is one byteSpan (corresponding to a DocPageLocations) per page.
type byteSpan struct {
	Offset uint32 // Offset in the data file for the DocPageLocations for a page.
	Size   uint32 // Size of the DocPageLocations in the data file.
	Check  uint32 // CRC checksum for the DocPageLocations data
}

// DocPositions tracks the data that is used to index a PDF file.
type DocPositions struct {
	lState    *PositionsState // State of whole store.
	docIdx    uint64
	dataFile  *os.File   // Positions are stored in this file.
	spans     []byteSpan // Indexes into `dataFile`. These is a byteSpan per page.
	dataPath  string     // Path of `dataFile`.
	spansPath string     // Path where `spans` is saved.
	textDir   string
}

// ReadDocPagePositions is inefficient. A DocPositions (a file) is opened and closed to read a page.
func (lState *PositionsState) ReadDocPagePositions(docIdx uint64, pageIdx uint32) (
	serial.DocPageLocations, error) {
	lDoc, err := lState.OpenPositionsDoc(docIdx)
	if err != nil {
		return serial.DocPageLocations{}, err
	}
	defer lDoc.Close()
	return lDoc.ReadPagePositions(pageIdx)
}

// CreatePositionsDoc opens lDoc.dataPath for writing.
func (lState *PositionsState) CreatePositionsDoc(fd FileDesc) (*DocPositions, error) {
	common.Log.Debug("CreatePositionsDoc: lState.positionsDir=%q", lState.positionsDir())
	docIdx, p, exists := lState.addFile(fd)
	if exists {
		common.Log.Error("ExtractDocPagePositions: %q is the same PDF as %q. Ignoring",
			fd.InPath, p)
		// panic(errors.New("duplicate PDF"))
		return nil, errors.New("duplicate PDF")
	}
	lDoc := lState.baseFields(docIdx)

	err := lState.createIfNecessary()
	if err != nil {
		panic(err)
	}
	if strings.HasPrefix(lDoc.dataPath, "9") {
		common.Log.Error("lState.positionsDir=%q", lState.positionsDir())
		panic(lDoc.dataPath)
	}
	if lState.positionsDir() == "" {
		panic("gggg")
	}
	lDoc.dataFile, err = os.Create(lDoc.dataPath)
	if err != nil {
		panic(err)
		return nil, err
	}
	err = MkDir(lDoc.textDir)
	return lDoc, err
}

func (lState *PositionsState) OpenPositionsDoc(docIdx uint64) (*DocPositions, error) {
	lDoc := lState.baseFields(docIdx)

	f, err := os.Open(lDoc.dataPath)
	if err != nil {
		panic(err)
		return nil, err
	}
	lDoc.dataFile = f

	b, err := ioutil.ReadFile(lDoc.spansPath)
	if err != nil {
		return nil, err
	}
	var spans []byteSpan
	if err := json.Unmarshal(b, &spans); err != nil {
		return nil, err
	}
	lDoc.spans = spans

	return lDoc, nil
}

// baseFields populates a DocPositions with the fields that are the same for Open and Create.
func (lState *PositionsState) baseFields(docIdx uint64) *DocPositions {
	hash := lState.fileList[docIdx].Hash
	locPath := lState.docPath(hash)
	dataPath := locPath + ".dat"
	spansPath := locPath + ".idx.json"
	textDir := locPath + ".pages"
	// var err error
	// spansPath, err = filepath.Abs(spansPath)
	// if err != nil {
	// 	panic(err)
	// }
	// dataPath, err = filepath.Abs(dataPath)
	// if err != nil {
	// 	panic(err)
	// }
	dp := DocPositions{
		lState:    lState,
		docIdx:    docIdx,
		dataPath:  dataPath,
		spansPath: spansPath,
		textDir:   textDir,
	}
	common.Log.Debug("baseFields: docIdx=%d dp=%+v", docIdx, dp)
	return &dp
}

func (lDoc *DocPositions) Save() error {
	b, err := json.MarshalIndent(lDoc.spans, "", "\t")
	if err != nil {
		return err
	}
	return ioutil.WriteFile(lDoc.spansPath, b, 0666)
}

func (lDoc *DocPositions) Close() error {
	err := lDoc.Save()
	if err != nil {
		return err
	}
	return lDoc.dataFile.Close()
}

// !@#$ Remove `text` param.
func (lDoc *DocPositions) AddPage(dpl serial.DocPageLocations, text string) (uint32, error) {
	b := flatbuffers.NewBuilder(0)
	buf := serial.MakeDocPageLocations(b, dpl)
	check := crc32.ChecksumIEEE(buf) // uint32
	offset, err := lDoc.dataFile.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}

	span := byteSpan{
		Offset: uint32(offset),
		Size:   uint32(len(buf)),
		Check:  check,
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

	filename := filepath.Join(lDoc.textDir, fmt.Sprintf("%03d.txt", pageIdx))
	err = ioutil.WriteFile(filename, []byte(text), 0644)
	return pageIdx, err
}

// ReadPagePositions returns the DocPageLocations of the text on the `pageIdx` (0-offset)
// returned text in document `lDoc`.
func (lDoc *DocPositions) ReadPagePositions(pageIdx uint32) (serial.DocPageLocations, error) {
	e := lDoc.spans[pageIdx]
	offset, err := lDoc.dataFile.Seek(int64(e.Offset), io.SeekStart)
	if err != nil || uint32(offset) != e.Offset {
		common.Log.Error("ReadPagePositions: Seek failed e=%+v offset=%d err=%v",
			e, offset, err)
		panic("wtf")
	}
	buf := make([]byte, e.Size)
	if _, err := lDoc.dataFile.Read(buf); err != nil {
		return serial.DocPageLocations{}, err
	}
	size := len(buf)
	check := crc32.ChecksumIEEE(buf)
	if check != e.Check {
		common.Log.Error("ReadPagePositions: e=%+v size=%d check=%d", e, size, check)
		panic(errors.New("bad checksum"))
		return serial.DocPageLocations{}, errors.New("bad checksum")
	}
	return serial.ReadDocPageLocations(buf)
}

// FileDesc describes a PDF file.
type FileDesc struct {
	InPath string  // Full path to PDF file.
	Hash   string  // SHA-256 hash of file contents.
	SizeMB float64 // Size of PDF file on disk.
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

func loadFileList(filename string) ([]FileDesc, error) {
	b, err := ioutil.ReadFile(filename)
	if err != nil {
		if !Exists(filename) {
			return nil, nil
		}
		return nil, err
	}
	var fileList []FileDesc
	err = json.Unmarshal(b, &fileList)
	return fileList, err
}

func saveFileList(filename string, fileList []FileDesc) error {
	b, err := json.MarshalIndent(fileList, "", "\t")
	if err != nil {
		return err
	}
	return ioutil.WriteFile(filename, b, 0666)
}

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
	return serial.TextLocation{
		Offset: uint32(loc.Offset),
		Llx:    float32(b.Llx),
		Lly:    float32(b.Lly),
		Urx:    float32(b.Urx),
		Ury:    float32(b.Ury),
	}
}
