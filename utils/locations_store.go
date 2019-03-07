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
          <hash2>.dat
          <hash2>.idx
          ...
*/

const hashUpdatePeriodSec = 1.0

// PositionsState is the global state of a writer or reader to the position indexes saved to disk.
type PositionsState struct {
	root         string            // top level directory of the data saved to disk
	fileList     []FileDesc        // list of file entries
	hashIndex    map[string]uint64 // {file hash: index into fileList}
	indexHash    map[uint64]string // {index into fileList: file hash}
	hashPath     map[string]string // {file hash: file path}
	updateTime   time.Time         // Time of last Flush()
	positionsDir string            // <root>/positions
}

// OpenPositionsState loads indexes from an existing locations directory `root` or creates one if it
// doesn't exist.
func OpenPositionsState(root string) (*PositionsState, error) {
	lState := PositionsState{root: root}
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

	return &PositionsState{
		root:      root,
		fileList:  fileList,
		hashIndex: hashIndex,
		indexHash: indexHash,
		hashPath:  hashPath,
	}, nil
}

func (lState *PositionsState) ExtractDocPagePositions(inPath string) ([]DocPageText, error) {
	fd, err := CreateFileDesc(inPath)
	if err != nil {
		panic(err)
		return nil, err
	}
	docIdx, p, exists := lState.AddFile(fd)
	if exists {
		common.Log.Error("ExtractDocPagePositions: %q is the same PDF as %q. Ignoring", inPath, p)
		panic(err)
		return nil, errors.New("duplicate PDF")
	}

	lDoc, err := lState.CreatePositionsDoc(docIdx)
	if err != nil {
		panic(err)
		return nil, err
	}

	var docPages []DocPageText

	err = ProcessPDFPages(inPath, func(pageNum int, page *pdf.PdfPage) error {
		text, locations, err := ExtractPageTextLocation(page)
		if err != nil {
			common.Log.Error("ExtractDocPagePositions: ExtractPageTextLocation failed. inPath=%q pageNum=%d err=%v",
				inPath, pageNum, err)
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

		pageIdx, err := lDoc.AddPage(dpl)
		if err != nil {
			panic(err)
			return err
		}
		docPages = append(docPages, DocPageText{
			DocIdx:  docIdx,
			PageIdx: pageIdx,
			PageNum: pageNum,
			Text:    text,
		})
		if len(docPages)%100 == 99 {
			common.Log.Info("\tpageNum=%d docPages=%d %q", pageNum, len(docPages), inPath)
		}
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

// addFile adds PDF file `fd` to `lState`. fileList.
func (lState *PositionsState) AddFile(fd FileDesc) (uint64, string, bool) {
	hash := fd.Hash
	idx, ok := lState.hashIndex[hash]
	if ok {
		return idx, lState.hashPath[hash], true
	}
	lState.fileList = append(lState.fileList, fd)
	idx = uint64(len(lState.fileList) - 1)
	lState.hashIndex[hash] = idx
	lState.indexHash[idx] = hash
	lState.hashPath[hash] = fd.InPath
	dt := time.Since(lState.updateTime)
	// fmt.Fprintf(os.Stderr, "*00 Flush: %s %.1f sec\n", lState.updateTime, dt.Seconds())
	if dt.Seconds() > hashUpdatePeriodSec {
		lState.Flush()
		fmt.Fprintf(os.Stderr, "*** Flush: %s (%.1f sec) %d elements\n",
			lState.updateTime, dt.Seconds(), idx)
		lState.updateTime = time.Now()
	}
	return idx, fd.InPath, false
}

func (lState *PositionsState) Flush() error {
	return saveFileList(lState.fileListPath(), lState.fileList)
}

// fileListPath is the path where lState.fileList is stored on disk.
func (lState *PositionsState) fileListPath() string {
	return filepath.Join(lState.root, "file_list.json")
}

// docPath returns the file path to the positions files for PDF with hash `hash`.
func (lState *PositionsState) docPath(hash string) string {
	return filepath.Join(lState.positionsDir, hash)
}

// createIfNecessary creates `lState`.positionsDir if it doesn't already exist.
// It is called at the start of CreatePositionsDoc() which allows us to avoid creating our directory
// structure until we have successfully extracted the text from a PDF pages.
func (lState *PositionsState) createIfNecessary() error {
	common.Log.Info("createIfNecessary: 1 positionsDir=%q", lState.positionsDir)
	if lState.positionsDir != "" {
		return nil
	}
	lState.positionsDir = filepath.Join(lState.root, "positions")
	common.Log.Info("createIfNecessary: 2 positionsDir=%q", lState.positionsDir)
	err := MkDir(lState.positionsDir)
	common.Log.Info("createIfNecessary: err=%v", err)
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
	dataFile  *os.File        // Positions are stored in this file.
	spans     []byteSpan      // Indexes into `dataFile`. These is a byteSpan per page.
	dataPath  string          // Path of `dataFile`.
	spansPath string          // Path where `spans` is saved
}

// ReadDocPagePositions is inefficient. A DocPositions (a file) is opened and closed to read a page.
func (lState *PositionsState) ReadDocPagePositions(docIdx uint64, pageIdx uint32) (
	serial.DocPageLocations, error) {
	lDoc, err := lState.OpenPositionsDoc(docIdx)
	if err != nil {
		return serial.DocPageLocations{}, err
	}
	defer lDoc.Close()
	return lDoc.ReadDocPagePositions(pageIdx)
}

func (lState *PositionsState) CreatePositionsDoc(docIdx uint64) (*DocPositions, error) {
	lDoc := lState.baseFields(docIdx)

	err := lState.createIfNecessary()
	if err != nil {
		panic(err)
	}
	lDoc.dataFile, err = os.Create(lDoc.dataPath)
	if err != nil {
		panic(err)
		return nil, err
	}
	return lDoc, nil
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
	// var err error
	// spansPath, err = filepath.Abs(spansPath)
	// if err != nil {
	// 	panic(err)
	// }
	// dataPath, err = filepath.Abs(dataPath)
	// if err != nil {
	// 	panic(err)
	// }
	return &DocPositions{
		lState:    lState,
		dataPath:  dataPath,
		spansPath: spansPath,
	}
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

func (lDoc *DocPositions) AddPage(dpl serial.DocPageLocations) (uint32, error) {
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
	return uint32(len(lDoc.spans) - 1), nil
}

// ReadDocPagePositions returns the DocPageLocations of the text on the `pageIdx` (0-offset)
// returned text in document `lDoc`.
func (lDoc *DocPositions) ReadDocPagePositions(pageIdx uint32) (serial.DocPageLocations, error) {
	e := lDoc.spans[pageIdx]
	lDoc.dataFile.Seek(io.SeekStart, int(e.Offset))
	buf := make([]byte, e.Size)
	if _, err := lDoc.dataFile.Read(buf); err != nil {
		return serial.DocPageLocations{}, err
	}
	if crc32.ChecksumIEEE(buf) != e.Check {
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

// DocPageText contains doc,page indexes, the PDF page number and the text extracted from from a PDF
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
		Offset: uint32(loc.Offset),
		Llx:    float32(b.Llx),
		Lly:    float32(b.Lly),
		Urx:    float32(b.Urx),
		Ury:    float32(b.Ury),
	}
}
