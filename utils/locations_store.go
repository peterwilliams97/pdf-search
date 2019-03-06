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
   LocationsState is for serializing and accesing DocPageLocations

   Locations are read from disk a page at a time by ReadLocations which returns the
   []DocPageLocations for the PDF page given by `doc` and `page`.

   func (lState *LocationsState) ReadLocations(doc uint64, page uint32) ([]DocPageLocations, error)

   We use this to allow an efficient look up of DocPageLocation of an offset within a page's text.
   1) Look up []DocPageLocations for the PDF page given by `doc` and `page`
   2) Binary search []DocPageLocations to find location for `offset`.

   Persistent storage
   -----------------
   1 data file + 1 index file per document.
   index file is small and contains offsets of pages in data file. It is made up of
     IndexEntry (12 byte data structure)
         offset uint32
         size   uint32
         check  uint32

   <root>/
      file_list.json
      locations/
          <hash1>.dat
          <hash1>.idx
          <hash2>.dat
          <hash2>.idx
          ...
*/

const hashUpdatePeriodSec = 1.0

type LocationsState struct {
	root         string            // top level directory of the data saved to disk
	fileList     []FileDesc        // list of file entries
	hashIndex    map[string]uint64 // {file hash: index into fileList}
	indexHash    map[uint64]string // {index into fileList: file hash}
	hashPath     map[string]string // {file hash: file path}
	updateTime   time.Time
	locationsDir string
}

// OpenLocationsState loads indexe from an existing locations directory `root`.
func OpenLocationsState(root string) (*LocationsState, error) {
	lState := LocationsState{root: root}
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

	return &LocationsState{
		root:      root,
		fileList:  fileList,
		hashIndex: hashIndex,
		indexHash: indexHash,
		hashPath:  hashPath,
	}, nil
}

func (lState *LocationsState) ExtractDocPagesLookup2(inPath string) ([]Loc2Page, error) {
	fd, err := CreateFileDesc(inPath)
	if err != nil {
		panic(err)
		return nil, err
	}
	docIdx, p, exists := lState.AddFile(fd)
	if exists {
		common.Log.Error("ExtractDocPagesLookup2: %q is the same PDF as %q. Ignoring", inPath, p)
		panic(err)
		return nil, errors.New("duplicate PDF")
	}

	lDoc, err := lState.CreateLocationsDoc(docIdx)
	if err != nil {
		panic(err)
		return nil, err
	}

	var docPages []Loc2Page

	err = ProcessPDFPages(inPath, func(pageNum int, page *pdf.PdfPage) error {
		text, locations, err := ExtractPageTextLocation(page)
		if err != nil {
			common.Log.Error("ExtractDocPagesLookup2: ExtractPageTextLocation failed. inPath=%q pageNum=%d err=%v",
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
		docPages = append(docPages, Loc2Page{
			DocIdx:  docIdx,
			PageIdx: pageIdx,
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
func (lState *LocationsState) AddFile(fd FileDesc) (uint64, string, bool) {
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

func (lState *LocationsState) Flush() error {
	return saveFileList(lState.fileListPath(), lState.fileList)
}

// fileListPath is the path where lState.fileList is stored on disk.
func (lState *LocationsState) fileListPath() string {
	return filepath.Join(lState.root, "file_list.json")
}

// locationsPath returns the file path to the locations file for PDF with hash `hash`.
func (lState *LocationsState) locationsPath(hash string) string {
	return filepath.Join(lState.locationsDir, hash)
}

func (lState *LocationsState) createIfNecessary() error {
	common.Log.Info("createIfNecessary: 1 locationsDir=%q", lState.locationsDir)
	if lState.locationsDir != "" {
		return nil
	}
	lState.locationsDir = filepath.Join(lState.root, "locations")
	common.Log.Info("createIfNecessary: 2 locationsDir=%q", lState.locationsDir)
	err := MkDir(lState.locationsDir)
	common.Log.Info("createIfNecessary: err=%v", err)
	return err
}

//
//
//
type IndexEntry struct {
	Offset uint32
	Size   uint32
	Check  uint32
}

type LocationsDoc struct {
	lState    *LocationsState
	dataFile  *os.File
	index     []IndexEntry
	dataPath  string
	indexPath string
}

func (lState *LocationsState) CreateLocationsDoc(docIdx uint64) (*LocationsDoc, error) {
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

// ReadDocPageLocations is inefficient. Document is opened and closed to read a file.
func (lState *LocationsState) ReadDocPageLocations(docIdx uint64, pageIdx uint32) (
	serial.DocPageLocations, error) {
	lDoc, err := lState.OpenLocationsDoc(docIdx)
	if err != nil {
		return serial.DocPageLocations{}, err
	}
	defer lDoc.Close()
	return lDoc.ReadDocPageLocations2(pageIdx)
}

// ReadLocations returns the DocPageLocations for the PDF page given by `doc`, `page`.
// func (lState *LocationsState) ReadLocations(doc uint64, page uint32) ([]serial.DocPageLocations, error) {
// 	hash, ok := lState.indexHash[doc]
// 	if !ok {
// 		panic("XXXX")
// 	}
// 	locationsPath := lState.locationsPath(hash)
// 	locationsFile, err := os.Open(locationsPath)
// 	if err != nil {
// 		fmt.Fprintf(os.Stderr, "Could not open locations file %q.\n", locationsPath)
// 		panic(err)
// 	}
// 	defer locationsFile.Close()

// 	var dplList []serial.DocPageLocation

// 		// dpl := l.ToDocPageLocations()
// 		dpl, err := serial.RReadDocPageLocations(locationsFile)
// 		if err != nil {
// 			panic(err)
// 		}
// 		dplList = append(dplList, dpl)
// 	}
// 	return dplList, nil
// }

func (lState *LocationsState) OpenLocationsDoc(docIdx uint64) (*LocationsDoc, error) {
	lDoc := lState.baseFields(docIdx)

	f, err := os.Open(lDoc.dataPath)
	if err != nil {
		panic(err)
		return nil, err
	}
	lDoc.dataFile = f

	b, err := ioutil.ReadFile(lDoc.indexPath)
	if err != nil {
		return nil, err
	}
	var index []IndexEntry
	if err := json.Unmarshal(b, &index); err != nil {
		return nil, err
	}
	lDoc.index = index

	return lDoc, nil
}

func (lState *LocationsState) baseFields(docIdx uint64) *LocationsDoc {
	hash := lState.fileList[docIdx].Hash
	locPath := lState.locationsPath(hash)
	dataPath := locPath + ".dat"
	indexPath := locPath + ".idx.json"
	var err error
	indexPath, err = filepath.Abs(indexPath)
	if err != nil {
		panic(err)
	}
	dataPath, err = filepath.Abs(dataPath)
	if err != nil {
		panic(err)
	}
	return &LocationsDoc{
		lState:    lState,
		dataPath:  dataPath,
		indexPath: indexPath,
	}
}

func (lDoc *LocationsDoc) Save() error {
	b, err := json.MarshalIndent(lDoc.index, "", "\t")
	if err != nil {
		return err
	}
	return ioutil.WriteFile(lDoc.indexPath, b, 0666)
}

func (lDoc *LocationsDoc) Close() error {
	err := lDoc.Save()
	if err != nil {
		return err
	}
	return lDoc.dataFile.Close()
}

func (lDoc *LocationsDoc) AddPage(dpl serial.DocPageLocations) (uint32, error) {
	b := flatbuffers.NewBuilder(0)
	buf := serial.MakeDocPageLocations(b, dpl)
	check := crc32.ChecksumIEEE(buf) // uint32
	offset, err := lDoc.dataFile.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}

	e := IndexEntry{
		Offset: uint32(offset),
		Size:   uint32(len(buf)),
		Check:  check,
	}

	if _, err := lDoc.dataFile.Write(buf); err != nil {
		return 0, err
	}
	lDoc.index = append(lDoc.index, e)
	return uint32(len(lDoc.index) - 1), nil
}

func (lDoc *LocationsDoc) ReadDocPageLocations2(idx uint32) (serial.DocPageLocations, error) {
	e := lDoc.index[idx]
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
	InPath string // Full path to PDF file.
	Hash   string // SHA-256 hash of file contents
	SizeMB float64
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

type Loc2Page struct {
	DocIdx  uint64 // Doc index.
	PageIdx uint32 // Page index.
	Text    string // Page text. !@#$ -> Text
}

// func (l Loc2Page) ID() string {
// 	return fmt.Sprintf("%04X.%d", l.Doc, l.Page)
// }

// func (l Loc2Page) ToPdfPage() PdfPage {
// 	return PdfPage{ID: l.ID(), Contents: l.Contents}
// }

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
