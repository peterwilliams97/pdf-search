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

   func (hs *LocationsState) ReadLocations(doc uint64, page uint32) ([]DocPageLocations, error)

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
	hashPath     map[string]string // {file hash: file path}
	updateTime   time.Time
	locationsDir string
}

// FileDesc describes a PDF file.
type FileDesc struct {
	InPath string // Full path to PDF file.
	Hash   string // SHA-256 hash of file contents
	SizeMB float64
}

// OpenLocationsState loads indexe from an existing locations directory `root`.
func OpenLocationsState(root string) (*LocationsState, error) {
	hs := LocationsState{root: root}
	filename := hs.fileListPath()
	fileList, err := loadFileList(filename)
	if err != nil {
		return nil, err
	}
	hashIndex := map[string]uint64{}
	hashPath := map[string]string{}
	for i, hip := range fileList {
		hashIndex[hip.Hash] = uint64(i)
		hashPath[hip.Hash] = hip.InPath
	}

	return &LocationsState{
		root:      root,
		fileList:  fileList,
		hashIndex: hashIndex,
		hashPath:  hashPath,
	}, nil
}

func (hs *LocationsState) ExtractDocPagesLookup2(inPath string) ([]Loc2Page, error) {
	fd, err := CreateFileDesc(inPath)
	if err != nil {
		return nil, err
	}
	docIdx, p, exists := hs.AddFile(fd)
	if exists {
		common.Log.Error("ExtractDocPagesLookup2: %q is the same PDF as %q. Ignoring", inPath, p)
		return nil, errors.New("duplicate PDF")
	}

	ls, err := hs.CreateLocationsStore(docIdx)
	if err != nil {
		return nil, err
	}

	var docPages []Loc2Page

	err = ProcessPDFPages(inPath, func(pageNum int, page *pdf.PdfPage) error {
		text, locations, err := ExtractPageTextLocation(page)
		if err != nil {
			common.Log.Error("ExtractDocPagesLookup2: ExtractPageTextLocation failed. inPath=%q pageNum=%d err=%v",
				inPath, pageNum, err)
			return err
		}
		if text == "" {
			return nil
		}

		pageIdx, err := ls.AddPage(locations)
		if err != nil {
			return err
		}
		docPages = append(docPages, Loc2Page{
			Doc:      docIdx,
			Page:     pageIdx,
			Contents: text,
		})
		if len(docPages)%100 == 99 {
			common.Log.Info("\tpageNum=%d docPages=%d %q", pageNum, len(docPages), inPath)
		}
		return nil
	})
	if err != nil {
		return docPages, err
	}
	err = ls.SaveLocations(hash, docPages)
	return docPages, err
}

type Loc2Page struct {
	Doc      uint64 // Doc index.
	Page     uint32 // Page index.
	Contents string // Page text. !@#$ -> Text
}

func CreateFileDesc(inPath string) (FileDesc, error) {
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

// fileListPath is the path where hs.fileList is stored on disk.
func (hs *LocationsState) fileListPath() string {
	return filepath.Join(hs.root, "file_list.json")
}

// locationsPath returns the file path to the locations file for PDF with hash `hash`.
func (hs *LocationsState) locationsPath(hash string) string {
	return filepath.Join(hs.locationsDir, hash)
}

func (hs *LocationsState) createIfNecessary() error {
	common.Log.Info("createIfNecessary: 1 locationsDir=%q", hs.locationsDir)
	if hs.locationsDir != "" {
		return nil
	}
	hs.locationsDir = filepath.Join(hs.root, "locations")
	common.Log.Info("createIfNecessary: 2 locationsDir=%q", hs.locationsDir)
	err := MkDir(hs.locationsDir)
	common.Log.Info("createIfNecessary: err=%v", err)
	return err
}

// ReadLocations returns the DocPageLocations for the PDF page given by `doc`, `page`.
func (hs *LocationsState) ReadLocations(doc uint64, page uint32) ([]DocPageLocations, error) {
	hash := hs.indexHash[doc]
	locationsPath := hs.locationsPath(hash)
	locationsFile, err := os.Open(locationsPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not open locations file %q.\n", locationsPath)
		panic(err)
	}
	defer locationsFile.Close()

	var dplList []DocPageLocation
	for _, l := range docPages {
		// dpl := l.ToDocPageLocations()
		dpl, err := serial.RReadDocPageLocations(locationsFile)
		if err != nil {
			panic(err)
		}
		dplList = append(dplList, dpl)
	}
	return dplList, nil
}

// addFile adds PDF file `fd` to `hs`. fileList.
func (hs *LocationsState) AddFile(fd FileDesc) (uint64, string, bool) {
	hash := fd.Hash
	idx, ok := hs.hashIndex[hash]
	if ok {
		return idx, hs.hashPath[hash], true
	}
	hs.fileList = append(hs.fileList, fd)
	idx = uint64(len(hs.fileList) - 1)
	hs.hashIndex[hash] = idx
	hs.hashPath[hash] = inPath
	dt := time.Since(hs.updateTime)
	// fmt.Fprintf(os.Stderr, "*00 Flush: %s %.1f sec\n", hs.updateTime, dt.Seconds())
	if dt.Seconds() > hashUpdatePeriodSec {
		hs.Flush()
		fmt.Fprintf(os.Stderr, "*** Flush: %s (%.1f sec) %d elements\n",
			hs.updateTime, dt.Seconds(), idx)
		hs.updateTime = time.Now()
	}
	return idx, inPath, false
}

func (hs *LocationsState) Flush() error {
	return saveFileList(hs.fileListPath(), hs.fileList)
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

func (l Loc2Page) ID() string {
	return fmt.Sprintf("%04X.%d", l.Doc, l.Page)
}

func (l Loc2Page) ToPdfPage() PdfPage {
	return PdfPage{ID: l.ID(), Contents: l.Contents}
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

//
//
//
type IndexEntry struct {
	offset uint32
	size   uint32
	check  uint32
}

type LocationsStore struct {
	hs       *LocationsState
	dataFile *os.File
	index    []IndexEntry
}

func (hs *LocationsState) CreateLocationsStore(docIdx uint64) (*LocationsStore, error) {
	hash := hs.FileList[docIdx].Hash
	locPath := hs.locationsPath(hash)
	dataPath := filepath.Join(locPath, "dat")
	f, err := os.Create(dataPath)
	if err != nil {
		return nil, err
	}
	return &LocationsStore{
		hs:       hs,
		dataFile: f,
	}, nil
}

func (hs *LocationsState) OpenLocationsStore(storeDir string) (*LocationsStore, error) {
	dataPath := filepath.Join(storeDir, "data")
	indexPath := filepath.Join(storeDir, "index")
	buf, err := ioutil.ReadFile(indexPath)
	if err != nil {
		return nil, err
	}
	var index []IndexEntry
	if err := json.Unmarshal(buf, &index); err != nil {
		return nil, err
	}
	f, err := os.Open(dataPath)
	if err != nil {
		return nil, err
	}
	return LocationsStore{
		storeDir: storeDir,
		dataFile: f,
		index:    index,
	}, nil
}

func (ls *LocationsStore) Close(hash string, docPages []Loc2Page) error {

	// if err := hs.createIfNecessary(); err != nil {
	// 	return err
	// }
	locationsPath := filepath.Join(hs.locationsDir, hash)
	locationsFile, err := os.Create(locationsPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not create locations file %q.\n", locationsPath)
		panic(err)
	}
	defer locationsFile.Close()

	for _, l := range docPages {
		dpl := l.ToDocPageLocations()
		if err := serial.WriteDocPageLocations(locationsFile, dpl); err != nil {
			panic(err)
		}
	}

	indexPath := filepath.Join(locPath, "idx")

	return nil
}

func (ls *LocationsStore) Close() error {
	for _, l := range ls.docPages {
		dpl := l.ToDocPageLocations()
		if err := serial.WriteDocPageLocations(locationsFile, dpl); err != nil {
			panic(err)
		}
	}

	if buf, err := json.MarshalIndent(ls.index, "", "\t"); err != nil {
		return err
	}
	return ls.dataFile.Close()
}

func (ls *LocationsStore) AddPage(dpl serial.DocPageLocations) (uint32, error) {
	b := flatbuffers.NewBuilder(0)
	buf := serial.MakeDocPageLocations(b, dpl)
	check := crc32.ChecksumIEEE(buf) // uint32
	offset, err := ls.dataFile.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}

	e := IndexEntry{
		offset: uint32(offset),
		size:   uint32(len(buf)),
		check:  check,
	}

	if _, err := ls.dataFile.Write(buf); err != nil {
		return 0, err
	}
	ls.index = append(ls.index, e)
	return uint32(len(ls.index) - 1), nil
}

func (ls *LocationsStore) ReadDocPageLocations2(idx int) (serial.DocPageLocations, error) {
	e := ls.index[idx]
	ls.dataFile.Seek(io.SeekStart, int(e.offset))
	buf := make([]byte, e.size)
	if _, err := ls.dataFile.Read(buf); err != nil {
		return serial.DocPageLocations{}, err
	}
	if crc32.ChecksumIEEE(buf) != e.check {
		return serial.DocPageLocations{}, errors.New("bad checksum")
	}
	return serial.ReadDocPageLocations(buf)
}
