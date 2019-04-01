package utils

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/blevesearch/bleve"
	"github.com/peterwilliams97/pdf-search/serial"
	"github.com/unidoc/unidoc/common"
	pdf "github.com/unidoc/unidoc/pdf/model"
)

// FileDesc describes a PDF file.
type FileDesc struct {
	InPath string  // Full path to PDF file.
	Hash   string  // SHA-256 hash of file contents.
	SizeMB float64 // Size of PDF file on disk.
}

// IndexPdfs creates a bleve+PositionsState index for `pathList`. If `persistDir` is not empty, the
// index is written to this directory.
func IndexPdfs(pathList []string, persistDir string, forceCreate, allowAppend bool) (
	*PositionsState, bleve.Index, error) {
	fmt.Fprintf(os.Stderr, "Indexing %d PDF files.\n", len(pathList))

	lState, err := OpenPositionsState(persistDir, forceCreate)
	if err != nil {
		return nil, nil, fmt.Errorf("Could not create positions store %q. err=%v", persistDir, err)
	}
	defer lState.Flush()

	var index bleve.Index
	if len(persistDir) == 0 {
		index, err = CreateBleveMemIndex()
		if err != nil {
			return nil, nil, fmt.Errorf("Could not create Bleve memoryindex. err=%v", err)
		}
	} else {
		indexPath := filepath.Join(persistDir, "bleve")
		common.Log.Info("indexPath=%q", indexPath)
		// Create a new Bleve index.
		index, err = CreateBleveIndex(indexPath, forceCreate, allowAppend)
		if err != nil {
			return nil, nil, fmt.Errorf("Could not create Bleve index in %q", indexPath)
		}
	}

	// Add the pages of all the PDFs in `pathList` to `index`.
	for i, inPath := range pathList {
		fmt.Fprintf(os.Stderr, ">> %3d of %d: %q\n", i+1, len(pathList), inPath)
		err := indexDocPagesLoc(index, lState, inPath)
		if err != nil {
			return nil, nil, fmt.Errorf("Could not index file %q", inPath)
		}
		docCount, err := index.DocCount()
		if err != nil {
			return nil, nil, err
		}
		common.Log.Info("Indexed %q. Total %d pages indexed.", inPath, docCount)
	}
	return lState, index, nil
}

type IDText struct {
	ID   string
	Text string
}

// indexDocPagesLoc adds the text of all the pages in PDF file `inPath` to Bleve index `index`.
func indexDocPagesLoc(index bleve.Index, lState *PositionsState, inPath string) error {
	docPages, err := lState.ExtractDocPagePositions(inPath)
	if err != nil {
		fmt.Printf("indexDocPagesLoc: Couldn't extract pages from %q err=%v\n", inPath, err)
		return nil
	}
	fmt.Printf("indexDocPagesLoc: inPath=%q docPages=%d\n", inPath, len(docPages))

	// for _, l := range docPages {
	// 	dpl := l.ToDocPageLocations()
	// 	if err := serial.WriteDocPageLocations(locationsFile, dpl); err != nil {
	// 		return err
	// 	}
	// }

	t0 := time.Now()
	for i, l := range docPages {
		// Don't weigh down the Bleve index with the text bounding boxes.
		id := fmt.Sprintf("%04X.%d", l.DocIdx, l.PageIdx)
		idText := IDText{ID: id, Text: l.Text}

		err = index.Index(id, idText)
		dt := time.Since(t0)
		if err != nil {
			return err
		}
		if i%100 == 0 {
			fmt.Printf("\tIndexed %2d of %d pages in %5.1f sec (%.2f sec/page)\n",
				i+1, len(docPages), dt.Seconds(), dt.Seconds()/float64(i+1))
			fmt.Printf("\tid=%q text=%d\n", id, len(idText.Text))
		}
	}
	dt := time.Since(t0)
	fmt.Printf("\tIndexed %d pages in %.1f sec (%.3f sec/page)\n",
		len(docPages), dt.Seconds(), dt.Seconds()/float64(len(docPages)))
	return nil
}

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
	root       string            // Top level directory of the data saved to disk
	fileList   []FileDesc        // List of file entries
	hashIndex  map[string]uint64 // {file hash: index into fileList}
	indexHash  map[uint64]string // {index into fileList: file hash}
	hashPath   map[string]string // {file hash: file path}
	updateTime time.Time         // Time of last Flush()
}

func (l PositionsState) String() string {
	return fmt.Sprintf("{PositionsState: %q fileList=%d hashIndex=%d indexHash=%d "+
		"hashPath=%d %s}",
		l.root, len(l.fileList), len(l.hashIndex), len(l.indexHash), len(l.hashPath), l.updateTime)
}

func (l PositionsState) Len() int {
	return len(l.fileList)
}

func (l PositionsState) isMem() bool {
	return l.root == ""
}

func (lState PositionsState) indexToPath(idx uint64) (string, bool) {
	hash, ok := lState.indexHash[idx]
	if !ok {
		return "", false
	}
	inPath, ok := lState.hashPath[hash]
	return inPath, ok
}

func (lState PositionsState) positionsDir() string {
	return filepath.Join(lState.root, "positions")
}

// OpenPositionsState loads indexes from an existing locations directory `root` or creates one if it
// doesn't exist.
// When opening for writing, do this to ensure final index is written to disk:
//    lState, err := utils.OpenPositionsState(persistDir, forceCreate)
//    defer lState.Flush()
func OpenPositionsState(root string, forceCreate bool) (*PositionsState, error) {
	lState := PositionsState{
		root:      root,
		hashIndex: map[string]uint64{},
		indexHash: map[uint64]string{},
		hashPath:  map[string]string{},
	}
	if !lState.isMem() {
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
		lState.fileList = fileList

		for i, hip := range fileList {
			lState.hashIndex[hip.Hash] = uint64(i)
			lState.indexHash[uint64(i)] = hip.Hash
			lState.hashPath[hip.Hash] = hip.InPath
		}
	}

	lState.updateTime = time.Now()

	fmt.Fprintf(os.Stderr, "lState=%s\n", lState)

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
			common.Log.Error("ExtractDocPagePositions: ExtractPageTextLocation failed. "+
				"inPath=%q pageNum=%d err=%v", inPath, pageNum, err)
			return nil // !@#$ Skip errors for now
		}
		if text == "" {
			return nil
		}

		var dpl serial.DocPageLocations
		for i, loc := range locations {
			stl := ToSerialTextLocation(loc)
			common.Log.Debug("%d: %s", i, stl)
			dpl.Locations = append(dpl.Locations, stl)
		}

		pageIdx, err := lDoc.AddDocPage(pageNum, dpl, text)
		if err != nil {
			panic(err)
			return err
		}
		// panic("1") // !@#$ CALLED
		if pageNum == 0 {
			panic("qqqq")
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
		common.Log.Debug("ExtractDocPagePositions: Doc=%d Page=%d locs=%d",
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
	if lState.isMem() {
		return nil
	}
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
	if lState.isMem() {
		panic(fmt.Errorf("lState=%s", *lState))
	}
	return filepath.Join(lState.positionsDir(), hash)
}

// createIfNecessary creates `lState`.positionsDir if it doesn't already exist.
// It is called at the start of CreatePositionsDoc() which allows us to avoid creating our directory
// structure until we have successfully extracted the text from a PDF pages.
func (lState *PositionsState) createIfNecessary() error {
	if lState.root == "" {
		panic(fmt.Errorf("lState=%s", *lState))
	}
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

func (lState *PositionsState) ReadDocPageText(docIdx uint64, pageIdx uint32) (string, error) {
	lDoc, err := lState.OpenPositionsDoc(docIdx)
	if err != nil {
		return "", err
	}
	defer lDoc.Close()
	common.Log.Debug("ReadDocPageText: lDoc=%s", lDoc)

	filename := lDoc.GetTextPath(pageIdx)
	b, err := ioutil.ReadFile(filename)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// ReadDocPagePositions is inefficient. A DocPositions (a file) is opened and closed to read a page.
func (lState *PositionsState) ReadDocPagePositions(docIdx uint64, pageIdx uint32) (
	string, uint32, serial.DocPageLocations, error) {

	lDoc, err := lState.OpenPositionsDoc(docIdx)
	if err != nil {
		return "", 0, serial.DocPageLocations{}, err
	}
	defer lDoc.Close()
	common.Log.Debug("lDoc=%s", lDoc)
	pageNum, dpl, err := lDoc.ReadPagePositions(pageIdx)
	return lDoc.inPath, pageNum, dpl, err
}

// CreatePositionsDoc opens lDoc.dataPath for writing.
func (lState *PositionsState) CreatePositionsDoc(fd FileDesc) (*DocPositions, error) {
	common.Log.Debug("CreatePositionsDoc: lState.positionsDir=%q", lState.positionsDir())

	docIdx, p, exists := lState.addFile(fd)
	if exists {
		common.Log.Error("ExtractDocPagePositions: %q is the same PDF as %q. Ignoring",
			fd.InPath, p)
		panic(errors.New("duplicate PDF"))
		return nil, errors.New("duplicate PDF")
	}
	lDoc := lState.baseFields(docIdx)

	if lState.isMem() {
		return lDoc, nil
	}

	err := lState.createIfNecessary()
	if err != nil {
		panic(err)
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

func (lState *PositionsState) GetHashPath(docIdx uint64) (hash, inPath string) {
	hash = lState.indexHash[docIdx]
	inPath = lState.hashPath[hash]
	return hash, inPath
}

func (lState *PositionsState) OpenPositionsDoc(docIdx uint64) (*DocPositions, error) {
	if lState.isMem() {
		panic(fmt.Errorf("lState=%s", *lState))
	}
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
	if int(docIdx) >= len(lState.fileList) {
		common.Log.Error("docIdx=%d lState=%s\n=%#v", docIdx, *lState, *lState)
		panic(fmt.Errorf("docIdx=%d lState=%s", docIdx, *lState))
	}
	inPath := lState.fileList[docIdx].InPath
	hash := lState.fileList[docIdx].Hash

	dp := DocPositions{
		lState:     lState,
		inPath:     inPath,
		docIdx:     docIdx,
		docPersist: &docPersist{pageDpl: map[int]serial.DocPageLocations{}},
	}

	if !lState.isMem() {
		locPath := lState.docPath(hash)
		dp.dataPath = locPath + ".dat"
		dp.spansPath = locPath + ".idx.json"
		dp.textDir = locPath + ".pages"
		dp.pageDplPath = locPath + ".dpl.json"
	}
	common.Log.Debug("baseFields: docIdx=%d dp=%+v", docIdx, dp)
	return &dp
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
