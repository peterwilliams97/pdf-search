package doclib

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
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

var ErrRange = errors.New("out of range")

// FileDesc describes a PDF file.
type FileDesc struct {
	InPath string  // Full path to PDF file.
	Hash   string  // SHA-256 hash of file contents.
	SizeMB float64 // Size of PDF file on disk.
}

// IndexPdfFiles creates a bleve+PositionsState index for `pathList`.
// If `persistDir` is not empty, the index is written to this directory.
// If `forceCreate` is true and `persistDir` is not empty, a new directory is always created.
// If `allowAppend` is true and `persistDir` is not empty and a bleve index already exists on disk
// then the bleve index will be appended to.
// `report` is a supplied function that is called to report progress.
// TODO: Remove `allowAppend` argument. Instead always append to a bleve index if it exists and
//      `forceCreate` is not set.
func IndexPdfFiles(pathList []string, persistDir string, forceCreate, allowAppend bool,
	report func(string)) (*PositionsState, bleve.Index, int, error) {

	var rsList []io.ReadSeeker
	for _, inPath := range pathList {
		rs, err := os.Open(inPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Opened %d files\n", len(rsList))
			break
			return nil, nil, 0, err
		}
		defer rs.Close()
		rsList = append(rsList, rs)
	}
	return IndexPdfReaders(pathList, rsList, persistDir, forceCreate, allowAppend, report)
}

// IndexPdfReaders returns a PositionsState and a bleve.Index over the PDF contents read by the
// io.ReaderSeeker's in `rsList`.
// The names of the PDFs are in the corresponding position in `pathList`.
// The inde`persistDir
// If `persist` is false, the index is stored in memory.
// If `persist` is true, the index is stored on disk in `persistDir`.
// `report` is a supplied function that is called to report progress.
func IndexPdfReaders(pathList []string, rsList []io.ReadSeeker, persistDir string, forceCreate,
	allowAppend bool, report func(string)) (*PositionsState, bleve.Index, int, error) {

	common.Log.Info("Indexing %d PDF files.", len(pathList))

	lState, err := OpenPositionsState(persistDir, forceCreate)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("Could not create positions store %q. err=%v", persistDir, err)
	}
	defer lState.Flush()

	var index bleve.Index
	if len(persistDir) == 0 {
		index, err = CreateBleveMemIndex()
		if err != nil {
			return nil, nil, 0, fmt.Errorf("Could not create Bleve memoryindex. err=%v", err)
		}
	} else {
		indexPath := filepath.Join(persistDir, "bleve")
		common.Log.Info("indexPath=%q", indexPath)
		// Create a new Bleve index.
		index, err = CreateBleveIndex(indexPath, forceCreate, allowAppend)
		if err != nil {
			return nil, nil, 0, fmt.Errorf("Could not create Bleve index in %q", indexPath)
		}
	}

	totalPages := 0
	// Add the pages of all the PDFs in `pathList` to `index`.
	for i, inPath := range pathList {
		readerOnly := ""
		if len(rsList) > 0 {
			readerOnly = " (readerOnly)"
		}
		if report != nil {
			report(fmt.Sprintf("%3d of %d: %q%s", i+1, len(pathList), inPath, readerOnly))
		}
		var err error
		if len(rsList) > 0 {
			rs := rsList[i]
			err = indexDocPagesLocReader(index, lState, inPath, rs)
		} else {
			err = indexDocPagesLocFile(index, lState, inPath)
		}
		if err != nil {
			return nil, nil, 0, fmt.Errorf("Could not index file %q", inPath)
		}
		docCount, err := index.DocCount()
		if err != nil {
			return nil, nil, 0, err
		}
		common.Log.Debug("Indexed %q. Total %d pages indexed.", inPath, docCount)
		totalPages += int(docCount)
	}

	return lState, index, totalPages, err
}

type IDText struct {
	ID   string
	Text string
}

// indexDocPagesLocFile adds the text of all the pages in PDF file `inPath` to Bleve index `index`.
func indexDocPagesLocFile(index bleve.Index, lState *PositionsState, inPath string) error {
	rs, err := os.Open(inPath)
	if err != nil {
		return err
	}
	defer rs.Close()
	return indexDocPagesLocReader(index, lState, inPath, rs)
}

// indexDocPagesLocReader updates `index` and `lState` with the text positions of the text in the
// PDF file accessed by `rs`. `inPath` is the name of the PDF file.
func indexDocPagesLocReader(index bleve.Index, lState *PositionsState,
	inPath string, rs io.ReadSeeker) error {

	docPages, err := lState.ExtractDocPagePositionsReader(inPath, rs)
	if err != nil {
		common.Log.Error("indexDocPagesLocReader: Couldn't extract pages from %q err=%v", inPath, err)
		return nil
	}
	common.Log.Debug("indexDocPagesLocReader: inPath=%q docPages=%d", inPath, len(docPages))

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
			common.Log.Debug("\tIndexed %2d of %d pages in %5.1f sec (%.2f sec/page)",
				i+1, len(docPages), dt.Seconds(), dt.Seconds()/float64(i+1))
			common.Log.Debug("\tid=%q text=%d", id, len(idText.Text))
		}
	}
	dt := time.Since(t0)
	common.Log.Debug("\tIndexed %d pages in %.1f sec (%.3f sec/page)\n",
		len(docPages), dt.Seconds(), dt.Seconds()/float64(len(docPages)))
	return nil
}

/*
   PositionsState is for serializing and accessing DocPageLocations.

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
	root       string                   // Top level directory of the data saved to disk
	fileList   []FileDesc               // List of file entries
	hashIndex  map[string]uint64        // {file hash: index into fileList}
	indexHash  map[uint64]string        // {index into fileList: file hash}
	hashPath   map[string]string        // {file hash: file path}
	hashDoc    map[string]*DocPositions // {file hash: DocPositions}
	updateTime time.Time                // Time of last Flush()
}

func (l PositionsState) String() string {
	var parts []string
	parts = append(parts,
		fmt.Sprintf("%q fileList=%d hashIndex=%d indexHash=%d hashPath=%d hashDoc=%d %s",
			l.root, len(l.fileList), len(l.hashIndex), len(l.indexHash), len(l.hashPath),
			len(l.hashDoc), l.updateTime))
	for k, lDoc := range l.hashDoc {
		parts = append(parts, fmt.Sprintf("%q: %d", k, lDoc.Len()))
	}
	return fmt.Sprintf("{PositionsState: %s}", strings.Join(parts, "\t"))
}

func (l PositionsState) Check() {
	err := fmt.Errorf("Bad PositionsState: %s", l)
	if len(l.fileList) == 0 || len(l.hashIndex) == 0 || len(l.indexHash) == 0 || len(l.hashPath) == 0 {
		panic(err)
	}
	if len(l.hashDoc) == 0 {
		panic(err)
	}
	for _, lDoc := range l.hashDoc {
		if lDoc.Len() == 0 {
			panic(err)
		}
	}
}

func FromHIPDs(hipds []serial.HashIndexPathDoc) PositionsState {
	var l PositionsState
	l.hashIndex = map[string]uint64{} // {file hash: index into fileList}
	l.indexHash = map[uint64]string{} // {index into fileList: file hash}
	l.hashPath = map[string]string{}  // {file hash: file path}
	l.hashDoc = map[string]*DocPositions{}
	for _, h := range hipds {
		hash := h.Hash
		idx := h.Index
		path := h.Path
		sdoc := h.Doc

		doc := DocPositions{
			inPath: sdoc.Path,   // Path of input PDF file.
			docIdx: sdoc.DocIdx, // Index into lState.fileList.
			docData: &docData{
				pageNums:  sdoc.PageNums,
				pageTexts: sdoc.PageTexts,
			},
		}
		if len(doc.pageNums) == 0 {
			panic("pageNums")
		}
		if len(doc.pageTexts) == 0 {
			panic("pageTexts")
		}
		l.hashPath[hash] = path
		l.hashDoc[hash] = &doc
		l.hashIndex[hash] = idx
		l.indexHash[idx] = hash
	}
	if len(l.hashPath) == 0 {
		panic("hashPath")
	}
	if len(l.hashDoc) == 0 {
		panic("hashDoc")
	}
	if len(l.hashIndex) == 0 {
		panic("hashIndex")
	}
	if len(l.indexHash) == 0 {
		panic("indexHash")
	}
	return l
}

func (l PositionsState) ToHIPDs() []serial.HashIndexPathDoc {
	var hipds []serial.HashIndexPathDoc
	for hash, idx := range l.hashIndex {
		path := l.hashPath[hash]
		doc := l.hashDoc[hash]
		sdoc := serial.DocPositions{
			Path:      doc.inPath, // Path of input PDF file.
			DocIdx:    doc.docIdx, // Index into lState.fileList.
			PageNums:  doc.pageNums,
			PageTexts: doc.pageTexts,
		}
		h := serial.HashIndexPathDoc{
			Hash:  hash,
			Index: idx,
			Path:  path,
			Doc:   sdoc,
		}
		hipds = append(hipds, h)
	}
	return hipds
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
//    lState, err := doclib.OpenPositionsState(persistDir, forceCreate)
//    defer lState.Flush()
func OpenPositionsState(root string, forceCreate bool) (*PositionsState, error) {
	lState := PositionsState{
		root:      root,
		hashIndex: map[string]uint64{},
		indexHash: map[uint64]string{},
		hashPath:  map[string]string{},
	}
	if lState.isMem() {
		lState.hashDoc = map[string]*DocPositions{}
	} else {
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
	common.Log.Debug("OpenPositionsState: lState=%s", lState)
	return &lState, nil
}

func (lState *PositionsState) ExtractDocPagePositions(inPath string) ([]DocPageText, error) {
	panic("ExtractDocPagePositions")
	rs, err := os.Open(inPath)
	if err != nil {
		return []DocPageText{}, err
	}
	defer rs.Close()
	return lState.ExtractDocPagePositionsReader(inPath, rs)
}

// ExtractDocPagePositionsReader extracts the text of the PDF file referenced by `rs`.
// It returns the text as a DocPageText per page.
// The []DocPageText refer to DocPositions which are stored in lState.hashDoc which is updated in
// this function.
func (lState *PositionsState) ExtractDocPagePositionsReader(inPath string, rs io.ReadSeeker) (
	[]DocPageText, error) {

	fd, err := CreateFileDesc(inPath, rs)
	if err != nil {
		return nil, err
	}

	lDoc, err := lState.CreatePositionsDoc(fd)
	if err != nil {
		return nil, err
	}

	var docPages []DocPageText

	err = ProcessPDFPagesReader(inPath, rs, func(pageNum uint32, page *pdf.PdfPage) error {
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
			return err
		}

		docPages = append(docPages, DocPageText{
			DocIdx:  lDoc.docIdx,
			PageIdx: pageIdx,
			PageNum: pageNum,
			Text:    text,
		})
		if len(docPages)%100 == 99 {
			common.Log.Debug("  pageNum=%d docPages=%d %q", pageNum, len(docPages),
				filepath.Base(inPath))
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
		return nil, err
	}
	if lState.isMem() {
		common.Log.Debug("ExtractDocPagePositions: pageNums=%v", lDoc.docData.pageNums)
		lState.hashDoc[fd.Hash] = lDoc
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
	common.Log.Debug("*** Flush %3d files (%4.1f sec) %s",
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
		panic(fmt.Errorf("lState=%s", *lState)) // !@#$ Remove this
	}
	return filepath.Join(lState.positionsDir(), hash)
}

// createIfNecessary creates `lState`.positionsDir if it doesn't already exist.
// It is called at the start of CreatePositionsDoc() which allows us to avoid creating our directory
// structure until we have successfully extracted the text from a PDF pages.
func (lState *PositionsState) createIfNecessary() error {
	if lState.root == "" {
		return fmt.Errorf("lState=%s", *lState)
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
	return lDoc.ReadPageText(pageIdx)
}

// ReadDocPagePositions is inefficient. A DocPositions (a file) is opened and closed to read a page.
func (lState *PositionsState) ReadDocPagePositions(docIdx uint64, pageIdx uint32) (
	string, uint32, serial.DocPageLocations, error) {

	lDoc, err := lState.OpenPositionsDoc(docIdx)
	if err != nil {
		return "", 0, serial.DocPageLocations{}, err
	}
	defer lDoc.Close()
	pageNum, dpl, err := lDoc.ReadPagePositions(pageIdx)
	common.Log.Debug("docIdx=%d lDoc=%s pageNum=%d", docIdx, lDoc, pageNum)
	return lDoc.inPath, pageNum, dpl, err
}

// CreatePositionsDoc creates a DocPositions for writing.
// CreatePositionsDoc always populates the DocPositions with base fields.
// In a persistent `lState`, necessary directories are created and files are opened.
func (lState *PositionsState) CreatePositionsDoc(fd FileDesc) (*DocPositions, error) {
	common.Log.Debug("CreatePositionsDoc: lState.positionsDir=%q", lState.positionsDir())

	docIdx, p, exists := lState.addFile(fd)
	if exists {
		common.Log.Error("ExtractDocPagePositions: %q is the same PDF as %q. Ignoring",
			fd.InPath, p)
		return nil, errors.New("duplicate PDF")
	}
	lDoc, err := lState.baseFields(docIdx)
	if err != nil {
		return nil, err
	}

	if lState.isMem() {
		return lDoc, nil
	}

	// Persistent case
	if err = lState.createIfNecessary(); err != nil {
		return nil, err
	}

	lDoc.dataFile, err = os.Create(lDoc.dataPath)
	if err != nil {
		return nil, err
	}
	err = MkDir(lDoc.textDir)
	return lDoc, err
}

// OpenPositionsDoc opens a DocPositions for reading.
// In a persistent `lState`, necessary files are opened in lDoc.openDoc().
func (lState *PositionsState) OpenPositionsDoc(docIdx uint64) (*DocPositions, error) {
	if lState.isMem() {
		hash := lState.indexHash[docIdx]
		lDoc := lState.hashDoc[hash]
		common.Log.Debug("OpenPositionsDoc(%d)->%s", docIdx, lDoc)
		return lDoc, nil
	}

	// Persistent handling.
	lDoc, err := lState.baseFields(docIdx)
	if err != nil {
		return nil, err
	}
	err = lDoc.openDoc()
	return lDoc, err
}

// baseFields populates a DocPositions with the fields that are the same for Open and Create.
func (lState *PositionsState) baseFields(docIdx uint64) (*DocPositions, error) {
	if int(docIdx) >= len(lState.fileList) {
		common.Log.Error("docIdx=%d lState=%s\n=%#v", docIdx, *lState, *lState)
		return nil, ErrRange
	}
	inPath := lState.fileList[docIdx].InPath
	hash := lState.fileList[docIdx].Hash

	lDoc := DocPositions{
		lState:  lState,
		inPath:  inPath,
		docIdx:  docIdx,
		pageDpl: map[uint32]serial.DocPageLocations{},
	}

	if lState.isMem() {
		mem := docData{}
		lDoc.docData = &mem
	} else {
		locPath := lState.docPath(hash)
		persist := docPersist{
			dataPath:    locPath + ".dat",
			spansPath:   locPath + ".idx.json",
			textDir:     locPath + ".pages",
			pageDplPath: locPath + ".dpl.json",
		}
		lDoc.docPersist = &persist
	}
	common.Log.Debug("baseFields: docIdx=%d lDoc=%+v", docIdx, lDoc)
	if lState.isMem() != lDoc.isMem() {
		panic(fmt.Errorf("lState.isMem()=%t lDoc.isMem()=%t", lState.isMem(), lDoc.isMem()))
	}
	return &lDoc, nil
}

func (lState *PositionsState) GetHashPath(docIdx uint64) (hash, inPath string) {
	hash = lState.indexHash[docIdx]
	inPath = lState.hashPath[hash]
	return hash, inPath
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
