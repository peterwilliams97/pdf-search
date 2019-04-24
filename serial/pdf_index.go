package serial

import (
	"errors"
	"fmt"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/peterwilliams97/pdf-search/serial/pdf_index"
	"github.com/unidoc/unidoc/common"
)

// table PdfIndex  {
// 	num_files:   uint32;
// 	num_pages:   uint32;
// 	index :     [byte];
// 	hipd:       [HashIndexPathDoc];
// }
type SerialPdfIndex struct {
	NumFiles uint32
	NumPages uint32
	BleveMem []byte
	HIPDs    []HashIndexPathDoc
}

// WriteSerialPdfIndex
func WriteSerialPdfIndex(spi SerialPdfIndex) []byte {
	b := flatbuffers.NewBuilder(0)
	buf := MakeSerialPdfIndex(b, spi)
	return buf
}

// func ReadSerialPdfIndex(buf []byte) (SerialPdfIndex, error) {
// 	return ReadSerialPdfIndex(buf)
// }

// func WWriteSerialPdfIndex(f *os.File, spi SerialPdfIndex) error {
// 	b := flatbuffers.NewBuilder(0)
// 	buf := MakeSerialPdfIndex(b, spi)
// 	check := crc32.ChecksumIEEE(buf) // uint32
// 	size := uint64(len(buf))
// 	if err := binary.Write(f, binary.LittleEndian, size); err != nil {
// 		return err
// 	}
// 	if err := binary.Write(f, binary.LittleEndian, check); err != nil {
// 		return err
// 	}
// 	_, err := f.Write(buf)
// 	return err
// }

// func RReadSerialPdfIndex(f *os.File) (SerialPdfIndex, error) {
// 	var size uint64
// 	var check uint32
// 	if err := binary.Read(f, binary.LittleEndian, &size); err != nil {
// 		return SerialPdfIndex{}, err
// 	}
// 	if err := binary.Read(f, binary.LittleEndian, &check); err != nil {
// 		return SerialPdfIndex{}, err
// 	}
// 	buf := make([]byte, size)
// 	if _, err := f.Read(buf); err != nil {
// 		return SerialPdfIndex{}, err
// 	}
// 	if crc32.ChecksumIEEE(buf) != check {
// 		panic(errors.New("bad checksum"))
// 		return SerialPdfIndex{}, errors.New("bad checksum")
// 	}
// 	return ReadSerialPdfIndex(buf)
// }

func MakeSerialPdfIndex(b *flatbuffers.Builder, spi SerialPdfIndex) []byte {
	b.Reset()

	var locOffsets []flatbuffers.UOffsetT
	for _, hipd := range spi.HIPDs {
		locOfs := addHashIndexPathDoc(b, hipd)
		locOffsets = append(locOffsets, locOfs)
	}
	pdf_index.PdfIndexStartHipdVector(b, len(spi.HIPDs))
	// Prepend TextLocations in reverse order.
	for i := len(locOffsets) - 1; i >= 0; i-- {
		b.PrependUOffsetT(locOffsets[i])
	}
	locationsOfs := b.EndVector(len(spi.HIPDs))

	// Write the SerialPdfIndex object.
	pdf_index.PdfIndexStart(b)
	pdf_index.PdfIndexAddNumFiles(b, spi.NumFiles)
	pdf_index.PdfIndexAddNumPages(b, spi.NumPages)
	pdf_index.PdfIndexAddHipd(b, locationsOfs)
	dplOfs := pdf_index.PdfIndexEnd(b)

	// Finish the write operations by our SerialPdfIndex the root object.
	b.Finish(dplOfs)

	// return the byte slice containing encoded data:
	return b.Bytes[b.Head():]
}

func ReadSerialPdfIndex(buf []byte) (SerialPdfIndex, error) {
	// Initialize a SerialPdfIndex reader from `buf`.
	spi := pdf_index.GetRootAsPdfIndex(buf, 0)

	// Vectors, such as `Hipd`, have a method suffixed with 'Length' that can be used
	// to query the length of the vector. You can index the vector by passing an index value
	// into the accessor.
	var hipds []HashIndexPathDoc
	for i := 0; i < spi.HipdLength(); i++ {
		var loc pdf_index.HashIndexPathDoc
		ok := spi.Hipd(&loc, i)
		if !ok {
			return SerialPdfIndex{}, errors.New("bad HashIndexPathDoc")
		}
		hipds = append(hipds, getHashIndexPathDoc(&loc))
	}

	common.Log.Info("ReadSerialPdfIndex: NumFiles=%d NumPages=%d HIPDs=%d",
		spi.NumFiles(), spi.NumPages(), len(hipds))
	for i := 0; i < len(hipds) && i < 2; i++ {
		common.Log.Info("ReadSerialPdfIndex: hipds[%d]=%s", i, hipds[i])
	}

	return SerialPdfIndex{
		NumFiles: spi.NumFiles(),
		NumPages: spi.NumPages(),
		// Index    []byte
		HIPDs: hipds,
	}, nil
}

// func RReadPdfIndex(f *os.File) (SerialPdfIndex, error) {
// 	var size uint64
// 	var check uint32
// 	if err := binary.Read(f, binary.LittleEndian, &size); err != nil {
// 		return SerialPdfIndex{}, err
// 	}
// 	if err := binary.Read(f, binary.LittleEndian, &check); err != nil {
// 		return SerialPdfIndex{}, err
// 	}
// 	buf := make([]byte, size)
// 	if _, err := f.Read(buf); err != nil {
// 		return SerialPdfIndex{}, err
// 	}
// 	if crc32.ChecksumIEEE(buf) != check {
// 		panic(errors.New("bad checksum"))
// 		return SerialPdfIndex{}, errors.New("bad checksum")
// 	}
// 	return ReadSerialPdfIndex(buf)
// }

// table HashIndexPathDoc {
// 	hash: string;
// 	index: uint64;
// 	path: string;
// 	doc: DocPositions;
// }
type HashIndexPathDoc struct {
	Hash  string
	Index uint64
	Path  string
	Doc   DocPositions
}

// addHashIndexPathDoc writes HashIndexPathDoc `hipd` to builder `b`.
func addHashIndexPathDoc(b *flatbuffers.Builder, hipd HashIndexPathDoc) flatbuffers.UOffsetT {
	hash := b.CreateString(hipd.Hash)
	path := b.CreateString(hipd.Path)
	doc := addDocPositions(b, hipd.Doc)

	// Write the HashIndexPathDoc object.
	pdf_index.HashIndexPathDocStart(b)
	pdf_index.HashIndexPathDocAddHash(b, hash)
	pdf_index.HashIndexPathDocAddIndex(b, hipd.Index)
	pdf_index.HashIndexPathDocAddPath(b, path)
	pdf_index.HashIndexPathDocAddDoc(b, doc)
	return pdf_index.HashIndexPathDocEnd(b)
}

// getHashIndexPathDoc reads a HashIndexPathDoc.
func getHashIndexPathDoc(loc *pdf_index.HashIndexPathDoc) HashIndexPathDoc {
	// Copy the HashIndexPathDoc's fields (since these are numbers).
	var pos pdf_index.DocPositions
	sdoc := loc.Doc(&pos)
	// doc := getDocPositions(sdoc)

	numPageNums := sdoc.PageNumsLength()
	numPageTexts := sdoc.PageTextsLength()
	common.Log.Info("numPageNums=%d", numPageNums)
	common.Log.Info("numPageTexts=%d", numPageTexts)
	var pageNums []uint32
	for i := 0; i < sdoc.PageNumsLength(); i++ {
		num := sdoc.PageNums(i)
		pageNums = append(pageNums, num)
	}
	var pageTexts []string
	for i := 0; i < sdoc.PageTextsLength(); i++ {
		text := string(sdoc.PageTexts(i))
		pageTexts = append(pageTexts, text)
	}

	doc := DocPositions{
		Path:      string(sdoc.Path()),
		DocIdx:    sdoc.DocIdx(),
		PageNums:  pageNums,
		PageTexts: pageTexts,
	}

	common.Log.Info("Hash=%q", string(loc.Hash()))
	common.Log.Info("Path=%#q", string(loc.Path()))
	hipd := HashIndexPathDoc{
		Hash:  string(loc.Hash()),
		Path:  string(loc.Path()),
		Index: loc.Index(),
		Doc:   doc,
	}
	// common.Log.Info("getHashIndexPathDoc: hipd=%#v", hipd)

	return hipd
}

// // DocPositions tracks the data that is used to index a PDF file.
// table DocPositions {
// 	path:  string;                               // Path of input PDF file.
// 	doc_idx:  uint64;                            // Index into lState.fileList.
// 	page_nums:  [uint32];
// 	page_texts: [string];
// }
type DocPositions struct {
	Path      string // Path of input PDF file.
	DocIdx    uint64 // Index into lState.fileList.
	PageNums  []uint32
	PageTexts []string
}

func (doc DocPositions) String() string {
	return fmt.Sprintf("{DocPositions: DocIdx=%d PageNums=%d PageTexts=%d %q}",
		doc.DocIdx, len(doc.PageNums), len(doc.PageTexts), doc.Path)
}

func MakeDocPositions(b *flatbuffers.Builder, doc DocPositions) []byte {

	common.Log.Info("MakeDocPositions: doc=%s", doc)
	b.Reset()

	dplOfs := addDocPositions(b, doc)

	// Finish the write operations by our SerialPdfIndex the root object.
	b.Finish(dplOfs)

	// return the byte slice containing encoded data:
	return b.Bytes[b.Head():]
}

func addDocPositions(b *flatbuffers.Builder, doc DocPositions) flatbuffers.UOffsetT {
	path := b.CreateString(doc.Path)

	var pageOffsets []flatbuffers.UOffsetT
	for _, pageNum := range doc.PageNums {
		b.StartObject(1)
		// common.Log.Info("i=%d pageNum=%d", i, pageNum)
		b.PrependUint32Slot(0, pageNum, 0)
		locOfs := b.EndObject()
		pageOffsets = append(pageOffsets, locOfs)
	}

	pdf_index.DocPositionsStartPageNumsVector(b, len(doc.PageNums))
	// Prepend TextLocations in reverse order.
	for i := len(pageOffsets) - 1; i >= 0; i-- {
		b.PrependUOffsetT(pageOffsets[i])
	}
	pageOfs := b.EndVector(len(doc.PageNums))

	var textOffsets []flatbuffers.UOffsetT
	for _, text := range doc.PageTexts {
		textOfs := b.CreateString(text)
		textOffsets = append(textOffsets, textOfs)
	}
	pdf_index.DocPositionsStartPageTextsVector(b, len(doc.PageTexts))
	// Prepend TextLocations in reverse order.
	for i := len(textOffsets) - 1; i >= 0; i-- {
		b.PrependUOffsetT(textOffsets[i])
	}
	textOfs := b.EndVector(len(doc.PageTexts))

	// Write the SerialPdfIndex object.
	pdf_index.DocPositionsStart(b)

	pdf_index.DocPositionsAddPath(b, path)
	pdf_index.DocPositionsAddDocIdx(b, doc.DocIdx)
	pdf_index.DocPositionsAddPageNums(b, pageOfs)
	pdf_index.DocPositionsAddPageTexts(b, textOfs)
	return pdf_index.DocPositionsEnd(b)
}

func ReadDocPositions(buf []byte) (DocPositions, error) {
	// Initialize a SerialPdfIndex reader from `buf`.
	sdoc := pdf_index.GetRootAsDocPositions(buf, 0)

	// Vectors, such as `PageNums`, have a method suffixed with 'Length' that can be used
	// to query the length of the vector. You can index the vector by passing an index value
	// into the accessor.
	var pageNums []uint32
	for i := 0; i < sdoc.PageNumsLength(); i++ {
		page := sdoc.PageNums(i)
		pageNums = append(pageNums, page)
	}

	var pageTexts []string
	for i := 0; i < sdoc.PageTextsLength(); i++ {
		text := string(sdoc.PageTexts(i))
		pageTexts = append(pageTexts, text)
	}

	doc := DocPositions{
		Path:      string(sdoc.Path()),
		DocIdx:    sdoc.DocIdx(),
		PageNums:  pageNums,
		PageTexts: pageTexts,
	}

	// common.Log.Info("ReadDocPositions: doc=%s", doc)
	return doc, nil
}
