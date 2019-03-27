package serial

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/peterwilliams97/pdf-search/serial/locations"
	"github.com/unidoc/unidoc/common"
)

func WriteDocPageLocations(f *os.File, dpl DocPageLocations) error {
	b := flatbuffers.NewBuilder(0)
	buf := MakeDocPageLocations(b, dpl)
	check := crc32.ChecksumIEEE(buf) // uint32
	size := uint64(len(buf))
	if err := binary.Write(f, binary.LittleEndian, size); err != nil {
		return err
	}
	if err := binary.Write(f, binary.LittleEndian, check); err != nil {
		return err
	}
	_, err := f.Write(buf)
	return err
}

func RReadDocPageLocations(f *os.File) (DocPageLocations, error) {
	var size uint64
	var check uint32
	if err := binary.Read(f, binary.LittleEndian, &size); err != nil {
		return DocPageLocations{}, err
	}
	if err := binary.Read(f, binary.LittleEndian, &check); err != nil {
		return DocPageLocations{}, err
	}
	buf := make([]byte, size)
	if _, err := f.Read(buf); err != nil {
		return DocPageLocations{}, err
	}
	if crc32.ChecksumIEEE(buf) != check {
		panic(errors.New("bad checksum"))
		return DocPageLocations{}, errors.New("bad checksum")
	}
	return ReadDocPageLocations(buf)
}

// table TextLocation {
// 	offset:   uint32;
// 	llx: float32;
// 	lly: float32;
// 	urx: float32;
// 	ury: float32;
// }
type TextLocation struct {
	Start, End         uint32
	Llx, Lly, Urx, Ury float32
}

func (t TextLocation) String() string {
	return fmt.Sprintf("{TextLocation: %d:%d (%5.1f, %5.1f) (%5.1f, %5.1f)",
		t.Start, t.End,
		t.Llx, t.Lly, t.Urx, t.Ury)
}

// table DocPageLocations  {
// 	doc:       uint64;
// 	page:      uint32;
// 	locations: [TextLocation];
// }
type DocPageLocations struct {
	Doc       uint64
	Page      uint32
	Locations []TextLocation
}

func MakeTextLocation(b *flatbuffers.Builder, loc TextLocation) []byte {
	// Re-use the already-allocated Builder.
	b.Reset()

	// Write the TextLocation object.
	locOffset := addTextLocation(b, loc)

	// Finish the write operations by our TextLocation the root object.
	b.Finish(locOffset)

	// Return the byte slice containing encoded data.
	return b.Bytes[b.Head():]
}

// addTextLocation writes `loc` to builder `b`.
func addTextLocation(b *flatbuffers.Builder, loc TextLocation) flatbuffers.UOffsetT {
	// Write the TextLocation object.
	locations.TextLocationStart(b)
	locations.TextLocationAddOffset(b, loc.Start)
	locations.TextLocationAddLlx(b, loc.Llx)
	locations.TextLocationAddLly(b, loc.Lly)
	locations.TextLocationAddUrx(b, loc.Urx)
	locations.TextLocationAddUry(b, loc.Ury)
	return locations.TextLocationEnd(b)
}

func ReadTextLocation(buf []byte) TextLocation {
	// Initialize a TextLocation reader from `buf`.
	loc := locations.GetRootAsTextLocation(buf, 0)
	return getTextLocation(loc)
}

func getTextLocation(loc *locations.TextLocation) TextLocation {
	// Copy the TextLocation's fields (since these are numbers).
	return TextLocation{
		loc.Offset(),
		0,
		loc.Llx(),
		loc.Lly(),
		loc.Urx(),
		loc.Ury(),
	}
}

func MakeDocPageLocations(b *flatbuffers.Builder, dpl DocPageLocations) []byte {
	b.Reset()

	var locOffsets []flatbuffers.UOffsetT
	for _, loc := range dpl.Locations {
		locOfs := addTextLocation(b, loc)
		locOffsets = append(locOffsets, locOfs)
	}
	locations.DocPageLocationsStartLocationsVector(b, len(dpl.Locations))
	// Prepend TextLocations in reverse order.
	for i := len(locOffsets) - 1; i >= 0; i-- {
		b.PrependUOffsetT(locOffsets[i])
	}
	locationsOfs := b.EndVector(len(dpl.Locations))

	// Write the DocPageLocations object.
	locations.DocPageLocationsStart(b)
	locations.DocPageLocationsAddDoc(b, dpl.Doc)
	locations.DocPageLocationsAddPage(b, dpl.Page)
	locations.DocPageLocationsAddLocations(b, locationsOfs)
	dplOfs := locations.DocPageLocationsEnd(b)

	// Finish the write operations by our DocPageLocations the root object.
	b.Finish(dplOfs)

	// return the byte slice containing encoded data:
	return b.Bytes[b.Head():]
}

func ReadDocPageLocations(buf []byte) (DocPageLocations, error) {
	// Initialize a DocPageLocations reader from `buf`.
	dpl := locations.GetRootAsDocPageLocations(buf, 0)

	// Vectors, such as `Locations`, have a method suffixed with 'Length' that can be used
	// to query the length of the vector. You can index the vector by passing an index value
	// into the accessor.
	var locs []TextLocation
	for i := 0; i < dpl.LocationsLength(); i++ {
		var loc locations.TextLocation
		ok := dpl.Locations(&loc, i)
		if !ok {
			return DocPageLocations{}, errors.New("bad TextLocation")
		}
		locs = append(locs, getTextLocation(&loc))
	}

	common.Log.Info("ReadDocPageLocations: Doc=%d Page=%d locs=%d", dpl.Doc(), dpl.Page(), len(locs))

	return DocPageLocations{
		dpl.Doc(),
		dpl.Page(),
		locs,
	}, nil
}
