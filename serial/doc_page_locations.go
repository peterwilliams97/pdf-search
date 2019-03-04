// Code from flatbuffers tutorial https://rwinslow.com/posts/use-flatbuffers-in-golang/
package serial

import (
	"errors"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/peterwilliams97/pdf-search/serial/locations"
)

// table TextLocation {
// 	offset:   uint32;
// 	llx: float32;
// 	lly: float32;
// 	urx: float32;
// 	ury: float32;
// }
type TextLocation struct {
	Offset             uint32
	Llx, Lly, Urx, Ury float32
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
	// re-use the already-allocated Builder:
	b.Reset()

	// write the TextLocation object:
	locOffset := addTextLocation(b, loc)

	// finish the write operations by our TextLocation the root object:
	b.Finish(locOffset)

	// return the byte slice containing encoded data:
	return b.Bytes[b.Head():]
}

func addTextLocation(b *flatbuffers.Builder, loc TextLocation) flatbuffers.UOffsetT {
	// write the TextLocation object:
	locations.TextLocationStart(b)
	locations.TextLocationAddOffset(b, loc.Offset)
	locations.TextLocationAddLlx(b, loc.Llx)
	locations.TextLocationAddLly(b, loc.Lly)
	locations.TextLocationAddUrx(b, loc.Urx)
	locations.TextLocationAddUry(b, loc.Ury)
	return locations.TextLocationEnd(b)
}

func ReadTextLocation(buf []byte) TextLocation {
	// initialize a User reader from the given buffer:
	loc := locations.GetRootAsTextLocation(buf, 0)

	// copy the TextLocation's fields (since these are numbers):
	return TextLocation{loc.Offset(),
		loc.Llx(),
		loc.Lly(),
		loc.Urx(),
		loc.Ury(),
	}
}

func getTextLocation(loc *locations.TextLocation) TextLocation {
	// initialize a User reader from the given buffer:

	// copy the TextLocation's fields (since these are numbers):
	return TextLocation{loc.Offset(),
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

	// write the DocPageLocations object:
	locations.DocPageLocationsStart(b)
	locations.DocPageLocationsAddDoc(b, dpl.Doc)
	locations.DocPageLocationsAddPage(b, dpl.Page)
	locations.DocPageLocationsAddLocations(b, locationsOfs)
	textlocOfs := locations.DocPageLocationsEnd(b)

	// finish the write operations by our DocPageLocations the root object:
	b.Finish(textlocOfs)

	// return the byte slice containing encoded data:
	return b.Bytes[b.Head():]
}

func ReadDocPageLocations(buf []byte) (DocPageLocations, error) {
	// initialize a User reader from the given buffer:
	dpl := locations.GetRootAsDocPageLocations(buf, 0)

	// For vectors, like `Inventory`, they have a method suffixed with 'Length' that can be used
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

	// copy the TextLocation's fields (since these are numbers):
	return DocPageLocations{
		dpl.Doc(),
		dpl.Page(),
		locs,
	}, nil
}
