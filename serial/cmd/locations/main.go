// Code from flatbuffers tutorial https://rwinslow.com/posts/use-flatbuffers-in-golang/
package main

import (
	"fmt"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/peterwilliams97/pdf-search/serial/locations"
)

func main() {
	// test1()
	test2()
}

func test1() {
	loc := TextLocation{100, 25.4, 25.4, (8.5 - 1) * 25.4, (11 - 1) * 25.4}
	b := flatbuffers.NewBuilder(0)
	buf := MakeTextLocation(b, loc)
	loc2 := ReadTextLocation(buf)
	fmt.Printf(" loc=%+v\nloc2=%+v\nThe encoded data is %d bytes long.\n", loc, loc2, len(buf))
}

// table DocPageLocations  {
// 	doc:       uint64;
// 	page:      uint32;
// 	locations: [TextLocation];
// }
type DocPageLocations struct {
	doc       uint64
	page      uint32
	locations []TextLocation
}

func MakeDplData() DocPageLocations {
	dpl := DocPageLocations{0xDEADBEEF, 111, nil}
	for i := uint32(0); i < 100; i++ {
		f := float32(i) * 25.4
		l := TextLocation{i * 10, f, f, f + 1.0, f + 2.0}
		dpl.locations = append(dpl.locations, l)
	}
	return dpl
}

func test2() {
	dpl := MakeDplData()
	b := flatbuffers.NewBuilder(0)
	buf := MakeDocPageLocations(b, dpl)
	dpl2 := ReadDocPageLocations(buf)
	fmt.Printf(" loc=%+v\nloc2=%+v\nThe encoded data is %d bytes long.\n", dpl, dpl2, len(buf))
	fmt.Printf("      doc 0x%08X 0x%08X\n", dpl.doc, dpl2.doc)
	fmt.Printf("     page %10d %10d\n", dpl.page, dpl2.page)
	fmt.Printf("locations %10d %10d\n", len(dpl.locations), len(dpl.locations))
	for i, l := range dpl.locations {
		l2 := dpl2.locations[i]
		fmt.Printf("%5d %+6v %+6v\n", i, l, l2)
	}
	if dpl.doc != dpl2.doc {
		panic("doc")
	}
	if dpl.page != dpl2.page {
		panic("doc")
	}
	if len(dpl.locations) != len(dpl.locations) {
		panic("locations")
	}
	for i, l := range dpl.locations {
		l2 := dpl2.locations[i]
		if l.llx != l2.llx {
			panic(fmt.Errorf("%d: llx", i))
		}
		if l.lly != l2.lly {
			panic(fmt.Errorf("%d: lly", i))
		}
		if l.urx != l2.urx {
			panic(fmt.Errorf("%d: urx", i))
		}
		if l.ury != l2.ury {
			panic(fmt.Errorf("%d: ury", i))
		}
	}
}

// table TextLocation {
// 	offset:   uint32;
// 	llx: float32;
// 	lly: float32;
// 	urx: float32;
// 	ury: float32;
// }
type TextLocation struct {
	offset             uint32
	llx, lly, urx, ury float32
}

func MakeTextLocation(b *flatbuffers.Builder, loc TextLocation) []byte {
	// re-use the already-allocated Builder:
	b.Reset()

	// write the TextLocation object:
	locations.TextLocationStart(b)
	locations.TextLocationAddOffset(b, loc.offset)
	locations.TextLocationAddLlx(b, loc.llx)
	locations.TextLocationAddLly(b, loc.lly)
	locations.TextLocationAddUrx(b, loc.urx)
	locations.TextLocationAddUry(b, loc.ury)
	textloc_position := locations.TextLocationEnd(b)

	// finish the write operations by our TextLocation the root object:
	b.Finish(textloc_position)

	// return the byte slice containing encoded data:
	return b.Bytes[b.Head():]
}

func addTextLocation(b *flatbuffers.Builder, loc TextLocation) flatbuffers.UOffsetT {
	// write the TextLocation object:
	locations.TextLocationStart(b)
	locations.TextLocationAddOffset(b, loc.offset)
	locations.TextLocationAddLlx(b, loc.llx)
	locations.TextLocationAddLly(b, loc.lly)
	locations.TextLocationAddUrx(b, loc.urx)
	locations.TextLocationAddUry(b, loc.ury)
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

	var llocs []flatbuffers.UOffsetT
	for _, loc := range dpl.locations {
		ll := addTextLocation(b, loc)
		llocs = append(llocs, ll)
	}
	locations.DocPageLocationsStartLocationsVector(b, len(dpl.locations))
	for i := len(llocs) - 1; i >= 0; i-- {
		ll := llocs[i]
		b.PrependUOffsetT(ll)
	}
	locs := b.EndVector(len(dpl.locations))

	// write the DocPageLocations object:
	locations.DocPageLocationsStart(b)
	locations.DocPageLocationsAddDoc(b, dpl.doc)
	locations.DocPageLocationsAddPage(b, dpl.page)
	locations.DocPageLocationsAddLocations(b, locs)
	textloc_position := locations.DocPageLocationsEnd(b)

	// finish the write operations by our DocPageLocations the root object:
	b.Finish(textloc_position)

	// return the byte slice containing encoded data:
	return b.Bytes[b.Head():]
}

func ReadDocPageLocations(buf []byte) DocPageLocations {
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
			panic("wtf")
		}
		locs = append(locs, getTextLocation(&loc))
	}

	// copy the TextLocation's fields (since these are numbers):
	return DocPageLocations{
		dpl.Doc(),
		dpl.Page(),
		locs,
	}
}
