// Code from flatbuffers tutorial https://rwinslow.com/posts/use-flatbuffers-in-golang/
package main

import (
	"fmt"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/peterwilliams97/pdf-search/serial"
)

func main() {
	// test1()
	test2()
}

func test1() {
	loc := serial.TextLocation{100, 25.4, 25.4, (8.5 - 1) * 25.4, (11 - 1) * 25.4}
	b := flatbuffers.NewBuilder(0)
	buf := serial.MakeTextLocation(b, loc)
	loc2 := serial.ReadTextLocation(buf)
	fmt.Printf(" loc=%+v\nloc2=%+v\nThe encoded data is %d bytes long.\n", loc, loc2, len(buf))
}

func test2() {
	dpl := MakeDplData()
	b := flatbuffers.NewBuilder(0)
	buf := serial.MakeDocPageLocations(b, dpl)
	dpl2, err := serial.ReadDocPageLocations(buf)
	if err != nil {
		panic(err)
	}
	fmt.Printf(" loc=%+v\nloc2=%+v\nThe encoded data is %d bytes long.\n", dpl, dpl2, len(buf))
	fmt.Printf("      doc 0x%08X 0x%08X\n", dpl.Doc, dpl2.Doc)
	fmt.Printf("     page %10d %10d\n", dpl.Page, dpl2.Page)
	fmt.Printf("locations %10d %10d\n", len(dpl.Locations), len(dpl.Locations))
	for i, l := range dpl.Locations {
		l2 := dpl2.Locations[i]
		fmt.Printf("%5d %+6v %+6v\n", i, l, l2)
	}
	if dpl.Doc != dpl2.Doc {
		panic("Doc")
	}
	if dpl.Page != dpl2.Page {
		panic("Page")
	}
	if len(dpl.Locations) != len(dpl.Locations) {
		panic("Locations")
	}
	for i, l := range dpl.Locations {
		l2 := dpl2.Locations[i]
		if l.Llx != l2.Llx {
			panic(fmt.Errorf("%d: Llx", i))
		}
		if l.Lly != l2.Lly {
			panic(fmt.Errorf("%d: Lly", i))
		}
		if l.Urx != l2.Urx {
			panic(fmt.Errorf("%d: Urx", i))
		}
		if l.Ury != l2.Ury {
			panic(fmt.Errorf("%d: Ury", i))
		}
	}
}

func MakeDplData() serial.DocPageLocations {
	dpl := serial.DocPageLocations{0xDEADBEEF, 111, nil}
	for i := uint32(0); i < 9; i++ {
		f := float32(i) * 25.4
		l := serial.TextLocation{i * 10, f, f, f + 1.0, f + 2.0}
		dpl.Locations = append(dpl.Locations, l)
	}
	return dpl
}
