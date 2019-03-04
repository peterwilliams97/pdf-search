// Code from flatbuffers tutorial https://rwinslow.com/posts/use-flatbuffers-in-golang/

// main_test.go
package main

import (
	"testing"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/peterwilliams97/pdf-search/serial"
)

func BenchmarkWrite(b *testing.B) {
	builder := flatbuffers.NewBuilder(0)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		builder.Reset()
		dpl := MakeDplData()
		buf := serial.MakeDocPageLocations(builder, dpl)
		if i == 0 {
			b.SetBytes(int64(len(buf)))
		}
	}
}

func BenchmarkRead(b *testing.B) {
	builder := flatbuffers.NewBuilder(0)
	dpl := MakeDplData()
	buf := serial.MakeDocPageLocations(builder, dpl)
	b.SetBytes(int64(len(buf)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		dpl2, err := serial.ReadDocPageLocations(buf)
		if err != nil {
			panic(err)
		}
		// do some work to prevent cheating the benchmark:
		if dpl2.Doc != dpl.Doc {
			panic("ddd")
		}
	}
}

func BenchmarkRoundtrip(b *testing.B) {
	builder := flatbuffers.NewBuilder(0)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		builder.Reset()
		dpl := MakeDplData()
		buf := serial.MakeDocPageLocations(builder, dpl)
		dpl2, err := serial.ReadDocPageLocations(buf)
		if err != nil {
			panic(err)
		}
		if i == 0 {
			b.SetBytes(int64(len(buf)))
		}
		// do some work to prevent cheating the benchmark:
		if dpl2.Doc != dpl.Doc {
			panic("ddd")
		}
	}
}
