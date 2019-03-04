// Code from flatbuffers tutorial https://rwinslow.com/posts/use-flatbuffers-in-golang/

// main_test.go
package main

import (
	"testing"

	flatbuffers "github.com/google/flatbuffers/go"
)

func BenchmarkWrite(b *testing.B) {
	builder := flatbuffers.NewBuilder(0)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		builder.Reset()
		dpl := MakeDplData()
		buf := MakeDocPageLocations(builder, dpl)
		if i == 0 {
			b.SetBytes(int64(len(buf)))
		}
	}
}

func BenchmarkRead(b *testing.B) {
	builder := flatbuffers.NewBuilder(0)
	dpl := MakeDplData()
	buf := MakeDocPageLocations(builder, dpl)
	b.SetBytes(int64(len(buf)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		dpl2 := ReadDocPageLocations(buf)
		// do some work to prevent cheating the benchmark:
		if dpl2.doc != dpl.doc {
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
		buf := MakeDocPageLocations(builder, dpl)
		dpl2 := ReadDocPageLocations(buf)
		if i == 0 {
			b.SetBytes(int64(len(buf)))
		}
		// do some work to prevent cheating the benchmark:
		if dpl2.doc != dpl.doc {
			panic("ddd")
		}
	}
}
