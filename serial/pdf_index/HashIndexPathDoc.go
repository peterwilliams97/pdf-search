// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package pdf_index

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

type HashIndexPathDoc struct {
	_tab flatbuffers.Table
}

func GetRootAsHashIndexPathDoc(buf []byte, offset flatbuffers.UOffsetT) *HashIndexPathDoc {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &HashIndexPathDoc{}
	x.Init(buf, n+offset)
	return x
}

func (rcv *HashIndexPathDoc) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *HashIndexPathDoc) Table() flatbuffers.Table {
	return rcv._tab
}

func (rcv *HashIndexPathDoc) Hash() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *HashIndexPathDoc) Index() uint64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		return rcv._tab.GetUint64(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *HashIndexPathDoc) MutateIndex(n uint64) bool {
	return rcv._tab.MutateUint64Slot(6, n)
}

func (rcv *HashIndexPathDoc) Path() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(8))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *HashIndexPathDoc) Doc(obj *DocPositions) *DocPositions {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(10))
	if o != 0 {
		x := rcv._tab.Indirect(o + rcv._tab.Pos)
		if obj == nil {
			obj = new(DocPositions)
		}
		obj.Init(rcv._tab.Bytes, x)
		return obj
	}
	return nil
}

func HashIndexPathDocStart(builder *flatbuffers.Builder) {
	builder.StartObject(4)
}
func HashIndexPathDocAddHash(builder *flatbuffers.Builder, hash flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(0, flatbuffers.UOffsetT(hash), 0)
}
func HashIndexPathDocAddIndex(builder *flatbuffers.Builder, index uint64) {
	builder.PrependUint64Slot(1, index, 0)
}
func HashIndexPathDocAddPath(builder *flatbuffers.Builder, path flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(2, flatbuffers.UOffsetT(path), 0)
}
func HashIndexPathDocAddDoc(builder *flatbuffers.Builder, doc flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(3, flatbuffers.UOffsetT(doc), 0)
}
func HashIndexPathDocEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}
