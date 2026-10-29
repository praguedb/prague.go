package wire_test

import (
	"encoding/binary"
	"iter"
	"testing"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/praguedb/prague.go/wire"
)

func TestHeaderValueReadsBothOfPraguesIntegerForms(t *testing.T) {
	var i32 int32
	if err := wire.HeaderValue(binary.LittleEndian.AppendUint32(nil, 0xFFFFFFFE), &i32); err != nil || i32 != -2 {
		t.Fatalf("raw int32: %d %v", i32, err)
	}
	mp, _ := msgpack.Marshal(int32(-300))
	if err := wire.HeaderValue(mp, &i32); err != nil || i32 != -300 {
		t.Fatalf("MessagePack int32: %d %v", i32, err)
	}
	var i64 int64
	if err := wire.HeaderValue(binary.LittleEndian.AppendUint64(nil, 1<<40), &i64); err != nil || i64 != 1<<40 {
		t.Fatalf("raw int64: %d %v", i64, err)
	}
	mp, _ = msgpack.Marshal(int64(7))
	if err := wire.HeaderValue(mp, &i64); err != nil || i64 != 7 {
		t.Fatalf("MessagePack int64: %d %v", i64, err)
	}
	// Exact: a MessagePack integer with bytes after it is not an integer header.
	if err := wire.HeaderValue(append(mp, 0x01), &i64); err == nil {
		t.Fatal("trailing bytes accepted")
	}
	var s string
	if err := wire.HeaderValue([]byte("eu-west"), &s); err != nil || s != "eu-west" {
		t.Fatalf("string: %q %v", s, err)
	}
	var b bool
	mp, _ = msgpack.Marshal(true)
	if err := wire.HeaderValue(mp, &b); err != nil || !b {
		t.Fatalf("bool: %v %v", b, err)
	}
}

type meta []struct {
	k string
	v []byte
}

func (m meta) Headers() iter.Seq2[string, []byte] {
	return func(yield func(string, []byte) bool) {
		for _, h := range m {
			if !yield(h.k, h.v) {
				return
			}
		}
	}
}
func (meta) Timestamp() int64 { return 0 }

// Prague's enricher walks every header: the last occurrence that decodes wins,
// one that does not is skipped, and an absent header leaves the member alone.
func TestHeaderTakesTheLastOccurrenceThatDecodes(t *testing.T) {
	m := meta{
		{"Tier", binary.LittleEndian.AppendUint32(nil, 1)},
		{"Tier", binary.LittleEndian.AppendUint32(nil, 2)},
		{"Tier", []byte("not-an-int")},
	}
	tier := int32(9)
	wire.Header(m, "Tier", &tier)
	if tier != 2 {
		t.Fatalf("tier %d, want 2", tier)
	}
	region := "kept"
	wire.Header(m, "x-region", &region)
	var opt *string
	wire.HeaderOpt(m, "x-region", &opt)
	if region != "kept" || opt != nil {
		t.Fatalf("an absent header wrote %q / %v", region, opt)
	}
	wire.HeaderOpt(meta{{"x-region", []byte("eu")}}, "x-region", &opt)
	if opt == nil || *opt != "eu" {
		t.Fatalf("optional: %v", opt)
	}
}

// .NET's Guid.ToByteArray() for 00112233-4455-6677-8899-aabbccddeeff.
var dotNetGuid = []byte{0x33, 0x22, 0x11, 0x00, 0x55, 0x44, 0x77, 0x66, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff}

func TestAUUIDHeaderIsDotNetsByteOrder(t *testing.T) {
	var u wire.UUID
	if err := wire.HeaderValue(dotNetGuid, &u); err != nil || u.String() != "00112233-4455-6677-8899-aabbccddeeff" {
		t.Fatalf("%s %v", u, err)
	}
	if string(u.DotNet()) != string(dotNetGuid) {
		t.Fatal("DotNet does not round-trip")
	}
	mp, _ := msgpack.Marshal("00112233-4455-6677-8899-AABBCCDDEEFF")
	var v wire.UUID
	if err := msgpack.Unmarshal(mp, &v); err != nil || v != u {
		t.Fatalf("value form: %s %v", v, err)
	}
}
