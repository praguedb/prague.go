package wire_test

import (
	"testing"
	"time"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/praguedb/prague.go/wire"
)

func TestTimestampFromBinary(t *testing.T) {
	want := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cases := map[string]int64{
		// DateTime(2026, 1, 1, Utc).ToBinary(): ticks | 1 << 62.
		"utc": 5250714242427387904,
		// The same instant as Unspecified: the bare ticks.
		"unspecified": 639028224000000000,
		// The same instant written as Local: UTC ticks with the top bit set.
		"local": 639028224000000000 | -0x8000_0000_0000_0000,
	}
	for name, v := range cases {
		if got := wire.TimestampFromBinary(v).Time(); !got.Equal(want) {
			t.Errorf("%s: %v, want %v", name, got, want)
		}
	}
	// DateTime.MinValue as Local in a zone east of UTC wraps below zero.
	wrapped := wire.TimestampFromBinary((0x4000_0000_0000_0000 - 2*3600*10_000_000) | -0x8000_0000_0000_0000)
	if wrapped != -2*3600*10_000_000 {
		t.Errorf("wrapped local: %d", int64(wrapped))
	}
}

func TestTimestampDecodesExtensionAndNil(t *testing.T) {
	at := time.Date(2026, 7, 4, 20, 30, 0, 123_456_700, time.UTC)
	b, _ := msgpack.Marshal(at)
	var ts wire.Timestamp
	if err := msgpack.Unmarshal(b, &ts); err != nil {
		t.Fatal(err)
	}
	if !ts.Time().Equal(at) || ts != wire.TimestampOf(at) {
		t.Fatalf("ext: %v, want %v", ts, at)
	}
	var opt *wire.Timestamp
	if err := msgpack.Unmarshal([]byte{0xc0}, &opt); err != nil || opt != nil {
		t.Fatalf("nil: %v %v", opt, err)
	}
	if ts.UnixMilli() != at.UnixMilli() {
		t.Fatalf("unix ms: %d", ts.UnixMilli())
	}
}

func TestDate(t *testing.T) {
	d := wire.DateOf(1994, time.May, 17)
	if d != 728064 {
		t.Fatalf("day number %d, want 728064", d)
	}
	if d.String() != "1994-05-17" {
		t.Fatalf("string %s", d)
	}
	b, _ := msgpack.Marshal(uint32(728064))
	var got wire.Date
	if err := msgpack.Unmarshal(b, &got); err != nil || got != d {
		t.Fatalf("decode %v %v", got, err)
	}
}
