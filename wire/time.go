package wire

import (
	"fmt"
	"time"

	"github.com/vmihailenco/msgpack/v5"
	"github.com/vmihailenco/msgpack/v5/msgpcode"
)

// Timestamp is a protop `timestamp`, a .NET DateTime, as an instant: UTC ticks,
// 100 ns each since 0001-01-01. It is an integer so that it compares, sorts and
// keys a range index as the instant it is, over DateTime's whole range.
//
// Prague writes a DateTime as DateTime.ToBinary(): the ticks in the low 62 bits
// and the DateTimeKind in the top two. There is no offset on the wire. A Utc or
// Local value is an instant (ToBinary stores a Local value's ticks already
// shifted to UTC); an Unspecified one is a wall-clock time with no zone, which
// is read as UTC because there is nothing else to read it as. MessagePack's
// own timestamp extension, which MessagePack-CSharp's standard resolver writes,
// is read too.
type Timestamp int64

const (
	ticksPerSecond = 10_000_000
	// unixEpochTicks is 1970-01-01T00:00:00Z in .NET ticks.
	unixEpochTicks = 621_355_968_000_000_000
	ticksMask      = 0x3FFF_FFFF_FFFF_FFFF
	localMask      = -0x8000_0000_0000_0000 // the top bit: DateTimeKind.Local
	// ticksCeiling is where .NET wraps a Local value whose UTC ticks went negative.
	ticksCeiling = 0x4000_0000_0000_0000
	maxTicks     = 3_155_378_975_999_999_999 // DateTime.MaxValue
)

// TimestampFromBinary reads DateTime.ToBinary()'s value as an instant.
func TimestampFromBinary(v int64) Timestamp {
	ticks := v & ticksMask
	if v&localMask != 0 && ticks > maxTicks {
		ticks -= ticksCeiling
	}
	return Timestamp(ticks)
}

// TimestampOf is t as a Timestamp, truncated to a tick.
func TimestampOf(t time.Time) Timestamp {
	sec := t.Unix()
	return Timestamp(unixEpochTicks + sec*ticksPerSecond + int64(t.Nanosecond())/100)
}

// Time is the instant in UTC.
func (x Timestamp) Time() time.Time {
	ticks := int64(x) - unixEpochTicks
	sec, rem := ticks/ticksPerSecond, ticks%ticksPerSecond
	if rem < 0 {
		sec, rem = sec-1, rem+ticksPerSecond
	}
	return time.Unix(sec, rem*100).UTC()
}

// UnixMilli is the instant in Unix milliseconds.
func (x Timestamp) UnixMilli() int64 { return x.Time().UnixMilli() }

func (x Timestamp) String() string { return x.Time().Format(time.RFC3339Nano) }

// DecodeMsgpack reads ToBinary's int64, or a timestamp extension. Nil is zero.
func (x *Timestamp) DecodeMsgpack(d *msgpack.Decoder) error {
	c, err := d.PeekCode()
	if err != nil {
		return err
	}
	switch {
	case c == msgpcode.Nil:
		*x = 0
		return d.DecodeNil()
	case msgpcode.IsExt(c) || msgpcode.IsFixedExt(c):
		t, err := d.DecodeTime()
		if err != nil {
			return err
		}
		*x = TimestampOf(t)
		return nil
	}
	v, err := d.DecodeInt64()
	if err != nil {
		return fmt.Errorf("timestamp: %w", err)
	}
	*x = TimestampFromBinary(v)
	return nil
}

// Date is a protop `date`, a .NET DateOnly: its day number, days since
// 0001-01-01, which is what MessagePack-CSharp writes. An integer, so it sorts
// and keys a range index as the date it is.
type Date int32

// DateOf is the civil date y-m-d.
func DateOf(y int, m time.Month, d int) Date {
	return Date((time.Date(y, m, d, 0, 0, 0, 0, time.UTC).Unix() + unixEpochTicks/ticksPerSecond) / 86400)
}

// Time is the date's midnight in UTC.
func (x Date) Time() time.Time {
	return time.Unix(int64(x)*86400-unixEpochTicks/ticksPerSecond, 0).UTC()
}

func (x Date) String() string { return x.Time().Format(time.DateOnly) }

// DecodeMsgpack reads the day number. Nil is zero.
func (x *Date) DecodeMsgpack(d *msgpack.Decoder) error {
	v, err := d.DecodeInt32()
	if err != nil {
		return fmt.Errorf("date: %w", err)
	}
	*x = Date(v)
	return nil
}
