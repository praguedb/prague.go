package wire

import (
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/vmihailenco/msgpack/v5"
	"github.com/vmihailenco/msgpack/v5/msgpcode"
)

// UUID is a protop `uuid`, a .NET Guid, in RFC 4122 byte order. It is
// comparable, so it keys a map.
type UUID [16]byte

// String is the canonical 8-4-4-4-12 form, as .NET's Guid.ToString() writes it.
func (u UUID) String() string {
	var b [36]byte
	hex.Encode(b[0:8], u[0:4])
	b[8] = '-'
	hex.Encode(b[9:13], u[4:6])
	b[13] = '-'
	hex.Encode(b[14:18], u[6:8])
	b[18] = '-'
	hex.Encode(b[19:23], u[8:10])
	b[23] = '-'
	hex.Encode(b[24:], u[10:])
	return string(b[:])
}

// ParseUUID reads the 8-4-4-4-12 form, in either case.
func ParseUUID(s string) (UUID, error) {
	var u UUID
	if len(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return u, fmt.Errorf("wire: %q is not a uuid", s)
	}
	h := s[0:8] + s[9:13] + s[14:18] + s[19:23] + s[24:]
	if _, err := hex.Decode(u[:], []byte(h)); err != nil {
		return u, fmt.Errorf("wire: %q is not a uuid", s)
	}
	return u, nil
}

// MustUUID is ParseUUID for constants.
func MustUUID(s string) UUID {
	u, err := ParseUUID(s)
	if err != nil {
		panic(err)
	}
	return u
}

// UUIDFromDotNet reads Guid.ToByteArray()'s layout, which is how Prague
// writes a Guid header: the first three groups little-endian.
func UUIDFromDotNet(b []byte) (UUID, error) {
	var u UUID
	if len(b) != 16 {
		return u, errors.New("wire: a .NET Guid is 16 bytes")
	}
	u[0], u[1], u[2], u[3] = b[3], b[2], b[1], b[0]
	u[4], u[5] = b[5], b[4]
	u[6], u[7] = b[7], b[6]
	copy(u[8:], b[8:])
	return u, nil
}

// DotNet is u as Guid.ToByteArray() lays it out.
func (u UUID) DotNet() []byte {
	b := make([]byte, 16)
	b[0], b[1], b[2], b[3] = u[3], u[2], u[1], u[0]
	b[4], b[5] = u[5], u[4]
	b[6], b[7] = u[7], u[6]
	copy(b[8:], u[8:])
	return b
}

// DecodeMsgpack implements msgpack.CustomDecoder. MessagePack-CSharp writes a
// Guid as its 36-character string.
func (u *UUID) DecodeMsgpack(d *msgpack.Decoder) error {
	c, err := d.PeekCode()
	if err != nil {
		return err
	}
	if !msgpcode.IsString(c) {
		return fmt.Errorf("wire: uuid: unexpected code %#x", c)
	}
	s, err := d.DecodeString()
	if err != nil {
		return err
	}
	*u, err = ParseUUID(s)
	return err
}

// Opt is an optional member of a struct that keys a cache. A pointer would
// make the key compare by address; Opt compares by value.
type Opt[T any] struct {
	Value T
	Valid bool
}

// Some is a present Opt.
func Some[T any](v T) Opt[T] { return Opt[T]{Value: v, Valid: true} }

// DecodeMsgpack implements msgpack.CustomDecoder: nil is absent.
func (o *Opt[T]) DecodeMsgpack(d *msgpack.Decoder) error {
	c, err := d.PeekCode()
	if err != nil {
		return err
	}
	if c == msgpcode.Nil {
		*o = Opt[T]{}
		return d.Skip()
	}
	o.Valid = true
	return d.Decode(&o.Value)
}
