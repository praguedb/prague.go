package wire

import (
	"bytes"
	"encoding/binary"
	"errors"
	"iter"

	"github.com/vmihailenco/msgpack/v5"
)

// Meta is what a Kafka message carries besides its key and value. The kafka
// package implements it over a record; generated Enrich methods read it.
type Meta interface {
	// Headers yields every header in order, repeats included.
	Headers() iter.Seq2[string, []byte]
	// Timestamp is the message timestamp in Unix milliseconds.
	Timestamp() int64
}

// Enrichment is what a type's Enrich reads from the message, which a consumer
// folds into change detection: the same value under a new header is a change.
type Enrichment struct {
	Headers   []string
	Timestamp bool
}

// Enricher is a value with members written from the Kafka message, as praguec
// emits for `[header]` and `[from_timestamp]`.
type Enricher interface {
	Enrich(Meta)
	Enrichment() Enrichment
}

// Header writes every occurrence of key that decodes into dst, in order, so the
// last decodable one wins and an undecodable one is skipped -- Prague's
// enricher, exactly. A message without the header leaves dst as the value had it.
func Header[T any](m Meta, key string, dst *T) {
	for k, b := range m.Headers() {
		if k != key {
			continue
		}
		var v T
		if HeaderValue(b, &v) == nil {
			*dst = v
		}
	}
}

// HeaderOpt is Header for an optional member.
func HeaderOpt[T any](m Meta, key string, dst **T) {
	for k, b := range m.Headers() {
		if k != key {
			continue
		}
		v := new(T)
		if HeaderValue(b, v) == nil {
			*dst = v
		}
	}
}

// HeaderValue decodes one header value as Prague's HeadersSerDe writes it: an
// int32 or int64 as raw little-endian bytes (exactly 4 or 8 of them, which no
// MessagePack integer is) or as exact MessagePack, a string as raw UTF-8, a
// uuid as Guid.ToByteArray()'s 16 bytes, and anything else as MessagePack.
func HeaderValue(b []byte, dst any) error {
	switch p := dst.(type) {
	case *UUID:
		u, err := UUIDFromDotNet(b)
		if err == nil {
			*p = u
		}
		return err
	case *string:
		*p = string(b)
		return nil
	case *int32:
		if len(b) == 4 {
			*p = int32(binary.LittleEndian.Uint32(b))
			return nil
		}
		return exact(b, p)
	case *int64:
		if len(b) == 8 {
			*p = int64(binary.LittleEndian.Uint64(b))
			return nil
		}
		return exact(b, p)
	}
	return msgpack.Unmarshal(b, dst)
}

var errTrailing = errors.New("wire: trailing bytes after header value")

// exact decodes one MessagePack value that must be the whole of b.
func exact(b []byte, dst any) error {
	r := bytes.NewReader(b)
	if err := msgpack.NewDecoder(r).Decode(dst); err != nil {
		return err
	}
	if r.Len() != 0 {
		return errTrailing
	}
	return nil
}

// IsNil reports whether a header value is MessagePack nil, which Prague's
// predicate filters answer with their passOnNull.
func IsNil(b []byte) bool { return len(b) == 1 && b[0] == 0xc0 }
