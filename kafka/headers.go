package kafka

import (
	"fmt"
	"iter"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/praguedb/prague.go/wire"
)

// HeaderFilter keeps or drops a record by one of its headers, before its key
// or value is decoded. Filters on one binding all have to pass. A filter runs
// on every occurrence of its header and passes when the header is absent --
// except Exists, which is the way to require one.
//
// A tombstone is filtered too: a rejected header drops the delete, so a
// consumer filtering out another tenant does not act on that tenant's
// removals. A tombstone missing a required header still applies, because
// producers do not stamp headers on a delete.
type HeaderFilter struct {
	key      string
	required bool
	pass     func([]byte) bool
	desc     string
}

// HeaderEquals keeps records whose header key equals one of values. T is any
// type HeaderValue decodes: string, int32, int64, wire.UUID, an enum, ….
func HeaderEquals[T comparable](key string, values ...T) HeaderFilter {
	return HeaderFilter{key: key, pass: func(b []byte) bool { return equalsAny(b, values) }, desc: fmt.Sprintf("%s == %v", key, values)}
}

// HeaderNotEquals keeps records whose header key equals none of values.
func HeaderNotEquals[T comparable](key string, values ...T) HeaderFilter {
	return HeaderFilter{key: key, pass: func(b []byte) bool { return !equalsAny(b, values) }, desc: fmt.Sprintf("%s != %v", key, values)}
}

// HeaderExists keeps only records that carry the header key.
func HeaderExists(key string) HeaderFilter {
	return HeaderFilter{key: key, required: true, pass: func([]byte) bool { return true }, desc: key + " exists"}
}

// HeaderPredicate keeps records whose header decodes to a value keep accepts. A
// MessagePack nil passes when passOnNull; a value that does not decode, or a
// keep that panics, rejects.
func HeaderPredicate[T any](key string, keep func(T) bool, passOnNull bool) HeaderFilter {
	return HeaderFilter{key: key, desc: key + " predicate", pass: func(b []byte) (ok bool) {
		if wire.IsNil(b) {
			return passOnNull
		}
		var v T
		if wire.HeaderValue(b, &v) != nil {
			return false
		}
		defer func() {
			if recover() != nil {
				ok = false
			}
		}()
		return keep(v)
	}}
}

func equalsAny[T comparable](b []byte, values []T) bool {
	var v T
	if wire.HeaderValue(b, &v) != nil {
		return false
	}
	for _, want := range values {
		if v == want {
			return true
		}
	}
	return false
}

// WithHeaderFilter adds header filters to a binding.
func WithHeaderFilter[K comparable, V any](filters ...HeaderFilter) Option[K, V] {
	return func(b *cacheBinding[K, V]) { b.headerFilters = append(b.headerFilters, filters...) }
}

// headersPass evaluates filters against r: every occurrence of a filtered
// header must pass, and every required header must be present unless r is a
// tombstone.
func headersPass(filters []HeaderFilter, r *kgo.Record) bool {
	for _, f := range filters {
		seen := false
		for _, h := range r.Headers {
			if h.Key != f.key {
				continue
			}
			seen = true
			if !f.pass(h.Value) {
				return false
			}
		}
		if f.required && !seen && len(r.Value) != 0 {
			return false
		}
	}
	return true
}

// recordMeta is a record as wire.Meta, without copying its headers. One
// pointer, so boxing it in the interface does not allocate.
type recordMeta struct{ r *kgo.Record }

func (m recordMeta) Headers() iter.Seq2[string, []byte] {
	return func(yield func(string, []byte) bool) {
		for _, h := range m.r.Headers {
			if !yield(h.Key, h.Value) {
				return
			}
		}
	}
}

func (m recordMeta) Timestamp() int64 { return m.r.Timestamp.UnixMilli() }
