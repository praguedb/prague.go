// Package prague is a read-side, in-memory event cache fed by compacted Kafka
// topics written by Prague (.NET) producers.
//
// A Cache holds the latest value per key. Secondary indexes are declared in
// code (AddUnique, AddMany, AddRange) because which lookups a consumer needs is
// a property of the consumer, not of the data. Queries intersect index lanes,
// apply residual filters, sort and page.
//
// Concurrency: one writer (the Kafka consumer), any number of readers. Writes
// take an exclusive lock; a query holds a shared lock for its whole run, so a
// result is a consistent snapshot. Values handed to readers are shared and
// must be treated as read-only.
package prague

import "sync"

// UpdateType is what a write did. The numbering matches Prague (.NET).
type UpdateType uint8

const (
	Filtered UpdateType = iota // rejected by a filter, nothing stored
	Same                       // the incoming value equals the stored one
	Add
	Update
	Delete
)

func (u UpdateType) String() string {
	switch u {
	case Filtered:
		return "Filtered"
	case Same:
		return "Same"
	case Add:
		return "Add"
	case Update:
		return "Update"
	case Delete:
		return "Delete"
	}
	return "UpdateType(?)"
}

// Keyed is a value that names its own identity. praguec-generated tables
// implement it on their pointer type.
type Keyed[K comparable] interface {
	CacheKey() K
}

type entry[V any] struct {
	v  V
	fp uint64 // fingerprint of the encoded value; 0 = unknown
	ts int64  // unix ms of the write
}

// indexer is what a Cache maintains on every write.
type indexer[K comparable, V any] interface {
	add(k K, v V)
	remove(k K, v V)
}

// Cache is the latest value per key, plus its secondary indexes.
type Cache[K comparable, V any] struct {
	name    string
	keyOf   func(V) K
	mu      sync.RWMutex
	items   map[K]*entry[V]
	indexes []indexer[K, V]
}

// New creates a cache for a type that knows its key.
func New[K comparable, V Keyed[K]](name string) *Cache[K, V] {
	return NewFunc(name, func(v V) K { return v.CacheKey() })
}

// NewFunc creates a cache whose key is extracted by keyOf.
func NewFunc[K comparable, V any](name string, keyOf func(V) K) *Cache[K, V] {
	return &Cache[K, V]{name: name, keyOf: keyOf, items: map[K]*entry[V]{}}
}

func (c *Cache[K, V]) Name() string { return c.name }

func (c *Cache[K, V]) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.items)
}

// Get returns the value stored under k.
func (c *Cache[K, V]) Get(k K) (V, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if e, ok := c.items[k]; ok {
		return e.v, true
	}
	var zero V
	return zero, false
}

// GetMany returns the values found for keys, in key order, skipping misses.
func (c *Cache[K, V]) GetMany(keys ...K) []V {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]V, 0, len(keys))
	for _, k := range keys {
		if e, ok := c.items[k]; ok {
			out = append(out, e.v)
		}
	}
	return out
}

// LastUpdated returns the unix-ms timestamp of k's last write.
func (c *Cache[K, V]) LastUpdated(k K) (int64, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if e, ok := c.items[k]; ok {
		return e.ts, true
	}
	return 0, false
}

// Each calls fn for every entry under the shared lock until fn returns false.
// fn must not write to this cache.
func (c *Cache[K, V]) Each(fn func(K, V) bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for k, e := range c.items {
		if !fn(k, e.v) {
			return
		}
	}
}

// Upsert stores v under its own key. Without a fingerprint every write of an
// existing key counts as an Update.
func (c *Cache[K, V]) Upsert(v V, ts int64) UpdateType {
	u, _ := c.Apply(c.keyOf(v), v, 0, ts)
	return u
}

// Apply stores v under k. fp is a fingerprint of the encoded value: when it is
// non-zero and equals the stored one, nothing changes and Apply reports Same.
// It is how a feeder suppresses re-published, unchanged records.
func (c *Cache[K, V]) Apply(k K, v V, fp uint64, ts int64) (UpdateType, V) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.items[k]; ok {
		if fp != 0 && e.fp == fp {
			return Same, e.v
		}
		old := e.v
		for _, ix := range c.indexes {
			ix.remove(k, old)
			ix.add(k, v)
		}
		e.v, e.fp, e.ts = v, fp, ts
		return Update, old
	}
	c.items[k] = &entry[V]{v: v, fp: fp, ts: ts}
	for _, ix := range c.indexes {
		ix.add(k, v)
	}
	var zero V
	return Add, zero
}

// Fingerprint returns the fingerprint stored for k, so a feeder can skip
// decoding a value it already holds.
func (c *Cache[K, V]) Fingerprint(k K) (uint64, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if e, ok := c.items[k]; ok {
		return e.fp, true
	}
	return 0, false
}

// Remove deletes k and returns what was stored.
func (c *Cache[K, V]) Remove(k K) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.items[k]
	if !ok {
		var zero V
		return zero, false
	}
	delete(c.items, k)
	for _, ix := range c.indexes {
		ix.remove(k, e.v)
	}
	return e.v, true
}

// register adds an index and backfills it from what is already stored.
func (c *Cache[K, V]) register(ix indexer[K, V]) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, e := range c.items {
		ix.add(k, e.v)
	}
	c.indexes = append(c.indexes, ix)
}
