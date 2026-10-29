package prague

import (
	"cmp"

	"github.com/google/btree"
)

// Lane narrows a query's candidates. Lanes come from an index (Eq, In, Gte, …)
// and AND together; Or unions two of them. All methods run under the owning
// cache's read lock.
type Lane[K comparable, V any] interface {
	// estimate counts candidates, stopping once the count exceeds limit.
	estimate(limit int) int
	// each yields candidate keys until fn returns false.
	each(fn func(K) bool)
	// match reports whether v belongs to the lane.
	match(v V) bool
}

func always[V, I any](of func(V) I) func(V) (I, bool) {
	return func(v V) (I, bool) { return of(v), true }
}

// ---- Unique ---------------------------------------------------------------

// UniqueIndex maps one indexed value to one key. If two keys share a value the
// latest write owns it.
type UniqueIndex[K comparable, V any, I comparable] struct {
	c  *Cache[K, V]
	of func(V) (I, bool)
	m  map[I]K
}

// AddUnique indexes of(v), which must be unique across the cache.
func AddUnique[K comparable, V any, I comparable](c *Cache[K, V], of func(V) I) *UniqueIndex[K, V, I] {
	return AddUniqueOpt(c, always(of))
}

// AddUniqueOpt indexes of(v) when it reports ok; values without one are not indexed.
func AddUniqueOpt[K comparable, V any, I comparable](c *Cache[K, V], of func(V) (I, bool)) *UniqueIndex[K, V, I] {
	ix := &UniqueIndex[K, V, I]{c: c, of: of, m: map[I]K{}}
	c.register(ix)
	return ix
}

func (ix *UniqueIndex[K, V, I]) add(k K, v V) {
	if i, ok := ix.of(v); ok {
		ix.m[i] = k
	}
}

func (ix *UniqueIndex[K, V, I]) remove(k K, v V) {
	if i, ok := ix.of(v); ok && ix.m[i] == k {
		delete(ix.m, i)
	}
}

// Get returns the value whose indexed field equals i.
func (ix *UniqueIndex[K, V, I]) Get(i I) (V, bool) {
	ix.c.mu.RLock()
	defer ix.c.mu.RUnlock()
	if k, ok := ix.m[i]; ok {
		return ix.c.items[k].v, true
	}
	var zero V
	return zero, false
}

// Eq is the lane of the value whose indexed field equals i.
func (ix *UniqueIndex[K, V, I]) Eq(i I) Lane[K, V] { return ix.In(i) }

// In is the lane of values whose indexed field is any of is.
func (ix *UniqueIndex[K, V, I]) In(is ...I) Lane[K, V] {
	return &uniqueLane[K, V, I]{ix: ix, set: setOf(is)}
}

type uniqueLane[K comparable, V any, I comparable] struct {
	ix  *UniqueIndex[K, V, I]
	set map[I]struct{}
}

func (l *uniqueLane[K, V, I]) estimate(int) int { return len(l.set) }

func (l *uniqueLane[K, V, I]) each(fn func(K) bool) {
	for i := range l.set {
		if k, ok := l.ix.m[i]; ok && !fn(k) {
			return
		}
	}
}

func (l *uniqueLane[K, V, I]) match(v V) bool {
	i, ok := l.ix.of(v)
	if !ok {
		return false
	}
	_, in := l.set[i]
	return in
}

// ---- Many -----------------------------------------------------------------

// ManyIndex maps an indexed value to every key holding it.
type ManyIndex[K comparable, V any, I comparable] struct {
	c  *Cache[K, V]
	of func(V) (I, bool)
	m  map[I]map[K]struct{}
}

// AddMany indexes of(v); many keys may share a value.
func AddMany[K comparable, V any, I comparable](c *Cache[K, V], of func(V) I) *ManyIndex[K, V, I] {
	return AddManyOpt(c, always(of))
}

// AddManyOpt indexes of(v) when it reports ok.
func AddManyOpt[K comparable, V any, I comparable](c *Cache[K, V], of func(V) (I, bool)) *ManyIndex[K, V, I] {
	ix := &ManyIndex[K, V, I]{c: c, of: of, m: map[I]map[K]struct{}{}}
	c.register(ix)
	return ix
}

func (ix *ManyIndex[K, V, I]) add(k K, v V) {
	i, ok := ix.of(v)
	if !ok {
		return
	}
	b := ix.m[i]
	if b == nil {
		b = map[K]struct{}{}
		ix.m[i] = b
	}
	b[k] = struct{}{}
}

func (ix *ManyIndex[K, V, I]) remove(k K, v V) {
	i, ok := ix.of(v)
	if !ok {
		return
	}
	if b := ix.m[i]; b != nil {
		delete(b, k)
		if len(b) == 0 {
			delete(ix.m, i)
		}
	}
}

// Lookup returns every value whose indexed field equals i, in no fixed order.
func (ix *ManyIndex[K, V, I]) Lookup(i I) []V {
	ix.c.mu.RLock()
	defer ix.c.mu.RUnlock()
	return ix.lookupLocked(i)
}

func (ix *ManyIndex[K, V, I]) lookupLocked(i I) []V {
	b := ix.m[i]
	if len(b) == 0 {
		return nil
	}
	out := make([]V, 0, len(b))
	for k := range b {
		out = append(out, ix.c.items[k].v)
	}
	return out
}

// Count returns how many values hold i.
func (ix *ManyIndex[K, V, I]) Count(i I) int {
	ix.c.mu.RLock()
	defer ix.c.mu.RUnlock()
	return len(ix.m[i])
}

// Eq is the lane of values whose indexed field equals i.
func (ix *ManyIndex[K, V, I]) Eq(i I) Lane[K, V] { return ix.In(i) }

// In is the lane of values whose indexed field is any of is.
func (ix *ManyIndex[K, V, I]) In(is ...I) Lane[K, V] {
	return &manyLane[K, V, I]{ix: ix, set: setOf(is)}
}

type manyLane[K comparable, V any, I comparable] struct {
	ix  *ManyIndex[K, V, I]
	set map[I]struct{}
}

func (l *manyLane[K, V, I]) estimate(limit int) int {
	n := 0
	for i := range l.set {
		if n += len(l.ix.m[i]); n > limit {
			break
		}
	}
	return n
}

func (l *manyLane[K, V, I]) each(fn func(K) bool) {
	for i := range l.set {
		for k := range l.ix.m[i] {
			if !fn(k) {
				return
			}
		}
	}
}

func (l *manyLane[K, V, I]) match(v V) bool {
	i, ok := l.ix.of(v)
	if !ok {
		return false
	}
	_, in := l.set[i]
	return in
}

// ---- Range ----------------------------------------------------------------

type rangeBucket[K comparable, I cmp.Ordered] struct {
	i    I
	keys map[K]struct{}
}

// RangeIndex orders values by an indexed field for range lanes and min/max.
type RangeIndex[K comparable, V any, I cmp.Ordered] struct {
	c  *Cache[K, V]
	of func(V) (I, bool)
	t  *btree.BTreeG[*rangeBucket[K, I]]
}

// AddRange indexes of(v) in order.
func AddRange[K comparable, V any, I cmp.Ordered](c *Cache[K, V], of func(V) I) *RangeIndex[K, V, I] {
	return AddRangeOpt(c, always(of))
}

// AddRangeOpt indexes of(v) in order when it reports ok.
func AddRangeOpt[K comparable, V any, I cmp.Ordered](c *Cache[K, V], of func(V) (I, bool)) *RangeIndex[K, V, I] {
	less := func(a, b *rangeBucket[K, I]) bool { return a.i < b.i }
	ix := &RangeIndex[K, V, I]{c: c, of: of, t: btree.NewG(32, less)}
	c.register(ix)
	return ix
}

func (ix *RangeIndex[K, V, I]) add(k K, v V) {
	i, ok := ix.of(v)
	if !ok {
		return
	}
	b, found := ix.t.Get(&rangeBucket[K, I]{i: i})
	if !found {
		b = &rangeBucket[K, I]{i: i, keys: map[K]struct{}{}}
		ix.t.ReplaceOrInsert(b)
	}
	b.keys[k] = struct{}{}
}

func (ix *RangeIndex[K, V, I]) remove(k K, v V) {
	i, ok := ix.of(v)
	if !ok {
		return
	}
	if b, found := ix.t.Get(&rangeBucket[K, I]{i: i}); found {
		delete(b.keys, k)
		if len(b.keys) == 0 {
			ix.t.Delete(b)
		}
	}
}

// Min returns the smallest indexed value.
func (ix *RangeIndex[K, V, I]) Min() (I, bool) {
	ix.c.mu.RLock()
	defer ix.c.mu.RUnlock()
	if b, ok := ix.t.Min(); ok {
		return b.i, true
	}
	var zero I
	return zero, false
}

// Max returns the largest indexed value.
func (ix *RangeIndex[K, V, I]) Max() (I, bool) {
	ix.c.mu.RLock()
	defer ix.c.mu.RUnlock()
	if b, ok := ix.t.Max(); ok {
		return b.i, true
	}
	var zero I
	return zero, false
}

func (ix *RangeIndex[K, V, I]) Gt(i I) *RangeLane[K, V, I] {
	return (&RangeLane[K, V, I]{ix: ix}).Gt(i)
}
func (ix *RangeIndex[K, V, I]) Gte(i I) *RangeLane[K, V, I] {
	return (&RangeLane[K, V, I]{ix: ix}).Gte(i)
}
func (ix *RangeIndex[K, V, I]) Lt(i I) *RangeLane[K, V, I] {
	return (&RangeLane[K, V, I]{ix: ix}).Lt(i)
}
func (ix *RangeIndex[K, V, I]) Lte(i I) *RangeLane[K, V, I] {
	return (&RangeLane[K, V, I]{ix: ix}).Lte(i)
}

// RangeLane is a bounded interval over a RangeIndex; chain the other side
// with Gt/Gte/Lt/Lte: idx.Gte(a).Lt(b) is [a, b).
type RangeLane[K comparable, V any, I cmp.Ordered] struct {
	ix             *RangeIndex[K, V, I]
	lo, hi         I
	hasLo, hasHi   bool
	loIncl, hiIncl bool
}

func (l *RangeLane[K, V, I]) Gt(i I) *RangeLane[K, V, I] {
	l.lo, l.hasLo, l.loIncl = i, true, false
	return l
}

func (l *RangeLane[K, V, I]) Gte(i I) *RangeLane[K, V, I] {
	l.lo, l.hasLo, l.loIncl = i, true, true
	return l
}

func (l *RangeLane[K, V, I]) Lt(i I) *RangeLane[K, V, I] {
	l.hi, l.hasHi, l.hiIncl = i, true, false
	return l
}

func (l *RangeLane[K, V, I]) Lte(i I) *RangeLane[K, V, I] {
	l.hi, l.hasHi, l.hiIncl = i, true, true
	return l
}

func (l *RangeLane[K, V, I]) in(i I) bool {
	if l.hasLo && (i < l.lo || (!l.loIncl && i == l.lo)) {
		return false
	}
	if l.hasHi && (i > l.hi || (!l.hiIncl && i == l.hi)) {
		return false
	}
	return true
}

// walk visits the buckets inside the interval in ascending order.
func (l *RangeLane[K, V, I]) walk(fn func(*rangeBucket[K, I]) bool) {
	visit := func(b *rangeBucket[K, I]) bool {
		if l.hasHi && (b.i > l.hi || (!l.hiIncl && b.i == l.hi)) {
			return false
		}
		if !l.in(b.i) {
			return true // the open low bound itself
		}
		return fn(b)
	}
	if l.hasLo {
		l.ix.t.AscendGreaterOrEqual(&rangeBucket[K, I]{i: l.lo}, visit)
	} else {
		l.ix.t.Ascend(visit)
	}
}

func (l *RangeLane[K, V, I]) estimate(limit int) int {
	n := 0
	l.walk(func(b *rangeBucket[K, I]) bool {
		n += len(b.keys)
		return n <= limit
	})
	return n
}

func (l *RangeLane[K, V, I]) each(fn func(K) bool) {
	l.walk(func(b *rangeBucket[K, I]) bool {
		for k := range b.keys {
			if !fn(k) {
				return false
			}
		}
		return true
	})
}

func (l *RangeLane[K, V, I]) match(v V) bool {
	i, ok := l.ix.of(v)
	return ok && l.in(i)
}

// ---- Or -------------------------------------------------------------------

// Or is the union of lanes.
func Or[K comparable, V any](lanes ...Lane[K, V]) Lane[K, V] { return orLane[K, V](lanes) }

type orLane[K comparable, V any] []Lane[K, V]

func (o orLane[K, V]) estimate(limit int) int {
	n := 0
	for _, l := range o {
		if n += l.estimate(limit - n); n > limit {
			break
		}
	}
	return n
}

func (o orLane[K, V]) each(fn func(K) bool) {
	if len(o) == 1 {
		o[0].each(fn)
		return
	}
	seen := map[K]struct{}{}
	for _, l := range o {
		stop := false
		l.each(func(k K) bool {
			if _, dup := seen[k]; dup {
				return true
			}
			seen[k] = struct{}{}
			if !fn(k) {
				stop = true
				return false
			}
			return true
		})
		if stop {
			return
		}
	}
}

func (o orLane[K, V]) match(v V) bool {
	for _, l := range o {
		if l.match(v) {
			return true
		}
	}
	return false
}

func setOf[I comparable](is []I) map[I]struct{} {
	s := make(map[I]struct{}, len(is))
	for _, i := range is {
		s[i] = struct{}{}
	}
	return s
}
