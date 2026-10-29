package prague

import (
	"math"
	"slices"
)

// Query is a one-shot read: index lanes AND together, Where filters what
// survives, Sort orders, Page slices. Build it with Cache.Query.
type Query[K comparable, V any] struct {
	c     *Cache[K, V]
	lanes []Lane[K, V]
	where []func(V) bool
	cmp   func(a, b V) int
	skip  int
	take  int
}

// Results is a query's page plus the number of matches before paging.
type Results[V any] struct {
	Items      []V
	TotalCount int
}

func (c *Cache[K, V]) Query() *Query[K, V] {
	return &Query[K, V]{c: c, take: math.MaxInt}
}

// With adds lanes. A value must match every lane.
func (q *Query[K, V]) With(lanes ...Lane[K, V]) *Query[K, V] {
	q.lanes = append(q.lanes, lanes...)
	return q
}

// Where adds a residual filter, run on candidates after the lanes.
func (q *Query[K, V]) Where(f func(V) bool) *Query[K, V] {
	q.where = append(q.where, f)
	return q
}

// Sort orders the result. Ties keep no particular order; make cmp total if
// pages must be stable.
func (q *Query[K, V]) Sort(cmp func(a, b V) int) *Query[K, V] {
	q.cmp = cmp
	return q
}

// Page keeps take results after skipping skip.
func (q *Query[K, V]) Page(skip, take int) *Query[K, V] {
	q.skip, q.take = max(skip, 0), max(take, 0)
	return q
}

// Execute runs the query.
func (q *Query[K, V]) Execute() Results[V] {
	q.c.mu.RLock()
	defer q.c.mu.RUnlock()
	return q.executeLocked()
}

// Count returns the number of matches without materializing them.
func (q *Query[K, V]) Count() int {
	q.c.mu.RLock()
	defer q.c.mu.RUnlock()
	n := 0
	q.scan(func(V) bool { n++; return true })
	return n
}

// First returns one match, or the first by Sort order when set.
func (q *Query[K, V]) First() (V, bool) {
	r := q.Page(0, 1).Execute()
	if len(r.Items) == 0 {
		var zero V
		return zero, false
	}
	return r.Items[0], true
}

func (q *Query[K, V]) executeLocked() Results[V] {
	var all []V
	// Unsorted: collect only the page, count the rest.
	if q.cmp == nil {
		n := 0
		q.scan(func(v V) bool {
			if n >= q.skip && n-q.skip < q.take {
				all = append(all, v)
			}
			n++
			return true
		})
		return Results[V]{Items: all, TotalCount: n}
	}
	q.scan(func(v V) bool { all = append(all, v); return true })
	slices.SortStableFunc(all, q.cmp)
	total := len(all)
	lo := min(q.skip, total)
	hi := lo + min(q.take, total-lo)
	return Results[V]{Items: all[lo:hi], TotalCount: total}
}

// scan yields every match. The plan: drive from the lane with the fewest
// candidates, check the others by value; no lanes means a full scan. An empty
// driving lane ends the query before anything is fetched.
func (q *Query[K, V]) scan(fn func(V) bool) {
	keep := func(v V) bool {
		for _, f := range q.where {
			if !f(v) {
				return false
			}
		}
		return true
	}
	if len(q.lanes) == 0 {
		for _, e := range q.c.items {
			if keep(e.v) && !fn(e.v) {
				return
			}
		}
		return
	}

	drive, best := 0, math.MaxInt
	for i, l := range q.lanes {
		if n := l.estimate(best); n < best {
			drive, best = i, n
		}
		if best == 0 {
			return
		}
	}
	q.lanes[drive].each(func(k K) bool {
		e, ok := q.c.items[k]
		if !ok {
			return true
		}
		for i, l := range q.lanes {
			if i != drive && !l.match(e.v) {
				return true
			}
		}
		if !keep(e.v) {
			return true
		}
		return fn(e.v)
	})
}
