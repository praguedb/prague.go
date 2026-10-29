package prague

// One is a left row with the right value its foreign key names, if any.
type One[L, R any] struct {
	Left  L
	Right R
	Found bool
}

// Many is a left row with every right value that refers to it.
type Many[L, R any] struct {
	Left  L
	Right []R
}

// JoinOne pairs each row with right's value under fk(row): the many-to-one or
// one-to-one direction of a `[references(T)]`. Rows whose fk reports !ok, or
// names nothing, get Found = false; pass inner to drop them instead.
//
// The right cache is read under one lock for the whole batch, but not under
// the lock that produced rows: two caches fed by two topics are never a
// transaction, as in Prague (.NET).
func JoinOne[L any, RK comparable, R any](rows []L, right *Cache[RK, R], fk func(L) (RK, bool), inner bool) []One[L, R] {
	out := make([]One[L, R], 0, len(rows))
	right.mu.RLock()
	defer right.mu.RUnlock()
	for _, l := range rows {
		j := One[L, R]{Left: l}
		if k, ok := fk(l); ok {
			if e, found := right.items[k]; found {
				j.Right, j.Found = e.v, true
			}
		}
		if inner && !j.Found {
			continue
		}
		out = append(out, j)
	}
	return out
}

// JoinMany pairs each row with the right values whose indexed foreign key
// equals key(row): the one-to-many direction, driven by a Many index on the
// referring cache. Pass inner to drop rows with no match.
func JoinMany[L any, RK comparable, R any, I comparable](rows []L, by *ManyIndex[RK, R, I], key func(L) I, inner bool) []Many[L, R] {
	out := make([]Many[L, R], 0, len(rows))
	by.c.mu.RLock()
	defer by.c.mu.RUnlock()
	for _, l := range rows {
		rs := by.lookupLocked(key(l))
		if inner && len(rs) == 0 {
			continue
		}
		out = append(out, Many[L, R]{Left: l, Right: rs})
	}
	return out
}
