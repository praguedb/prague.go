package prague_test

import (
	"cmp"
	"slices"
	"testing"

	"github.com/praguedb/prague.go"
)

type item struct {
	ID    int
	Brand string
	Date  int64
	Code  *string
}

func (i *item) CacheKey() int { return i.ID }

func str(s string) *string { return &s }

func fixture(t *testing.T) (*prague.Cache[int, *item], *prague.ManyIndex[int, *item, string], *prague.RangeIndex[int, *item, int64], *prague.UniqueIndex[int, *item, string]) {
	t.Helper()
	c := prague.New[int, *item]("items")
	// Data before indexes: registration backfills.
	c.Upsert(&item{ID: 1, Brand: "a", Date: 10, Code: str("x1")}, 0)
	brand := prague.AddMany(c, func(i *item) string { return i.Brand })
	date := prague.AddRange(c, func(i *item) int64 { return i.Date })
	code := prague.AddUniqueOpt(c, func(i *item) (string, bool) {
		if i.Code == nil {
			return "", false
		}
		return *i.Code, true
	})
	for _, it := range []*item{
		{ID: 2, Brand: "a", Date: 20},
		{ID: 3, Brand: "b", Date: 30, Code: str("x3")},
		{ID: 4, Brand: "b", Date: 40},
		{ID: 5, Brand: "c", Date: 50},
	} {
		c.Upsert(it, 0)
	}
	return c, brand, date, code
}

func ids(items []*item) []int {
	out := make([]int, len(items))
	for i, it := range items {
		out[i] = it.ID
	}
	slices.Sort(out)
	return out
}

func TestQueryLanes(t *testing.T) {
	c, brand, date, code := fixture(t)
	cases := []struct {
		name string
		q    *prague.Query[int, *item]
		want []int
	}{
		{"all", c.Query(), []int{1, 2, 3, 4, 5}},
		{"many eq", c.Query().With(brand.Eq("b")), []int{3, 4}},
		{"many in", c.Query().With(brand.In("a", "c")), []int{1, 2, 5}},
		{"range [20,40)", c.Query().With(date.Gte(20).Lt(40)), []int{2, 3}},
		{"range (20,40]", c.Query().With(date.Gt(20).Lte(40)), []int{3, 4}},
		{"range open high", c.Query().With(date.Gt(30)), []int{4, 5}},
		{"intersect", c.Query().With(brand.Eq("b"), date.Lt(35)), []int{3}},
		{"empty lane", c.Query().With(brand.Eq("zzz"), date.Gte(0)), nil},
		{"unique", c.Query().With(code.Eq("x3")), []int{3}},
		{"or", c.Query().With(prague.Or(brand.Eq("c"), code.Eq("x1"))), []int{1, 5}},
		{"where", c.Query().With(brand.In("a", "b")).Where(func(i *item) bool { return i.ID%2 == 0 }), []int{2, 4}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := tc.q.Execute()
			if got := ids(r.Items); !slices.Equal(got, tc.want) && !(len(got) == 0 && len(tc.want) == 0) {
				t.Fatalf("got %v want %v", got, tc.want)
			}
			if r.TotalCount != len(tc.want) {
				t.Fatalf("TotalCount %d want %d", r.TotalCount, len(tc.want))
			}
		})
	}
}

func TestSortPageCount(t *testing.T) {
	c, _, date, _ := fixture(t)
	byDateDesc := func(a, b *item) int { return cmp.Compare(b.Date, a.Date) }
	r := c.Query().With(date.Gte(0)).Sort(byDateDesc).Page(1, 2).Execute()
	if got := []int{r.Items[0].ID, r.Items[1].ID}; !slices.Equal(got, []int{4, 3}) || r.TotalCount != 5 {
		t.Fatalf("page %v total %d", got, r.TotalCount)
	}
	if n := c.Query().With(date.Lt(30)).Count(); n != 2 {
		t.Fatalf("count %d", n)
	}
	first, ok := c.Query().Sort(byDateDesc).First()
	if !ok || first.ID != 5 {
		t.Fatalf("first %v", first)
	}
	if mn, _ := date.Min(); mn != 10 {
		t.Fatalf("min %d", mn)
	}
}

func TestUpdatesMoveIndexEntries(t *testing.T) {
	c, brand, date, code := fixture(t)
	if u := c.Upsert(&item{ID: 3, Brand: "a", Date: 5}, 0); u != prague.Update {
		t.Fatalf("update type %v", u)
	}
	if got := ids(c.Query().With(brand.Eq("b")).Execute().Items); !slices.Equal(got, []int{4}) {
		t.Fatalf("old bucket still holds 3: %v", got)
	}
	if got := ids(c.Query().With(date.Lt(10)).Execute().Items); !slices.Equal(got, []int{3}) {
		t.Fatalf("range not moved: %v", got)
	}
	if _, ok := code.Get("x3"); ok {
		t.Fatal("unique value survived its removal")
	}
	if _, ok := c.Remove(4); !ok || brand.Count("b") != 0 || c.Len() != 4 {
		t.Fatalf("remove: count %d len %d", brand.Count("b"), c.Len())
	}
}

func TestApplyFingerprintSuppressesSame(t *testing.T) {
	c := prague.New[int, *item]("x")
	if u, _ := c.Apply(1, &item{ID: 1}, 77, 1); u != prague.Add {
		t.Fatal(u)
	}
	if u, _ := c.Apply(1, &item{ID: 1}, 77, 2); u != prague.Same {
		t.Fatal(u)
	}
	if ts, _ := c.LastUpdated(1); ts != 1 {
		t.Fatalf("Same must not touch the entry, ts=%d", ts)
	}
	if u, old := c.Apply(1, &item{ID: 1, Brand: "n"}, 78, 3); u != prague.Update || old.Brand != "" {
		t.Fatal(u)
	}
}

type offer struct {
	ID, ProductID int
}

func TestJoins(t *testing.T) {
	products, _, _, _ := fixture(t)
	offers := prague.NewFunc("offers", func(o *offer) int { return o.ID })
	byProduct := prague.AddMany(offers, func(o *offer) int { return o.ProductID })
	for _, o := range []*offer{{10, 1}, {11, 1}, {12, 3}, {13, 99}} {
		offers.Upsert(o, 0)
	}

	all := offers.Query().Sort(func(a, b *offer) int { return cmp.Compare(a.ID, b.ID) }).Execute().Items
	one := prague.JoinOne(all, products, func(o *offer) (int, bool) { return o.ProductID, true }, false)
	if len(one) != 4 || !one[0].Found || one[0].Right.ID != 1 || one[3].Found {
		t.Fatalf("outer JoinOne: %+v", one)
	}
	if inner := prague.JoinOne(all, products, func(o *offer) (int, bool) { return o.ProductID, true }, true); len(inner) != 3 {
		t.Fatalf("inner JoinOne len %d", len(inner))
	}

	ps := products.Query().With().Sort(func(a, b *item) int { return cmp.Compare(a.ID, b.ID) }).Execute().Items
	many := prague.JoinMany(ps, byProduct, func(p *item) int { return p.ID }, true)
	if len(many) != 2 || many[0].Left.ID != 1 || len(many[0].Right) != 2 || many[1].Left.ID != 3 {
		t.Fatalf("inner JoinMany: %+v", many)
	}
}

func TestResolveTopic(t *testing.T) {
	t.Setenv("PRAGUE_ENV", "prod")
	got, err := prague.ResolveTopic("Cache.[v:brandId].BetType.[e:PRAGUE_ENV]", map[string]string{"brandId": "7"})
	if err != nil || got != "Cache.7.BetType.prod" {
		t.Fatalf("%q %v", got, err)
	}
	if _, err := prague.ResolveTopic("Cache.[v:missing]", nil); err == nil {
		t.Fatal("missing var must be an error")
	}
}
