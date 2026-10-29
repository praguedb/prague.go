package shop_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/praguedb/prague.go/internal/shop"
)

// The fixtures are written by Prague (.NET)'s CacheSerde from praguec's C#
// emission of schemas/shop.prague; this package is praguec's Go emission of the
// same file.
func fixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "testdata", "interop", name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func decode[T any](t *testing.T, name string) T {
	t.Helper()
	var v T
	if err := msgpack.Unmarshal(fixture(t, name), &v); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return v
}

func TestProductFull(t *testing.T) {
	if k := decode[int64](t, "product-full.key"); k != 42 {
		t.Fatalf("key = %d", k)
	}
	p := decode[*shop.Product](t, "product-full.value")
	if p.Id != 42 || p.Name != "Widget" || p.Brand == nil || *p.Brand != "Acme" {
		t.Fatalf("scalars: %+v", p)
	}
	if p.Status != shop.StatusActive || p.Override == nil || *p.Override != shop.StatusRetired {
		t.Fatalf("enums: %v %v", p.Status, p.Override)
	}
	if p.Released != 1_767_225_600_000 || p.Stock == nil || *p.Stock != 7 || !p.Published {
		t.Fatalf("numbers: %+v", p)
	}
	if p.Discount == nil || p.Discount.String() != "12.34" {
		t.Fatalf("discount = %v", p.Discount)
	}
	if p.Size == nil || p.Size.Width != 1.5 || p.Size.Height != 2.25 {
		t.Fatalf("size = %+v", p.Size)
	}
	if len(p.Prices) != 2 || p.Prices[0].Currency != "EUR" || p.Prices[0].Amount.String() != "19.99" ||
		p.Prices[1].Amount.String() != "-0.0001" {
		t.Fatalf("prices = %+v", p.Prices)
	}
	if len(p.Tags) != 2 || p.Tags[1] != "sale" {
		t.Fatalf("tags = %v", p.Tags)
	}
	if p.CacheKey() != 42 {
		t.Fatalf("CacheKey = %d", p.CacheKey())
	}
}

func TestProductSparseHasNoPresence(t *testing.T) {
	p := decode[*shop.Product](t, "product-sparse.value")
	if p.Id != 7 || p.Name != "Bare" {
		t.Fatalf("%+v", p)
	}
	if p.Brand != nil || p.Override != nil || p.Stock != nil || p.Discount != nil || p.Size != nil {
		t.Fatalf("absent members decoded as present: %+v", p)
	}
}

func TestOfferCompositeKey(t *testing.T) {
	k := decode[shop.OfferKey](t, "offer.key")
	if k != (shop.OfferKey{ProductId: 42, OfferId: 3}) {
		t.Fatalf("key = %+v", k)
	}
	o := decode[*shop.Offer](t, "offer.value")
	if o.CacheKey() != k || o.OfferId() != 3 || o.Price.Amount.String() != "17.5" || o.UpdatedAt != 99 {
		t.Fatalf("offer = %+v", o)
	}
}

// A view decodes a table's bytes: same tags, fewer members.
func TestViewReadsTableBytes(t *testing.T) {
	s := decode[*shop.SlimProduct](t, "product-full.value")
	if s.Id != 42 || s.Name != "Widget" || s.Status != shop.StatusActive || s.CacheKey() != 42 {
		t.Fatalf("view = %+v", s)
	}
}

// The writer (schemas/shop-writer.prague) is ahead of this reader -- Product
// tags 16 and 17, Price and Dimensions tag 3 -- and behind it, filling tags
// 4..6 that the reader reserves. Every one of those slots must be skipped, at
// the top level and inside nested records and list elements, and nothing
// after a skipped slot may shift.
func TestReaderSkipsWhatTheWriterKnowsAndItDoesNot(t *testing.T) {
	p := decode[*shop.Product](t, "product-writer.value")
	if p.Id != 43 || p.Name != "Gadget" || p.Brand == nil || *p.Brand != "Acme" {
		t.Fatalf("before the reserved slots: %+v", p)
	}
	// Tag 7 comes straight after the three reserved slots the writer filled.
	if p.Status != shop.StatusActive || p.Released != 1_767_225_600_000 || *p.Stock != 3 || p.Discount.String() != "0.5" {
		t.Fatalf("after the reserved slots: %+v", p)
	}
	if p.Size == nil || p.Size.Width != 1 || p.Size.Height != 2 {
		t.Fatalf("struct with an unknown trailing member: %+v", p.Size)
	}
	if len(p.Prices) != 1 || p.Prices[0].Currency != "EUR" || p.Prices[0].Amount.String() != "9.99" {
		t.Fatalf("list of records with an unknown member: %+v", p.Prices)
	}
	// Tag 15 is the last the reader knows; 16 and 17 follow it.
	if len(p.Tags) != 1 || p.Tags[0] != "writer" || !p.Published {
		t.Fatalf("before the unknown trailing slots: %+v", p)
	}
}
