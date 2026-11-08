package catalog_test

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/praguedb/prague.go/internal/catalog"
	"github.com/praguedb/prague.go/wire"
)

// The fixtures were written by the producer's own C# contract types through
// Prague's CacheSerde; this package is praguec's Go emission of
// schemas/catalog.prague, which describes the same wire.
func decode[T any](t *testing.T, name string) T {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "testdata", "catalog", name))
	if err != nil {
		t.Fatal(err)
	}
	var v T
	if err := msgpack.Unmarshal(b, &v); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return v
}

func eq[T any](t *testing.T, name string, got, want T) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%s:\n got %+v\nwant %+v", name, got, want)
	}
}

func ptr[T any](v T) *T { return &v }

var (
	tour    = wire.MustUUID("5e1f0c2a-7b3d-4c8e-9f10-a1b2c3d4e5f6")
	edition = wire.MustUUID("9a8b7c6d-5e4f-4a3b-8c2d-1e0f9a8b7c6d")
)

// DateTime.ToBinary() of UTC instants: ticks with the Utc kind in bit 62.
const (
	created  = 5250714242427387904 // 2026-01-01
	updated  = 5250715106427387904 // 2026-01-02
	starting = 5250873956427387904 // 2026-07-04 20:30
)

const (
	act1      = 7301234567890123
	act2      = 7301234567890456
	performer = 7309876543210001
	venue     = 7305555555555501
	show      = 7312345678901234
)

func TestKnowledgeBase(t *testing.T) {
	eq(t, "genre.key", decode[int32](t, "genre.key"), 3)
	eq(t, "genre", decode[catalog.GenreInfo](t, "genre.value"), catalog.GenreInfo{
		Id: 3, Slug: "indie-rock", Name: "Indie Rock", Version: 2, CreatedAt: created, UpdatedAt: updated,
	})
	src := decode[catalog.SourceInfo](t, "source.value")
	eq(t, "source", src, catalog.SourceInfo{
		Id: 1, Name: "Primary Source", Version: 1, CreatedAt: created, UpdatedAt: updated,
		Types: catalog.SourceTypesPricing | catalog.SourceTypesState,
	})
	eq(t, "country", decode[catalog.CountryInfo](t, "country.value"), catalog.CountryInfo{
		Id: 44, Name: "Portugal", Version: 1, CreatedAt: created, UpdatedAt: updated, CountryCode: ptr("PRT"),
	})
	eq(t, "country-grouping", decode[catalog.CountryInfo](t, "country-grouping.value"), catalog.CountryInfo{
		Id: 1, Name: "International", IsArchived: true, Version: 3, CreatedAt: created, UpdatedAt: updated,
	})
	eq(t, "tour.key", decode[wire.UUID](t, "tour.key"), tour)
	eq(t, "tour", decode[catalog.TourInfo](t, "tour.value"), catalog.TourInfo{
		Id: tour, GenreId: 3, CountryId: 44, Name: "Summer Arena Tour", Version: 1, CreatedAt: created, UpdatedAt: updated,
	})
	eq(t, "act-full", decode[catalog.ActInfo](t, "act-full.value"), catalog.ActInfo{
		Id: act1, GenreId: 3, Name: "The Nightingales", ShortName: ptr("Nightingales"), Abbreviation: ptr("NGT"),
		CountryId: ptr[int32](44), Gender: catalog.GenderMixed, AgeGroup: ptr("U21"), ActType: catalog.ActTypeBand,
		IsVirtual: true, Version: 4, CreatedAt: created, UpdatedAt: updated,
	})
	eq(t, "act-sparse", decode[catalog.ActInfo](t, "act-sparse.value"), catalog.ActInfo{
		Id: act2, GenreId: 3, Name: "The Larks", ActType: catalog.ActTypeSolo, Version: 1, CreatedAt: created, UpdatedAt: updated,
	})
	eq(t, "performer", decode[catalog.PerformerInfo](t, "performer.value"), catalog.PerformerInfo{
		Id: performer, GenreId: 3, Name: "Rosa Almeida", ShortName: ptr("R. Almeida"), CountryId: ptr[int32](44),
		Gender: catalog.GenderFemale, DateOfBirth: ptr[int32](728064), Role: ptr("vocals"),
		Version: 1, CreatedAt: created, UpdatedAt: updated,
	})
	eq(t, "venue", decode[catalog.VenueInfo](t, "venue.value"), catalog.VenueInfo{
		Id: venue, Name: "Riverside Hall", City: ptr("Porto"), CountryId: ptr[int32](44), Capacity: ptr[int32](12000),
		Version: 1, CreatedAt: created, UpdatedAt: updated,
	})
	eq(t, "edition", decode[catalog.EditionInfo](t, "edition.value"), catalog.EditionInfo{
		Id: edition, TourId: tour, Name: "Summer Arena Tour 2026", Year: ptr("2026"), StartDate: ptr[int32](739767),
		Version: 1, CreatedAt: created, UpdatedAt: updated,
	})
	got := decode[catalog.ShowInfo](t, "show.value")
	eq(t, "show", got, catalog.ShowInfo{
		Id: show, GenreId: 3, CountryId: 44, TourId: tour, EditionId: &edition, VenueId: ptr[int64](venue),
		HeadlinerId: act1, OpenerId: act2, ScheduledStart: ptr[int64](starting),
		Version: 2, CreatedAt: created, UpdatedAt: updated,
	})
	eq(t, "show cache key", got.CacheKey(), int64(show))
}

func TestMappings(t *testing.T) {
	key := decode[catalog.MappingKey](t, "act-mapping.key")
	eq(t, "act-mapping.key", key, catalog.MappingKey{SourceId: 1, ProviderId: "src:act:4698"})
	act := decode[catalog.ActMappingInfo](t, "act-mapping.value")
	eq(t, "act-mapping", act, catalog.ActMappingInfo{
		Key: key, ActId: act1, Version: 2, CreatedAt: created, UpdatedAt: updated,
		ProviderName: ptr("Nightingales"), Method: catalog.MappingMethodAlias, Score: ptr(0.75),
	})
	eq(t, "act-mapping cache key", act.CacheKey(), key)
	eq(t, "genre-mapping", decode[catalog.GenreMappingInfo](t, "genre-mapping.value"), catalog.GenreMappingInfo{
		Key: catalog.MappingKey{SourceId: 1, ProviderId: "src:genre:9"}, GenreId: 3, Version: 1, CreatedAt: created, UpdatedAt: updated,
		ProviderName: ptr("Indie"), Method: catalog.MappingMethodName, Score: ptr(1.0),
	})
	eq(t, "edition-mapping", decode[catalog.EditionMappingInfo](t, "edition-mapping.value"), catalog.EditionMappingInfo{
		Key: catalog.MappingKey{SourceId: 1, ProviderId: "src:edition:118"}, EditionId: edition, Version: 1,
		CreatedAt: created, UpdatedAt: updated, Method: catalog.MappingMethodManual,
	})
	eq(t, "show-mapping", decode[catalog.ShowMappingInfo](t, "show-mapping.value"), catalog.ShowMappingInfo{
		Key: catalog.MappingKey{SourceId: 1, ProviderId: "src:show:73664876"}, ShowId: show, Version: 1,
		CreatedAt: created, UpdatedAt: updated, ProviderName: ptr("Nightingales v Larks"),
		Method: catalog.MappingMethodSchedule, Score: ptr(0.5),
	})
}

func TestTicketCatalogue(t *testing.T) {
	eq(t, "tier-family.key", decode[int32](t, "tier-family.key"), 2)
	eq(t, "tier-family", decode[catalog.TierFamilyInfo](t, "tier-family.value"), catalog.TierFamilyInfo{
		Id: 2, Slug: "over-under", Name: "Over/Under", Description: "Two tiers either side of a line.",
		Version: 1, CreatedAt: created, UpdatedAt: updated,
	})
	eq(t, "ticket-type", decode[catalog.TicketTypeInfo](t, "ticket-type.value"), catalog.TicketTypeInfo{
		Id: 7320000000000002, GenreId: 3, Slug: "total", Revision: 2, Name: "Total", FamilyId: 2,
		Offered:          catalog.SaleWindowBoth,
		Slots:            []catalog.TicketSlot{{Role: catalog.SlotRoleOver}, {Role: catalog.SlotRoleUnder}},
		LineAttributes:   []catalog.AttributeField{{Name: "total", Kind: catalog.AttributeKindLine}},
		PublishedAt:      updated,
		Version:          3,
		CreatedAt:        created,
		UpdatedAt:        updated,
		TicketAttributes: []catalog.AttributeField{{Name: "set", Kind: catalog.AttributeKindSet}},
	})
	picks := decode[catalog.TicketTypeInfo](t, "ticket-type-picks.value")
	eq(t, "ticket-type-picks slots", picks.Slots, []catalog.TicketSlot{
		{Role: catalog.SlotRolePick, Name: ptr("1X")}, {Role: catalog.SlotRolePick, Name: ptr("X2")}, {Role: catalog.SlotRoleHeadliner},
	})
	eq(t, "ticket-type-picks set", picks.SetId, ptr[int32](4))
	eq(t, "ticket-type-picks lines", len(picks.LineAttributes), 0)

	key := decode[catalog.SetKey](t, "set.key")
	eq(t, "set.key", key, catalog.SetKey{GenreId: 3, Id: 5})
	set := decode[catalog.SetInfo](t, "set.value")
	eq(t, "set", set, catalog.SetInfo{
		Key: key, Slug: "encore", Name: "Encore", ShortName: "ENC", Kind: catalog.SetKindPhase, IsOnStage: true,
		Position: 5, Version: 1, CreatedAt: created, UpdatedAt: updated,
	})
}

func TestMappingRequest(t *testing.T) {
	eq(t, "mapping-request.key", decode[string](t, "mapping-request.key"), "1:event:src:show:73664876")
	eq(t, "mapping-request", decode[catalog.MappingRequest](t, "mapping-request.value"), catalog.MappingRequest{
		SourceId: 1, Kind: catalog.MappingKindShow, ProviderId: "src:show:73664876", Name: ptr("Nightingales v Larks"),
		GenreProviderId: ptr("src:genre:9"), CountryProviderId: ptr("src:country:44"), TourProviderId: ptr("src:tour:17"),
		CountryCode: ptr("PRT"), City: ptr("Porto"), HeadlinerProviderId: ptr("src:act:4698"),
		OpenerProviderId: ptr("src:act:4702"), EditionProviderId: ptr("src:edition:118"),
		VenueProviderId: ptr("src:venue:31"), StartTimeUnixMs: 1_783_197_000_000,
	})
}
