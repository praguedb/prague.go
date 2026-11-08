package showfeed_test

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/praguedb/prague.go/internal/showfeed"
	"github.com/praguedb/prague.go/wire"
)

// The fixtures were written by the producer's own C# contract types through
// Prague's CacheSerde. Those types carry two members schemas/showfeed.prague
// reserves -- the per-producer stamps map and a tier line's attributes -- so
// every row here also proves the reader skips a map and a list of mixed
// MessagePack types it has no member for.
func decode[T any](t *testing.T, name string) T {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "testdata", "showfeed", name))
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

const (
	show = 7312345678901234
	act1 = 7301234567890123
	act2 = 7301234567890456
)

var tour = wire.MustUUID("5e1f0c2a-7b3d-4c8e-9f10-a1b2c3d4e5f6")

func TestShow(t *testing.T) {
	key := decode[showfeed.ShowKey](t, "show.key")
	eq(t, "show.key", key, showfeed.ShowKey{Source: 1, ShowId: show})
	row := decode[showfeed.SourceShowInfo](t, "show.value")
	eq(t, "show", row, showfeed.SourceShowInfo{
		Key: key, ShowId: show, GenreId: 3, CountryId: 44, TourId: tour, StartTimeUnixMs: 1_783_197_000_000,
		Status: showfeed.ShowStatusLive, HeadlinerId: act1, OpenerId: act2, ProviderShowId: "src:show:73664876",
	})
	eq(t, "show cache key", row.CacheKey(), key)
}

func TestLiveShow(t *testing.T) {
	eq(t, "live-show", decode[showfeed.SourceLiveShowInfo](t, "live-show.value"), showfeed.SourceLiveShowInfo{
		Key: showfeed.ShowKey{Source: 1, ShowId: show}, ShowId: show, Status: showfeed.ShowStatusLive, StatusCode: 6,
		HeadlinerScore: 2, OpenerScore: 1,
		Sets:         []showfeed.SetScore{{Number: 1, Headliner: 1, Opener: 0}, {Number: 2, Headliner: 1, Opener: 1}},
		ClockSeconds: 2710,
	})

	// A stage-health row: keyed [source, -1 - stage], the rest defaults.
	key := decode[showfeed.ShowKey](t, "live-health.key")
	eq(t, "live-health.key", key, showfeed.ShowKey{Source: 1, ShowId: -2})
	eq(t, "live-health", decode[showfeed.SourceLiveShowInfo](t, "live-health.value"), showfeed.SourceLiveShowInfo{
		Key: key, ShowId: -2, Status: showfeed.ShowStatusUnknown, HeadlinerScore: -1, OpenerScore: -1, ClockSeconds: -1,
		Sets:   []showfeed.SetScore{},
		Health: &showfeed.StageHealth{Stage: showfeed.SaleStageLive, State: showfeed.SourceStateDown, SinceUnixMs: 1_783_190_000_456},
	})
}

func TestTicketType(t *testing.T) {
	key := decode[showfeed.TicketTypeKey](t, "ticket-type.key")
	eq(t, "ticket-type.key", key, showfeed.TicketTypeKey{Source: 1, ShowId: show, TicketTypeId: 2})
	row := decode[showfeed.SourceTicketTypeInfo](t, "ticket-type.value")
	eq(t, "ticket-type cache key", row.CacheKey(), key)
	if row.ShowId != show || row.Stage != showfeed.SaleStageLive || len(row.Lines) != 4 {
		t.Fatalf("ticket-type: %+v", row)
	}
	q := func(price float32, s showfeed.QuoteStatus) showfeed.Quote {
		return showfeed.Quote{Price: float64(price), Status: s}
	}
	eq(t, "line 0", row.Lines[0], showfeed.TierLine{
		Slot: 0, Status: showfeed.SaleStatusOpen, Favourite: true,
		Quotes: []showfeed.Quote{q(1.85, showfeed.QuoteStatusActive), q(1.95, showfeed.QuoteStatusActive)},
	})
	eq(t, "line 1", row.Lines[1], showfeed.TierLine{
		Slot: 0, Status: showfeed.SaleStatusSuspended,
		Quotes: []showfeed.Quote{q(2.6, showfeed.QuoteStatusInactive), q(1.45, showfeed.QuoteStatusActive)},
	})
	eq(t, "line 2", row.Lines[2], showfeed.TierLine{
		Slot: 1, Status: showfeed.SaleStatusSettled,
		Quotes: []showfeed.Quote{
			q(3.25, showfeed.QuoteStatusWon), q(2.2, showfeed.QuoteStatusLost),
			q(5, showfeed.QuoteStatusVoid), q(0, showfeed.QuoteStatusHalfLost),
		},
		Settlement: showfeed.LineSettlementSettled, Certainty: showfeed.SettlementCertaintyConfirmed,
	})
	eq(t, "line 3", row.Lines[3], showfeed.TierLine{Slot: 2, Status: showfeed.SaleStatusOpen, Quotes: []showfeed.Quote{}})
}
