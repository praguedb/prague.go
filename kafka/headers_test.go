package kafka_test

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/praguedb/prague.go"
	"github.com/praguedb/prague.go/internal/shop"
	"github.com/praguedb/prague.go/kafka"
)

func raw32(v int32) []byte { return binary.LittleEndian.AppendUint32(nil, uint32(v)) }

func mp(t *testing.T, v any) []byte {
	t.Helper()
	b, err := msgpack.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func hdr(kv ...any) []kgo.RecordHeader {
	var hs []kgo.RecordHeader
	for i := 0; i < len(kv); i += 2 {
		v, ok := kv[i+1].([]byte)
		if !ok {
			v = []byte(kv[i+1].(string))
		}
		hs = append(hs, kgo.RecordHeader{Key: kv[i].(string), Value: v})
	}
	return hs
}

// Prague's header filter semantics, one record per rule, and enrichment from
// the headers that pass: Region and Tier are reader tags no fixture fills.
func TestHeaderFiltersAndEnrichment(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, productsTopic))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	brokers := cluster.ListenAddrs()
	prod, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.DefaultProduceTopic(productsTopic))
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	value := fixture(t, "product-full.value")
	send := func(key int64, tombstone bool, hs []kgo.RecordHeader) {
		t.Helper()
		r := &kgo.Record{Key: mp(t, key), Value: value, Headers: hs}
		if tombstone {
			r.Value = nil
		}
		if err := prod.ProduceSync(ctx, r).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}

	send(1, false, hdr("x-region", "eu", "Tier", raw32(1)))                       // kept; Tier as raw little-endian
	send(2, false, hdr("x-region", "us", "Tier", mp(t, int32(2))))                // kept; Tier as MessagePack
	send(3, false, hdr("x-region", "moon", "Tier", raw32(1)))                     // Equals rejects
	send(4, false, hdr("x-region", "eu"))                                         // Exists rejects
	send(5, false, hdr("x-region", "eu", "Tier", raw32(9)))                       // the predicate rejects
	send(6, false, hdr("x-region", "eu", "Tier", []byte{0xc0}))                   // nil: passOnNull keeps
	send(7, false, hdr("x-region", "eu", "x-region", "moon", "Tier", raw32(1)))   // every occurrence is checked
	send(8, false, hdr("x-region", "eu", "Tier", raw32(1), "Tier", "not-an-int")) // the predicate rejects a Tier that does not decode
	send(1, true, hdr("x-region", "moon"))                                        // a rejected tombstone is dropped
	send(2, true, nil)                                                            // a tombstone needs no required header

	products := prague.New[int64, *shop.Product]("products")
	events := make(chan event, 16)
	c := kafka.NewConsumer(kafka.Config{Brokers: brokers, OnError: func(err error) { t.Errorf("consumer: %v", err) }})
	must(t, kafka.Register(c, productsTopic, products,
		kafka.WithHeaderFilter[int64, *shop.Product](
			kafka.HeaderEquals("x-region", "eu", "us"),
			kafka.HeaderExists("Tier"),
			kafka.HeaderPredicate("Tier", func(v int32) bool { return v < 3 }, true),
		),
		kafka.WithAfterHandler(func(u prague.UpdateType, k int64, _, _ *shop.Product) { events <- event{u, k} })))
	runErr := make(chan error, 1)
	go func() { runErr <- c.Run(ctx) }()
	must(t, c.WaitLoaded(ctx))

	var keys []int64
	products.Each(func(k int64, _ *shop.Product) bool { keys = append(keys, k); return true })
	if products.Len() != 2 {
		t.Fatalf("kept %v, want 1 and 6", keys)
	}
	for _, k := range []int64{1, 6} {
		if _, ok := products.Get(k); !ok {
			t.Fatalf("kept %v, want 1 and 6", keys)
		}
	}
	if p, _ := products.Get(1); p.Region != "eu" || p.Tier != 1 || p.Name != "Widget" {
		t.Fatalf("product 1 enriched as %q/%d: %+v", p.Region, p.Tier, p)
	}

	// Live. The same value under a new Tier is a change; the same value under
	// the same headers is Same. Product 10 is the sentinel that proves the
	// Same in between raised nothing.
	send(1, false, hdr("x-region", "eu", "Tier", raw32(2)))
	send(1, false, hdr("x-region", "eu", "Tier", raw32(2)))
	send(10, false, hdr("x-region", "us", "Tier", raw32(1)))
	for _, want := range []event{{prague.Update, 1}, {prague.Add, 10}} {
		select {
		case got := <-events:
			if got != want {
				t.Fatalf("event %+v, want %+v", got, want)
			}
		case <-ctx.Done():
			t.Fatalf("waiting for %+v: %v", want, ctx.Err())
		}
	}
	if p, _ := products.Get(1); p.Tier != 2 {
		t.Fatalf("header-only change not applied: tier %d", p.Tier)
	}
	cancel()
	<-runErr
}
