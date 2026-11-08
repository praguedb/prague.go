package kafka_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/praguedb/prague.go"
	"github.com/praguedb/prague.go/internal/shop"
	"github.com/praguedb/prague.go/kafka"
)

const (
	productsTopic = "Cache.shop.Product"
	offersTopic   = "Cache.shop.Offer"
)

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "testdata", "interop", name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

type event struct {
	u   prague.UpdateType
	key int64
}

func TestConsumerLoadThenLive(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, productsTopic), kfake.SeedTopics(3, offersTopic))
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
	send := func(topic string, key, value []byte) {
		t.Helper()
		if err := prod.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: key, Value: value}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}

	fullK, fullV := fixture(t, "product-full.key"), fixture(t, "product-full.value")
	sparseK, sparseV := fixture(t, "product-sparse.key"), fixture(t, "product-sparse.value")
	// Load phase: 42 twice (the second is Same), 7 then its tombstone.
	send(productsTopic, fullK, fullV)
	send(productsTopic, sparseK, sparseV)
	send(productsTopic, fullK, fullV)
	send(productsTopic, sparseK, nil)
	send(offersTopic, fixture(t, "offer.key"), fixture(t, "offer.value"))

	products := prague.New[int64, *shop.Product]("products")
	byStatus := prague.AddMany(products, func(p *shop.Product) shop.Status { return p.Status })
	offers := prague.New[shop.OfferKey, *shop.Offer]("offers")

	events := make(chan event, 16)
	c := kafka.NewConsumer(kafka.Config{
		Brokers: brokers,
		Vars:    map[string]string{"tenant": "shop"},
		OnError: func(err error) { t.Errorf("consumer: %v", err) },
	})
	must(t, kafka.Register(c, "Cache.[v:tenant].Product", products,
		kafka.WithAfterHandler(func(u prague.UpdateType, k int64, _, _ *shop.Product) { events <- event{u, k} })))
	must(t, kafka.Register(c, "Cache.[v:tenant].Offer", offers))

	runErr := make(chan error, 1)
	go func() { runErr <- c.Run(ctx) }()
	must(t, c.WaitLoaded(ctx))

	if products.Len() != 1 {
		t.Fatalf("products after load: %d", products.Len())
	}
	if p, ok := products.Get(42); !ok || p.Name != "Widget" || len(byStatus.Lookup(shop.StatusActive)) != 1 {
		t.Fatalf("product 42 after load: %+v", p)
	}
	if o, ok := offers.Get(shop.OfferKey{ProductId: 42, OfferId: 3}); !ok || o.Price.Currency != "EUR" {
		t.Fatalf("offer after load: %+v", o)
	}
	select {
	case e := <-events:
		t.Fatalf("after-handler fired during load: %+v", e)
	default:
	}

	// Live phase.
	send(productsTopic, fullK, fullV) // Same: no event
	send(productsTopic, sparseK, sparseV)
	send(productsTopic, fullK, nil)
	for _, want := range []event{{prague.Add, 7}, {prague.Delete, 42}} {
		select {
		case got := <-events:
			if got != want {
				t.Fatalf("event %+v want %+v", got, want)
			}
		case <-ctx.Done():
			t.Fatalf("waiting for %+v: %v", want, ctx.Err())
		}
	}
	if _, ok := products.Get(42); ok || products.Len() != 1 {
		t.Fatalf("tombstone not applied: len %d", products.Len())
	}

	cancel()
	if err := <-runErr; err != context.Canceled {
		t.Fatalf("Run: %v", err)
	}
}

func TestConsumerRejectsMissingTopic(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	c := kafka.NewConsumer(kafka.Config{Brokers: cluster.ListenAddrs()})
	must(t, kafka.Register(c, "nope", prague.New[int64, *shop.Product]("p")))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.Run(ctx); err == nil {
		t.Fatal("Run must fail on a topic that does not exist")
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// A transactional producer ends the log with a commit or abort marker, which
// is an offset no data record holds. Loading must reach past it, and the
// records of an aborted transaction must never reach the cache.
func TestConsumerLoadsPastTransactionMarkers(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, productsTopic))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	brokers := cluster.ListenAddrs()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	prod, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.DefaultProduceTopic(productsTopic), kgo.TransactionalID("prague-test"))
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	txn := func(commit kgo.TransactionEndTry, recs ...*kgo.Record) {
		t.Helper()
		must(t, prod.BeginTransaction())
		must(t, prod.ProduceSync(ctx, recs...).FirstErr())
		must(t, prod.EndTransaction(ctx, commit))
	}
	txn(kgo.TryCommit, &kgo.Record{Key: fixture(t, "product-full.key"), Value: fixture(t, "product-full.value")})
	txn(kgo.TryAbort, &kgo.Record{Key: fixture(t, "product-sparse.key"), Value: fixture(t, "product-sparse.value")})

	products := prague.New[int64, *shop.Product]("products")
	c := kafka.NewConsumer(kafka.Config{Brokers: brokers, OnError: func(err error) { t.Errorf("consumer: %v", err) }})
	must(t, kafka.Register(c, productsTopic, products))
	go func() { _ = c.Run(ctx) }()

	loadCtx, loadCancel := context.WithTimeout(ctx, 5*time.Second)
	defer loadCancel()
	if err := c.WaitLoaded(loadCtx); err != nil {
		t.Fatalf("load did not reach past the abort marker: %v", err)
	}
	if _, ok := products.Get(42); !ok {
		t.Fatal("the committed record is missing")
	}
	if _, ok := products.Get(7); ok || products.Len() != 1 {
		t.Fatalf("an aborted record reached the cache: len %d", products.Len())
	}
}

// A partition whose records retention or DeleteRecords removed has an end
// offset but nothing left to read: it is loaded from the start.
func TestConsumerLoadsAnEmptiedPartition(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, productsTopic))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	brokers := cluster.ListenAddrs()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	prod, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.DefaultProduceTopic(productsTopic))
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	must(t, prod.ProduceSync(ctx, &kgo.Record{Key: fixture(t, "product-full.key"), Value: fixture(t, "product-full.value")}).FirstErr())
	adm := kadm.NewClient(prod)
	deleted, err := adm.DeleteRecords(ctx, kadm.Offsets{productsTopic: {0: kadm.Offset{At: 1}}})
	must(t, err)
	must(t, deleted.Error())

	c := kafka.NewConsumer(kafka.Config{Brokers: brokers, OnError: func(err error) { t.Errorf("consumer: %v", err) }})
	must(t, kafka.Register(c, productsTopic, prague.New[int64, *shop.Product]("products")))
	go func() { _ = c.Run(ctx) }()

	loadCtx, loadCancel := context.WithTimeout(ctx, 5*time.Second)
	defer loadCancel()
	if err := c.WaitLoaded(loadCtx); err != nil {
		t.Fatalf("an emptied partition never loaded: %v", err)
	}
}
