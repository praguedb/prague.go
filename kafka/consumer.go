// Package kafka feeds prague caches from compacted topics written by Prague
// (.NET) producers.
//
// The wire contract is Prague's: the message key is the MessagePack encoding
// of the cache key, the value is the MessagePack encoding of the item, and an
// empty value is a tombstone. Every topic is read from the beginning, with no
// consumer group and no committed offsets, so every process holds the whole
// topic and a restart is a full reload.
//
// A topic is loaded once every partition has been read up to the end offset
// observed at startup. After-handlers fire only once a topic is loaded, so
// they see live changes and not the replay.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"hash/maphash"
	"log/slog"
	"sync"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/praguedb/prague.go"
	"github.com/praguedb/prague.go/wire"
)

// Config configures a Consumer.
type Config struct {
	Brokers []string
	// Vars resolve `[v:name]` placeholders in topic templates.
	Vars map[string]string
	// ClientOptions are passed to franz-go after Prague's own (TLS, SASL, …).
	ClientOptions []kgo.Opt
	// OnError receives undecodable records and fetch errors. Default: slog.
	OnError func(error)
}

// Consumer owns one Kafka client and feeds every registered cache.
type Consumer struct {
	cfg      Config
	mu       sync.Mutex
	started  bool
	bindings map[string]binding
	loaded   chan struct{}
}

type binding interface {
	apply(r *kgo.Record, live bool)
}

func NewConsumer(cfg Config) *Consumer {
	if cfg.OnError == nil {
		cfg.OnError = func(err error) { slog.Error("prague/kafka", "err", err) }
	}
	return &Consumer{cfg: cfg, bindings: map[string]binding{}, loaded: make(chan struct{})}
}

// Option configures one cache's binding.
type Option[K comparable, V any] func(*cacheBinding[K, V])

// WithKeyFilter drops records whose key fails keep. Tombstones are not
// filtered: a delete always applies.
func WithKeyFilter[K comparable, V any](keep func(K) bool) Option[K, V] {
	return func(b *cacheBinding[K, V]) { b.keyFilters = append(b.keyFilters, keep) }
}

// WithValueFilter drops records whose decoded value fails keep. With
// deleteOnReject a rejected value also removes what the cache holds under its
// key, so an item that stops matching leaves the cache.
func WithValueFilter[K comparable, V any](keep func(V) bool, deleteOnReject bool) Option[K, V] {
	return func(b *cacheBinding[K, V]) {
		b.valueFilters = append(b.valueFilters, valueFilter[V]{keep, deleteOnReject})
	}
}

// AfterHandler observes a live change. old is the zero value on Add, cur on
// Delete. It runs on the consume goroutine and must not block.
type AfterHandler[K comparable, V any] func(u prague.UpdateType, key K, cur, old V)

// WithAfterHandler registers h for changes once the topic is loaded. Same and
// Filtered are not reported.
func WithAfterHandler[K comparable, V any](h AfterHandler[K, V]) Option[K, V] {
	return func(b *cacheBinding[K, V]) { b.after = append(b.after, h) }
}

// Register binds cache to the topic named by template. It must be called
// before Run.
func Register[K comparable, V any](c *Consumer, template string, cache *prague.Cache[K, V], opts ...Option[K, V]) error {
	topic, err := prague.ResolveTopic(template, c.cfg.Vars)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started {
		return errors.New("prague/kafka: Register after Run")
	}
	if _, dup := c.bindings[topic]; dup {
		return fmt.Errorf("prague/kafka: topic %q registered twice", topic)
	}
	b := &cacheBinding[K, V]{topic: topic, cache: cache, seed: maphash.MakeSeed(), onError: c.cfg.OnError}
	var zero V
	if e, ok := any(zero).(wire.Enricher); ok {
		en := e.Enrichment()
		b.enrichment = &en
	}
	for _, o := range opts {
		o(b)
	}
	c.bindings[topic] = b
	return nil
}

// Loaded is closed once every registered topic is loaded.
func (c *Consumer) Loaded() <-chan struct{} { return c.loaded }

// WaitLoaded blocks until every topic is loaded or ctx ends.
func (c *Consumer) WaitLoaded(ctx context.Context) error {
	select {
	case <-c.loaded:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Run consumes until ctx ends. It fails fast if a topic does not exist:
// Prague topics are provisioned, never auto-created.
func (c *Consumer) Run(ctx context.Context) error {
	c.mu.Lock()
	if c.started {
		c.mu.Unlock()
		return errors.New("prague/kafka: Run called twice")
	}
	c.started = true
	topics := make([]string, 0, len(c.bindings))
	for t := range c.bindings {
		topics = append(topics, t)
	}
	c.mu.Unlock()
	if len(topics) == 0 {
		return errors.New("prague/kafka: no caches registered")
	}

	// No ConsumerGroup: franz-go then consumes every partition directly and
	// commits nothing, and does not auto-create topics.
	opts := append([]kgo.Opt{
		kgo.SeedBrokers(c.cfg.Brokers...),
		kgo.ConsumeTopics(topics...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	}, c.cfg.ClientOptions...)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return err
	}
	defer cl.Close()

	pending, err := endOffsets(ctx, kadm.NewClient(cl), topics)
	if err != nil {
		return err
	}
	live := map[string]bool{}
	markLoaded := func(t string) {
		live[t] = true
		if len(live) == len(topics) {
			close(c.loaded)
		}
	}
	for _, t := range topics {
		if len(pending[t]) == 0 {
			markLoaded(t)
		}
	}

	for {
		fs := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if fs.IsClientClosed() {
			return errors.New("prague/kafka: client closed")
		}
		for _, fe := range fs.Errors() {
			c.cfg.OnError(fmt.Errorf("fetch %s[%d]: %w", fe.Topic, fe.Partition, fe.Err))
		}
		fs.EachPartition(func(p kgo.FetchTopicPartition) {
			b := c.bindings[p.Topic]
			if b == nil {
				return
			}
			for _, r := range p.Records {
				b.apply(r, live[p.Topic])
				if parts, ok := pending[p.Topic]; ok && !live[p.Topic] {
					if end, ok := parts[r.Partition]; ok && r.Offset+1 >= end {
						delete(parts, r.Partition)
						if len(parts) == 0 {
							markLoaded(p.Topic)
						}
					}
				}
			}
		})
	}
}

// endOffsets is, per topic, the end offset of every non-empty partition: what
// a topic must be read up to before it counts as loaded.
func endOffsets(ctx context.Context, adm *kadm.Client, topics []string) (map[string]map[int32]int64, error) {
	listed, err := adm.ListEndOffsets(ctx, topics...)
	if err != nil {
		return nil, fmt.Errorf("prague/kafka: list end offsets: %w", err)
	}
	out := map[string]map[int32]int64{}
	for _, t := range topics {
		parts, ok := listed[t]
		if !ok || len(parts) == 0 {
			return nil, fmt.Errorf("prague/kafka: topic %q does not exist", t)
		}
		out[t] = map[int32]int64{}
		for p, o := range parts {
			if o.Err != nil {
				return nil, fmt.Errorf("prague/kafka: topic %q partition %d: %w", t, p, o.Err)
			}
			if o.Offset > 0 {
				out[t][p] = o.Offset
			}
		}
	}
	return out, nil
}

type valueFilter[V any] struct {
	keep           func(V) bool
	deleteOnReject bool
}

type cacheBinding[K comparable, V any] struct {
	topic         string
	cache         *prague.Cache[K, V]
	seed          maphash.Seed
	keyFilters    []func(K) bool
	valueFilters  []valueFilter[V]
	headerFilters []HeaderFilter
	after         []AfterHandler[K, V]
	onError       func(error)
	// enrichment is what V's Enrich reads, nil when V has none.
	enrichment *wire.Enrichment
}

func (b *cacheBinding[K, V]) apply(r *kgo.Record, live bool) {
	if !headersPass(b.headerFilters, r) {
		return
	}
	var k K
	if err := msgpack.Unmarshal(r.Key, &k); err != nil {
		b.onError(fmt.Errorf("%s[%d]@%d: key: %w", r.Topic, r.Partition, r.Offset, err))
		return
	}
	if len(r.Value) == 0 {
		if old, ok := b.cache.Remove(k); ok {
			b.notify(live, prague.Delete, k, old, old)
		}
		return
	}
	for _, keep := range b.keyFilters {
		if !keep(k) {
			return
		}
	}
	// Prague producers re-publish unchanged state; skip the decode outright.
	fp := b.fingerprint(r)
	if have, ok := b.cache.Fingerprint(k); ok && have == fp {
		return
	}
	var v V
	if err := msgpack.Unmarshal(r.Value, &v); err != nil {
		b.onError(fmt.Errorf("%s[%d]@%d: value: %w", r.Topic, r.Partition, r.Offset, err))
		return
	}
	for _, f := range b.valueFilters {
		if !f.keep(v) {
			if f.deleteOnReject {
				if old, ok := b.cache.Remove(k); ok {
					b.notify(live, prague.Delete, k, old, old)
				}
			}
			return
		}
	}
	// After the value filters, which is Prague's order: a value filter sees
	// what the value carried, never what the message stamped over it.
	if e, ok := any(v).(wire.Enricher); ok {
		e.Enrich(recordMeta{r})
	}
	u, old := b.cache.Apply(k, v, fp, r.Timestamp.UnixMilli())
	b.notify(live, u, k, v, old)
}

// fingerprint identifies what a record writes into the cache: its value, and
// for an enriched type the headers and timestamp Enrich reads. A re-publish
// that changes only a stamped header is then a change, not Same.
func (b *cacheBinding[K, V]) fingerprint(r *kgo.Record) uint64 {
	if b.enrichment == nil {
		return maphash.Bytes(b.seed, r.Value) | 1
	}
	var h maphash.Hash
	h.SetSeed(b.seed)
	h.Write(r.Value)
	for _, key := range b.enrichment.Headers {
		for _, rh := range r.Headers {
			if rh.Key == key {
				// Length-prefixed, so no two header sets hash the same bytes.
				maphash.WriteComparable(&h, len(key))
				h.WriteString(key)
				maphash.WriteComparable(&h, len(rh.Value))
				h.Write(rh.Value)
			}
		}
	}
	if b.enrichment.Timestamp {
		maphash.WriteComparable(&h, r.Timestamp.UnixMilli())
	}
	return h.Sum64() | 1
}

func (b *cacheBinding[K, V]) notify(live bool, u prague.UpdateType, k K, cur, old V) {
	if !live || u == prague.Same {
		return
	}
	for _, h := range b.after {
		h(u, k, cur, old)
	}
}
