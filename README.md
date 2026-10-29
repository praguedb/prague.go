# prague.go

A read-only Go consumer for [Prague](https://github.com/nanov/prague.net) caches. It loads the
compacted Kafka topics a Prague (.NET) producer writes into typed in-memory caches, keeps them
live, and lets you query them through secondary indexes. It has no producer.

Contracts are not hand-written. The same `.prague` schema that praguec compiles to C# for the
producer compiles to Go for the consumer, the way `protoc-gen-go` emits structs that import a small
runtime:

```
shop.prague ──praguec──▶ C# types  ──Prague.Kafka (CacheSerde)──▶ topic
            └─praguec --lang go──▶ Go types ◀──prague.go/kafka──────┘
```

## Generate the contracts

```bash
# from a praguec checkout
go run ./cmd/praguec generate --lang go --go-package shop -o <dir> shop.prague
```

Each table gets a struct, a `DecodeMsgpack` that reads the MessagePack array by index (tag − 1)
and skips reserved or unknown slots, and `CacheKey()`. A composite key becomes a comparable
`<Table>Key` struct, which is also what the Kafka message key decodes into. Views key by their
source table's key type. `decimal(N)` is `wire.Decimal` (shopspring/decimal underneath), `uuid` is
`wire.UUID`, and an optional member of a struct used as a key is `wire.Opt[T]`, so the key stays
comparable. A `union X of Kind` is a struct with `Kind`, one field per arm and `Raw`, decoded from
`[kind, payload]`; a kind the schema has no arm for keeps its payload in `Raw`.

## Use

```go
products := prague.New[int64, *shop.Product]("products")
byStatus := prague.AddMany(products, func(p *shop.Product) shop.Status { return p.Status })
released := prague.AddRange(products, func(p *shop.Product) int64 { return p.Released })

offers := prague.New[shop.OfferKey, *shop.Offer]("offers")
offersByProduct := prague.AddMany(offers, func(o *shop.Offer) int64 { return o.ProductId })

c := kafka.NewConsumer(kafka.Config{Brokers: []string{"kafka:9092"}, Vars: map[string]string{"brandId": "7"}})
kafka.Register(c, "Cache.[v:brandId].Product", products,
	kafka.WithHeaderFilter[int64, *shop.Product](kafka.HeaderEquals("x-region", "eu", "us")),
	kafka.WithValueFilter(func(p *shop.Product) bool { return p.Published }, true),
	kafka.WithAfterHandler(func(u prague.UpdateType, id int64, cur, old *shop.Product) { /* live only */ }))
kafka.Register(c, "Cache.[v:brandId].Offer", offers)

go c.Run(ctx)
c.WaitLoaded(ctx)

p, ok := products.Get(42)

page := products.Query().
	With(byStatus.Eq(shop.StatusActive), released.Gte(from).Lt(to)). // lanes AND; prague.Or unions
	Where(func(p *shop.Product) bool { return p.Stock != nil }).    // residual filter
	Sort(func(a, b *shop.Product) int { return cmp.Compare(b.Released, a.Released) }).
	Page(0, 20).
	Execute() // page.Items, page.TotalCount

withOffers := prague.JoinMany(page.Items, offersByProduct, func(p *shop.Product) int64 { return p.Id }, false)
```

Indexes: `AddUnique`, `AddMany`, `AddRange`, plus `…Opt` variants whose extractor returns
`(value, ok)` for nullable fields. Indexes can be added before or after data arrives. A query
drives from the lane with the fewest candidates and checks the rest against each value. If any lane
is empty, the query returns before fetching anything.

## Semantics, as in Prague (.NET)

- Message key = MessagePack of the cache key, value = MessagePack of the item, empty value =
  tombstone. Topics are read from the start with no consumer group and no committed offsets, so a
  restart is a full reload.
- A re-published, byte-identical value is `Same`. It is detected from a fingerprint of the value
  bytes, before decoding, and is neither stored nor reported.
- Header filters run before the key is decoded, and all of them must pass:
  `HeaderEquals` (any of several values), `HeaderNotEquals`, `HeaderExists` and
  `HeaderPredicate[T]` (with `passOnNull` for a MessagePack nil). A filter checks every occurrence
  of its header and passes when the header is absent, except `HeaderExists`. A rejected header
  also drops a tombstone. A tombstone missing a required header still applies.
- Members annotated `[header]`, `[header("name")]` or `[from_timestamp]` in the schema are written
  from the message after value filters run. The generated type's `Enrich` reads them the way
  Prague's `HeadersSerDe` writes them: raw little-endian or MessagePack integers, UTF-8 strings,
  MessagePack for everything else. The headers and timestamp a type reads are part of its `Same`
  fingerprint, so a changed header is an `Update`.
- After-handlers fire only once the topic is loaded. A topic counts as loaded when every partition
  has been read to the end offset seen at startup, so multi-partition topics load correctly.
- Topic templates resolve `[v:name]` from `Config.Vars` and `[e:NAME]` from the environment. A
  missing topic makes `Run` fail rather than wait.

Differences: one writer and many readers behind a `sync.RWMutex`, so a query sees a consistent
snapshot. That is stronger than Prague's lock-free reads and slower on hot read paths. Values are
shared pointers and must not be mutated. There are no prepared or frozen queries or global
last-update index yet. `Same` compares bytes, not Prague's structural equality, so a
re-encoding that changes bytes without changing data counts as `Update`.

## Proof of wire compatibility

Two schemas, deliberately out of step. `schemas/shop.prague` is the reader: Go types in
`internal/shop`. `schemas/shop-writer.prague` is a producer both ahead of it (Product tags 16–17, a
third member on `Price` and `Dimensions`) and behind it (data in tags 4–6, which the reader has
reserved). The fixtures in `testdata/interop` were written by Prague's `CacheSerde<T>` from the
writer's C# types.

- `go test ./...`: generated Go decodes those bytes. That covers nil presence, decimals, the
  composite key, a view read from table bytes, and the writer's extra and reserved slots skipped at
  every level. The consumer is tested against an in-memory cluster (`kfake`).

`schemas/tenant.prague` is a larger schema in the shape a multi-tenant platform has: eight caches
(operators, brands, sites, platform settings, settings, currencies, exchange rates, rate
providers). It includes tagless `[IgnoreMember][DataCacheHeader]` members, struct keys with an
optional `uuid`, and the hand-formatted `SettingValue` pseudo-union, whose GUID arm is `bytes`
because its formatter writes 16 raw bytes.

- `go test ./internal/tenant`: the Go types decode `testdata/tenant`, including every `SettingKind`
  and an unknown kind kept as raw bytes.

```bash
go test -race ./...
```
