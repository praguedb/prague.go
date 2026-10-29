package tenant_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/praguedb/prague.go/internal/tenant"
	"github.com/praguedb/prague.go/wire"
)

// The fixtures were written by C# types through Prague's CacheSerde; this
// package is praguec's Go emission of schemas/tenant.prague, the same types.
func decode[T any](t *testing.T, name string) T {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "testdata", "tenant", name))
	if err != nil {
		t.Fatal(err)
	}
	var v T
	if err := msgpack.Unmarshal(b, &v); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return v
}

var (
	acme    = wire.MustUUID("0b6f2a1e-5c3d-4e7f-8a9b-0c1d2e3f4a5b")
	zeta    = wire.MustUUID("7f8e9d0c-1b2a-4c3d-9e8f-a0b1c2d3e4f5")
	alpha   = wire.MustUUID("a1b2c3d4-e5f6-4789-abcd-ef0123456789")
	site    = wire.MustUUID("c3d4e5f6-a7b8-4901-8def-012345678901")
	license = wire.MustUUID("d4e5f6a7-b8c9-4012-9ef0-123456789012")
	// SettingOwner.Platform.
	platform = wire.MustUUID("00000000-0000-0000-706c-6174666f726d")
)

const created, updated = 1_767_225_600_000, 1_767_312_000_000

func TestTenancy(t *testing.T) {
	if k := decode[wire.UUID](t, "operator-acme.key"); k != acme {
		t.Fatalf("operator key %s", k)
	}
	o := decode[*tenant.OperatorInfo](t, "operator-acme.value")
	if o.Id != acme || o.Slug != "acme" || o.LegalEntity == nil || *o.LegalEntity != "Acme Europe AG" ||
		o.Status != tenant.OperatorStatusActive || o.BaseCurrency != tenant.CurrencyEur || o.BaseCurrencyCode != "EUR" ||
		o.CreatedAtUnixMs != created || o.UpdatedAtUnixMs != updated || o.CacheKey() != acme {
		t.Fatalf("operator: %+v", o)
	}
	if z := decode[*tenant.OperatorInfo](t, "operator-zeta.value"); z.Id != zeta || z.LegalEntity != nil || z.BaseCurrency != tenant.CurrencyUsd || z.Status != tenant.OperatorStatusPending {
		t.Fatalf("operator zeta: %+v", z)
	}
	b := decode[*tenant.BrandInfo](t, "brand-alpha.value")
	if b.Id != alpha || b.OperatorId != acme || b.Name != "Alpha Stores" || b.Status != tenant.BrandStatusActive {
		t.Fatalf("brand: %+v", b)
	}
	s := decode[*tenant.SiteInfo](t, "site-alpha-com.value")
	if s.Id != site || s.BrandId != alpha || s.OperatorId != acme || s.Domain != "alpha.example" || s.LicenseId != license {
		t.Fatalf("site: %+v", s)
	}
	// Header-only members have no slot: the value leaves them zero.
	if o.SchemaVersion != 0 || b.SchemaVersion != 0 {
		t.Fatal("a header-only member decoded from the value")
	}
}

func TestPlatformSettingKeysByAComparableStruct(t *testing.T) {
	k := decode[tenant.PlatformSettingKey](t, "platform-setting-platform.key")
	if k != (tenant.PlatformSettingKey{Type: tenant.PlatformSettingTypePasswordPolicy}) {
		t.Fatalf("platform key %+v", k)
	}
	p := decode[*tenant.PlatformSetting](t, "platform-setting-platform.value")
	if p.CacheKey() != k || p.Version != 3 || p.PasswordPolicy == nil {
		t.Fatalf("platform setting: %+v", p)
	}
	pp := p.PasswordPolicy
	if pp.MinLength != 10 || !pp.RequireUppercase || pp.RequireLowercase || !pp.RequireNumber || !pp.PasskeysAllowedForEveryone ||
		pp.Expiry.Mode != tenant.PasswordRuleModeRequiredForEveryone || pp.Expiry.Value == nil || *pp.Expiry.Value != 90 || pp.Expiry.Locked == nil || !*pp.Expiry.Locked ||
		pp.Lockout.Mode != tenant.PasswordRuleModeOperatorDecides || pp.Lockout.Value != nil || pp.Lockout.Locked != nil {
		t.Fatalf("password policy: %+v / %+v / %+v", pp, pp.Expiry, pp.Lockout)
	}
	tk := decode[tenant.PlatformSettingKey](t, "platform-setting-acme.key")
	if tk != (tenant.PlatformSettingKey{Type: tenant.PlatformSettingTypePasswordPolicy, TenantId: wire.Some(acme)}) {
		t.Fatalf("tenant key %+v", tk)
	}
	if a := decode[*tenant.PlatformSetting](t, "platform-setting-acme.value"); a.CacheKey() != tk || a.PasswordPolicy != nil {
		t.Fatalf("tenant setting: %+v", a)
	}
	// Equal keys from different decodes are one map key.
	m := map[tenant.PlatformSettingKey]int{k: 1, tk: 2}
	if m[decode[tenant.PlatformSettingKey](t, "platform-setting-acme.key")] != 2 {
		t.Fatal("a decoded key is not the same map key")
	}
}

// SettingValue is the pseudo-union: [kind, payload], the payload's type chosen
// by the kind. One fixture per SettingKind.
func TestSettingValueEveryKind(t *testing.T) {
	get := func(name string) *tenant.Setting {
		t.Helper()
		s := decode[*tenant.Setting](t, "setting-"+name+".value")
		if k := decode[tenant.SettingKey](t, "setting-"+name+".key"); k != s.CacheKey() {
			t.Fatalf("%s: message key %+v, value key %+v", name, k, s.CacheKey())
		}
		if s.LockState != tenant.SettingLockBounded || s.Version != 2 || s.CreatedAtUnixMs != created {
			t.Fatalf("%s: %+v", name, s)
		}
		return s
	}
	if s := get("nil"); s.Value.Kind != tenant.SettingKindNil || s.Key.Owner != platform || s.OwnerType != tenant.SettingOwnerTypePlatform {
		t.Fatalf("nil: %+v", s)
	}
	if s := get("bool"); s.Value.Kind != tenant.SettingKindBool || !s.Value.Bool || s.Key.Code != tenant.SettingCodePassword {
		t.Fatalf("bool: %+v", s.Value)
	}
	if s := get("int"); s.Value.Kind != tenant.SettingKindInt || s.Value.Int != 12 || s.Key != (tenant.SettingKey{Owner: acme, Group: tenant.SettingGroupPasswordPolicy, Code: tenant.SettingCodePasswordMinLength}) {
		t.Fatalf("int: %+v %+v", s.Value, s.Key)
	}
	if s := get("real"); s.Value.Kind != tenant.SettingKindReal || s.Value.Real != 2.5 || s.OwnerType != tenant.SettingOwnerTypeBrand {
		t.Fatalf("real: %+v", s.Value)
	}
	s := get("guid")
	if u, err := wire.UUIDFromDotNet(s.Value.Guid); s.Value.Kind != tenant.SettingKindGuid || err != nil || u != license ||
		s.Key.SubjectId != wire.Some(site) || s.EntityType != tenant.SettingEntityTypeSite {
		t.Fatalf("guid: %+v %v", s.Value, err)
	}
	if s := get("string"); s.Value.Kind != tenant.SettingKindString || s.Value.String != "days" {
		t.Fatalf("string: %+v", s.Value)
	}
	if s := get("bytes"); s.Value.Kind != tenant.SettingKindBytes || string(s.Value.Bytes) != "\x01\x02\x03" {
		t.Fatalf("bytes: %+v", s.Value)
	}
	if s := get("json"); s.Value.Kind != tenant.SettingKindJson || s.Value.Json != `{"a":1}` {
		t.Fatalf("json: %+v", s.Value)
	}
	s = get("object")
	var obj map[string]any
	if err := msgpack.Unmarshal(s.Value.Object, &obj); s.Value.Kind != tenant.SettingKindObject || err != nil || obj["kind"] != "temporary" {
		t.Fatalf("object: %+v %v %v", s.Value, obj, err)
	}
}

// A kind this schema does not know keeps its payload as written, and an
// element after the payload is skipped, both as the C# formatter does.
func TestSettingValueKeepsAnUnknownKind(t *testing.T) {
	b, _ := msgpack.Marshal([]any{99, map[string]any{"x": 1}, "later"})
	var v tenant.SettingValue
	if err := msgpack.Unmarshal(b, &v); err != nil || v.Kind != 99 {
		t.Fatalf("%+v %v", v, err)
	}
	var back map[string]any
	if err := msgpack.Unmarshal(v.Raw, &back); err != nil || back["x"] != int8(1) {
		t.Fatalf("raw %x: %v %v", []byte(v.Raw), back, err)
	}
}

func TestFinance(t *testing.T) {
	if k := decode[int32](t, "currency-EUR.key"); k != 978 {
		t.Fatalf("currency key %d", k)
	}
	c := decode[*tenant.CurrencyInfo](t, "currency-EUR.value")
	if c.Number != 978 || c.Code != "EUR" || c.MinorUnit != 2 || c.StorageExponent != 4 || !c.IsEnabled || !c.IsPivot || c.Provider != "ecb" {
		t.Fatalf("currency: %+v", c)
	}
	r := decode[*tenant.ExchangeRate](t, "exchange-rate.value")
	if r.Pair != 978<<16|840 || r.Base != 978 || r.Quote != 840 || r.RateMantissa != 10_850 || r.RateScale != 4 || r.RateId != 70 {
		t.Fatalf("rate: %+v", r)
	}
	if p := decode[*tenant.RateProviderStatus](t, "rate-provider.value"); p.CacheKey() != "ecb" || p.NextUpdateUnixMs != updated+3_600_000 {
		t.Fatalf("provider: %+v", p)
	}
}
