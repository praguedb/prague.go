// Package wire holds the MessagePack types praguec's Go backend emits that
// have no direct Go equivalent. Generated code imports it; application code
// rarely needs to.
package wire

import (
	"fmt"

	"github.com/shopspring/decimal"
	"github.com/vmihailenco/msgpack/v5"
	"github.com/vmihailenco/msgpack/v5/msgpcode"
)

// Decimal is a protop `decimal(N)`. MessagePack-CSharp writes System.Decimal
// as its invariant-culture string ("12.3400"), so that is the form read;
// numeric forms are accepted too, for producers with a numeric formatter.
type Decimal struct {
	decimal.Decimal
}

// DecodeMsgpack implements msgpack.CustomDecoder.
func (x *Decimal) DecodeMsgpack(d *msgpack.Decoder) error {
	c, err := d.PeekCode()
	if err != nil {
		return err
	}
	switch {
	case msgpcode.IsString(c) || msgpcode.IsBin(c):
		s, err := d.DecodeString()
		if err != nil {
			return err
		}
		v, err := decimal.NewFromString(s)
		if err != nil {
			return fmt.Errorf("wire: decimal %q: %w", s, err)
		}
		x.Decimal = v
	case c == msgpcode.Float || c == msgpcode.Double:
		f, err := d.DecodeFloat64()
		if err != nil {
			return err
		}
		x.Decimal = decimal.NewFromFloat(f)
	default:
		i, err := d.DecodeInt64()
		if err != nil {
			return err
		}
		x.Decimal = decimal.NewFromInt(i)
	}
	return nil
}
