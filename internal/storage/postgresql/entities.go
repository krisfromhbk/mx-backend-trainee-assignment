package postgresql

import (
	"errors"
	"github.com/shopspring/decimal"
)

var floatErr = errors.New("decimal value can not be presented as float64")

type Product struct {
	MerchantID int64
	OfferID    int64
	Name       string
	Price      decimal.Decimal
	Quantity   int64
}

func (p Product) interfaceSlice() ([]interface{}, error) {
	floatPrice, ok := p.Price.Float64()
	if !ok {
		// the magnitude of underlying value is too big
		// see comments for https://github.com/shopspring/decimal/blob/v1.2.0/decimal.go#L729
		// and https://golang.org/src/math/big/rat.go?s=8033:8080#L279
		return nil, floatErr
	}

	return []interface{}{
		p.MerchantID,
		p.OfferID,
		p.Name,
		floatPrice,
		p.Quantity,
	}, nil
}
