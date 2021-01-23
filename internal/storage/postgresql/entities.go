package postgresql

type product struct {
	merchantID int64
	offerID    int64
	name       string
	price      float64
	quantity   int64
}

func (p product) interfaceSlice() []interface{} {
	return []interface{}{
		p.merchantID,
		p.offerID,
		p.name,
		p.price,
		p.quantity,
	}
}
