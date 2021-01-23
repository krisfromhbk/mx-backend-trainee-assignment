package postgresql

type bulkProducts struct {
	rows []product
	idx  int
}

func (b *bulkProducts) Next() bool {
	b.idx++
	return b.idx < len(b.rows)
}

func (b *bulkProducts) Values() ([]interface{}, error) {
	return b.rows[b.idx].interfaceSlice(), nil
}

func (b *bulkProducts) Err() error {
	return nil
}

type bulkOfferIDs struct {
	rows []int64
	idx  int
}

func (b *bulkOfferIDs) Next() bool {
	b.idx++
	return b.idx < len(b.rows)
}

func (b *bulkOfferIDs) Values() ([]interface{}, error) {
	return []interface{}{b.rows[b.idx]}, nil
}

func (b *bulkOfferIDs) Err() error {
	return nil
}
