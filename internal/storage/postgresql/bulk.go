package postgresql

type bulkProducts struct {
	rows []Product
	idx  int
	err  error
}

func (b *bulkProducts) Next() bool {
	b.idx++
	return b.idx < len(b.rows)
}

func (b *bulkProducts) Values() ([]interface{}, error) {
	data, err := b.rows[b.idx].interfaceSlice()
	b.err = err
	return data, err
}

func (b *bulkProducts) Err() error {
	return b.err
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
