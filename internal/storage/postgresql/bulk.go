package postgresql

type bulk struct {
	rows []product
	idx  int
}

func (b *bulk) Next() bool {
	b.idx++
	return b.idx < len(b.rows)
}

func (b *bulk) Values() ([]interface{}, error) {
	return b.rows[b.idx].interfaceSlice(), nil
}

func (b *bulk) Err() error {
	return nil
}
