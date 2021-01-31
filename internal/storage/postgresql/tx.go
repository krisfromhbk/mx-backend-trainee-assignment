package postgresql

import "github.com/jackc/pgx/v4"

type txOptions struct {
	runAsChild bool
	parentTx   pgx.Tx
}

func defaultTxOptions() *txOptions {
	return &txOptions{
		runAsChild: false,
		parentTx:   nil,
	}
}

func buildOptions(options ...txOption) *txOptions {
	resultOptions := defaultTxOptions()
	for _, o := range options {
		o.apply(resultOptions)
	}
	return resultOptions
}

type txOption interface {
	apply(options *txOptions)
}

type txOptionFunc func(options *txOptions)

func (f txOptionFunc) apply(opts *txOptions) { f(opts) }

func asNestedTo(parentTx pgx.Tx) txOption {
	return txOptionFunc(func(opts *txOptions) {
		opts.runAsChild = true
		opts.parentTx = parentTx
	})
}
