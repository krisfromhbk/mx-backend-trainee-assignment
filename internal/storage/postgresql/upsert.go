package postgresql

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v4"
)

// Upsert performs three-step transaction:
// 1. creates temporary table
// 2. fills it via bulkProducts insert with incoming data
// 3. insert rows from temporary table into "products"
// if provided ctx is not canceled or timed out transaction will be committed.
//
// Upsert will run as nested transaction providing asNestedTo option. By default, it runs as stand-alone one.
//
// Returns added and updated rows count and error
func (s *Storage) Upsert(ctx context.Context, products []Product, options ...txOption) (int64, int64, error) {
	bulkData := bulkProducts{
		rows: products,
		idx:  -1,
		err:  nil,
	}

	txOptions := buildOptions(options...)
	var tx pgx.Tx
	var err error
	if txOptions.runAsChild {
		s.logger.Debug("running upsert as nested transaction")
		tx, err = txOptions.parentTx.Begin(ctx)
	} else {
		s.logger.Debug("running upsert as stand-alone transaction")
		tx, err = s.db.Begin(ctx)
	}

	if err != nil {
		s.logger.Error("failed to begin upsert transaction")
		return 0, 0, err
	}
	// error handling can be omitted for rollback according to docs
	// see https://pkg.go.dev/github.com/jackc/pgx/v4?tab=doc#hdr-Transactions or any source comment on Rollback
	// TODO: define timeout for transaction rollback
	defer tx.Rollback(context.Background())

	s.logger.Debug("creating temporary table")

	sql := `CREATE TEMPORARY TABLE products_temporary
             (LIKE products
         INCLUDING CONSTRAINTS
         INCLUDING INDEXES)
                ON COMMIT DROP`

	_, err = tx.Exec(ctx, sql)
	if err != nil {
		s.logger.Error("failed to create temporary table")
		return 0, 0, err
	}

	s.logger.Debug("performing bulk products insert on temporary table")

	columnNames := []string{"merchant_id", "offer_id", "name", "price", "quantity"}
	_, err = tx.CopyFrom(ctx, pgx.Identifier{"products_temporary"}, columnNames, &bulkData)
	if err != nil {
		s.logger.Error("failed to perform bulk insert")
		return 0, 0, err
	}

	s.logger.Debug("performing insert from temporary table to products")
	var inserted, updated int64
	sql = `WITH xmax_values AS
                    (INSERT INTO products
                     SELECT * FROM products_temporary
                         ON CONFLICT (merchant_id, offer_id) DO UPDATE
			            SET name = excluded.name,
                            price = excluded.price,
                            quantity = excluded.quantity
                      WHERE products.name <> excluded.name
                         OR products.price <> excluded.price
                         OR products.quantity <> excluded.quantity
                  RETURNING xmax),
                 temp_stats AS
                    (SELECT SUM(CASE WHEN xmax = 0 THEN 1 ELSE 0 END) AS inserted,
                            SUM(CASE WHEN xmax::text::int > 0 THEN 1 ELSE 0 END) AS updated
                       FROM xmax_values)
                     SELECT COALESCE(inserted, 0) AS inserted,
		                    COALESCE(updated, 0) AS updated
		               FROM temp_stats`

	err = tx.QueryRow(ctx, sql).Scan(&inserted, &updated)
	if err != nil {
		s.logger.Error("failed to insert from temporary table to products")
		return 0, 0, err
	}

	ctxErr := ctx.Err()
	if ctxErr != nil {
		switch {
		case errors.Is(ctxErr, context.DeadlineExceeded):
			s.logger.Info("task deadline is exceeded")
			return 0, 0, ctxErr

		case errors.Is(ctxErr, context.Canceled):
			s.logger.Info("task is canceled")
			return 0, 0, ctxErr
		}
	}

	if txOptions.runAsChild {
		s.logger.Debug("committing nested upsert transaction")
	} else {
		s.logger.Debug("committing stand-alone upsert transaction")
	}

	err = tx.Commit(ctx)
	if err != nil {
		s.logger.Error("failed to commit nested upsert transaction")
		return 0, 0, err
	}

	return inserted, updated, nil
}
