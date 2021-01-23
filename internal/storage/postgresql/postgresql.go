package postgresql

import (
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/log/zapadapter"
	"github.com/jackc/pgx/v4/pgxpool"
	"go.uber.org/zap"
)

type Storage struct {
	logger *zap.Logger
	db     *pgxpool.Pool
}

func NewStorage(ctx context.Context, logger *zap.Logger) (*Storage, error) {
	if logger == nil {
		return nil, errors.New("no logger provided")
	}

	config, _ := pgxpool.ParseConfig("")

	config.ConnConfig.Logger = zapadapter.NewLogger(logger)

	pool, err := pgxpool.ConnectConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("cannot connect using config %+v: %w", config, err)
	}

	return &Storage{
		logger: logger,
		db:     pool,
	}, nil
}

// Upsert performs three-step transaction:
// 1. creates temporary table
// 2. feels it via bulk insert with incoming data that is not marked for deletion
// 3. insert rows from temporary table into "products"
// if provided ctx is not canceled or timed out transaction will be committed.
// Returns added and updated rows count and error
func (s *Storage) Upsert(ctx context.Context, products []product) (int, int, error) {
	bulkData := bulk{
		rows: products,
		idx:  0,
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return 0, 0, err
	}
	// error handling can be omitted for rollback according to docs
	// see https://pkg.go.dev/github.com/jackc/pgx/v4?tab=doc#hdr-Transactions or any source comment on Rollback
	defer tx.Rollback(context.Background())

	s.logger.Debug("Creating temporary table")

	sql := `CREATE TEMPORARY TABLE products_temporary
             (LIKE products
         INCLUDING CONSTRAINTS
         INCLUDING INDEXES)
                ON COMMIT DELETE ROWS`

	_, err = tx.Exec(ctx, sql)
	if err != nil {
		s.logger.Error("Create temporary table", zap.Error(err))
		return 0, 0, err
	}

	s.logger.Debug("Performing bulk insert on temporary table")

	columnNames := []string{"merchant_id", "offer_id", "name", "price", "quantity"}
	_, err = tx.CopyFrom(ctx, pgx.Identifier{"products_temporary"}, columnNames, &bulkData)
	if err != nil {
		s.logger.Error("Bulk insert", zap.Error(err))
		return 0, 0, err
	}

	s.logger.Debug("Performing insert from temporary to products")
	var inserted, updated int
	sql = `WITH t AS
        (INSERT INTO products
         SELECT * FROM products_temporary
             ON CONFLICT (merchant_id, offer_id) DO UPDATE
			SET name = excluded.name
                price = excluded.price
                quantity = excluded.quantity
      RETURNING xmax)
         SELECT SUM(CASE WHEN xmax = 0 THEN 1 ELSE 0 END) AS inserted
                SUM(CASE WHEN xmax::text::int > 0 THEN 1 ELSE 0 END) AS updated
           FROM t`

	err = tx.QueryRow(ctx, sql).Scan(&inserted, &updated)
	if err != nil {
		s.logger.Error("Insert from temporary to products")
		return 0, 0, err
	}

	ctxErr := ctx.Err()
	if ctxErr != nil {
		switch {
		case errors.Is(ctxErr, context.DeadlineExceeded):
			s.logger.Info("Task deadline exceeded")
			return 0, 0, ctxErr

		case errors.Is(ctxErr, context.Canceled):
			s.logger.Info("Task is canceled")
			return 0, 0, ctxErr
		}
	}

	err = tx.Commit(ctx)
	if err != nil {
		s.logger.Error("Commit transaction", zap.Error(err))
		return 0, 0, err
	}

	return inserted, updated, nil
}

// Close closes all database connections in pool
func (s *Storage) Close() {
	s.logger.Info("Closing storage connections")
	s.db.Close()
}
