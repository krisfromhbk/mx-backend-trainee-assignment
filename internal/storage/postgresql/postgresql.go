package postgresql

import (
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/log/zapadapter"
	"github.com/jackc/pgx/v4/pgxpool"
	"go.uber.org/zap"
	"strconv"
	"strings"
)

// Storage defines fields used in db interaction processes
type Storage struct {
	logger *zap.Logger
	db     *pgxpool.Pool
}

// NewStorage constructs Store instance with configured logger
func NewStorage(ctx context.Context, logger *zap.Logger) (*Storage, error) {
	if logger == nil {
		return nil, errors.New("no logger provided")
	}

	config, _ := pgxpool.ParseConfig("")

	config.ConnConfig.Logger = zapadapter.NewLogger(logger)
	config.ConnConfig.LogLevel = pgx.LogLevelError

	pool, err := pgxpool.ConnectConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("cannot connect using config %+v: %w", config, err)
	}

	return &Storage{
		logger: logger,
		db:     pool,
	}, nil
}

// Close closes all database connections in pool
func (s *Storage) Close() {
	s.logger.Info("Closing storage connections")
	s.db.Close()
}

// Upsert performs three-step transaction:
// 1. creates temporary table
// 2. fills it via bulkProducts insert with incoming data
// 3. insert rows from temporary table into "products"
// if provided ctx is not canceled or timed out transaction will be committed.
//
// Returns added and updated rows count and error
func (s *Storage) Upsert(ctx context.Context, products []Product) (int64, int64, error) {
	bulkData := bulkProducts{
		rows: products,
		idx:  -1,
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
                ON COMMIT DROP`

	_, err = tx.Exec(ctx, sql)
	if err != nil {
		s.logger.Error("Create temporary table", zap.Error(err))
		return 0, 0, err
	}

	s.logger.Debug("Performing bulkProducts insert on temporary table")

	columnNames := []string{"merchant_id", "offer_id", "name", "price", "quantity"}
	_, err = tx.CopyFrom(ctx, pgx.Identifier{"products_temporary"}, columnNames, &bulkData)
	if err != nil {
		s.logger.Error("Bulk insert", zap.Error(err))
		return 0, 0, err
	}

	s.logger.Debug("Performing insert from temporary to products")
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

// Delete performs variable-step transaction in order to delete provided products.
// A. Transaction will have one step if Product slice length is relatively small.
// B. Transaction will have three steps if Product slice length is relatively big.
// The actual values of "small" and "big" should be found by tests, but for now let's
// state that less than 500 is small.
//
// Transaction B has following steps:
// 1. create temporary table
// 2. fill it via bulkProducts insert with incoming data
// 3. perform delete using temporary table
//
// Returns deleted rows and an error.
func (s *Storage) Delete(ctx context.Context, merchantID int64, offerIDs []int64) (int64, error) {
	isLarge := len(offerIDs) > 500
	var deleted int64

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return 0, err
	}
	// error handling can be omitted for rollback according to docs
	// see https://pkg.go.dev/github.com/jackc/pgx/v4?tab=doc#hdr-Transactions or any source comment on Rollback
	defer tx.Rollback(context.Background())

	if !isLarge {
		s.logger.Debug("Performing 'values based' delete")

		sql := `DELETE FROM products
                 WHERE merchant_id = $1
                   AND offer_id IN (VALUES `

		builder := new(strings.Builder)
		builder.WriteString(sql)
		var i int
		for ; i < len(offerIDs)-1; i++ {
			builder.WriteString("(")
			builder.WriteString(strconv.FormatInt(offerIDs[i], 10))
			builder.WriteString("), ")
		}
		builder.WriteString("(")
		builder.WriteString(strconv.FormatInt(offerIDs[i], 10))
		builder.WriteString("))")

		tag, err := tx.Exec(ctx, builder.String(), merchantID)
		if err != nil {
			s.logger.Error("Performing 'values based' delete", zap.Error(err))
			return 0, err
		}

		deleted = tag.RowsAffected()
	} else {
		s.logger.Debug("Performing 'temporary table based' delete")

		s.logger.Debug("Creating temporary table")

		sql := `CREATE TEMPORARY TABLE offer_ids_temporary (offer_id offer_id)
                    ON COMMIT DROP`

		_, err = tx.Exec(ctx, sql)
		if err != nil {
			s.logger.Error("Create temporary table", zap.Error(err))
			return 0, err
		}

		s.logger.Debug("Performing bulk insert on temporary table")

		bulkData := &bulkOfferIDs{
			rows: offerIDs,
			idx:  -1,
		}
		_, err = tx.CopyFrom(ctx, pgx.Identifier{"offer_ids_temporary"}, []string{"offer_id"}, bulkData)
		if err != nil {
			s.logger.Error("Bulk insert", zap.Error(err))
			return 0, err
		}

		s.logger.Debug("Performing delete using temporary table")

		sql = `DELETE FROM products
                USING offer_ids_temporary
                WHERE merchant_id = $1
                  AND products.offer_id = offer_ids_temporary.offer_id`

		tag, err := tx.Exec(ctx, sql, merchantID)
		if err != nil {
			s.logger.Error("Delete using temporary table", zap.Error(err))
			return 0, err
		}

		deleted = tag.RowsAffected()
	}

	ctxErr := ctx.Err()
	if ctxErr != nil {
		switch {
		case errors.Is(ctxErr, context.DeadlineExceeded):
			s.logger.Info("Task deadline exceeded")
			return 0, ctxErr

		case errors.Is(ctxErr, context.Canceled):
			s.logger.Info("Task is canceled")
			return 0, ctxErr
		}
	}

	err = tx.Commit(ctx)
	if err != nil {
		s.logger.Error("Commit transaction", zap.Error(err))
		return 0, err
	}

	return deleted, nil
}

type listParameters struct {
	merchantID int64
	offerID    int64
	nameQuery  string
}

const (
	defaultMerchantID = 0
	defaultOfferID    = 0
	defaultNameQuery  = ""
)

func (lp listParameters) isAnyNonDefault() bool {
	return lp.merchantID == defaultMerchantID || lp.offerID == defaultOfferID || lp.nameQuery == defaultNameQuery
}

type ListOption func(parameters *listParameters)

func WithMerchantID(id int64) ListOption {
	return func(p *listParameters) {
		p.merchantID = id
	}
}

func WithOfferID(id int64) ListOption {
	return func(p *listParameters) {
		p.offerID = id
	}
}

func WithNameQuery(q string) ListOption {
	return func(p *listParameters) {
		p.nameQuery = q
	}
}

func (s *Storage) List(ctx context.Context, options ...ListOption) ([]Product, error) {
	parameters := &listParameters{
		merchantID: defaultMerchantID,
		offerID:    defaultOfferID,
		nameQuery:  defaultNameQuery,
	}

	for _, opt := range options {
		opt(parameters)
	}

	var rows pgx.Rows
	var err error

	b := strings.Builder{}
	b.WriteString("SELECT * FROM products")

	if parameters.isAnyNonDefault() {
		b.WriteString(" WHERE 1 = 1")

		if parameters.merchantID != defaultMerchantID {
			b.WriteString(" AND merchant_id = " + strconv.FormatInt(parameters.merchantID, 10))
		}

		if parameters.offerID != defaultOfferID {
			b.WriteString(" AND offer_id = " + strconv.FormatInt(parameters.offerID, 10))
		}

		if parameters.nameQuery != defaultNameQuery {
			b.WriteString(" AND name ^@ $1")
			rows, err = s.db.Query(ctx, b.String(), parameters.nameQuery)
		} else {
			sql := b.String()
			rows, err = s.db.Query(ctx, sql)
		}
	}

	if err != nil {
		s.logger.Error("Selecting rows", zap.Error(err))
		return nil, err
	}

	var products []Product
	for rows.Next() {
		var p Product
		err = rows.Scan(&p.MerchantID, &p.OfferID, &p.Name, &p.Price, &p.Quantity)
		if err != nil {
			s.logger.Error("Scanning row", zap.Error(err))
			return nil, err
		}

		products = append(products, p)
	}

	if rows.Err() != nil {
		return nil, err
	}

	return products, nil
}
