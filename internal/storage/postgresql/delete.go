package postgresql

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v4"
	"strconv"
	"strings"
)

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
// Delete will run as nested transaction providing asNestedTo option. By default it runs as stand-alone one.
//
// Returns deleted rows and an error.
func (s *Storage) Delete(ctx context.Context, merchantID int64, offerIDs []int64, options ...txOption) (int64, error) {
	// TODO: set "large" definition as external parameter, e.g. field in Storage
	isLarge := len(offerIDs) > 500
	var deleted int64

	txOptions := buildOptions(options...)
	var tx pgx.Tx
	var err error
	if txOptions.runAsChild {
		s.logger.Debug("Running delete as nested transaction")
		tx, err = txOptions.parentTx.Begin(ctx)
	} else {
		s.logger.Debug("Running delete as stand-alone transaction")
		tx, err = s.db.Begin(ctx)
	}

	if err != nil {
		s.logger.Error("Begin delete transaction")
		return 0, err
	}
	// error handling can be omitted for rollback according to docs
	// see https://pkg.go.dev/github.com/jackc/pgx/v4?tab=doc#hdr-Transactions or any source comment on Rollback
	// TODO: define timeout for transaction rollback
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
			s.logger.Error("Performing 'values based' delete")
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
			s.logger.Error("Create temporary table")
			return 0, err
		}

		s.logger.Debug("Performing bulk insert on temporary table")

		bulkData := &bulkOfferIDs{
			rows: offerIDs,
			idx:  -1,
		}
		_, err = tx.CopyFrom(ctx, pgx.Identifier{"offer_ids_temporary"}, []string{"offer_id"}, bulkData)
		if err != nil {
			s.logger.Error("Bulk insert")
			return 0, err
		}

		s.logger.Debug("Performing delete using temporary table")

		sql = `DELETE FROM products
                USING offer_ids_temporary
                WHERE merchant_id = $1
                  AND products.offer_id = offer_ids_temporary.offer_id`

		tag, err := tx.Exec(ctx, sql, merchantID)
		if err != nil {
			s.logger.Error("Delete using temporary table")
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

	if txOptions.runAsChild {
		s.logger.Debug("Committing nested delete transaction")
	} else {
		s.logger.Debug("Committing stand-alone delete transaction")
	}

	err = tx.Commit(ctx)
	if err != nil {
		s.logger.Error("Commit nested delete transaction")
		return 0, err
	}

	return deleted, nil
}
