package task

import (
	"context"
	"github.com/shopspring/decimal"
	"github.com/tealeg/xlsx/v3"
	"go.uber.org/zap"
	"mx/internal/storage/postgresql"
)

func processTask(ctx context.Context, logger *zap.Logger, resultChannel chan<- taskResult, abortChannel chan<- struct{}, db *postgresql.Storage, merchantID int64, filePath string) {
	wb, err := xlsx.OpenFile(filePath)
	if err != nil {
		logger.Error("OpenFile", zap.Error(err))
		close(abortChannel)
		return
	}

	var toUpsert []postgresql.Product
	var toDelete []int64

	sh := wb.Sheets[0]

	total := int64(sh.MaxRow) - 1
	var ignored int64

	err = sh.ForEachRow(func(row *xlsx.Row) error {
		if row.GetCoordinate() == 0 {
			return nil
		}

		offerIDFloat, err := row.GetCell(0).Float()
		if err != nil {
			ignored += 1
			return nil
		}

		name := row.GetCell(1).String()
		if name == "" {
			ignored += 1
			return nil
		}

		priceFloat, err := row.GetCell(2).Float()
		if err != nil {
			ignored += 1
			return nil
		}

		quantity, err := row.GetCell(3).Float()
		if err != nil {
			ignored += 1
			return nil
		}

		var available bool
		switch row.GetCell(4).Value {
		case "true":
			available = true
		case "false":
			available = false
		case "1":
			available = true
		case "0":
			available = false
		default:
			ignored += 1
			return nil
		}

		if available {
			toUpsert = append(toUpsert, postgresql.Product{
				MerchantID: merchantID,
				OfferID:    decimal.NewFromFloat(offerIDFloat).IntPart(),
				Name:       name,
				Price:      decimal.NewFromFloat(priceFloat),
				Quantity:   decimal.NewFromFloat(quantity).IntPart(),
			})
		} else {
			toDelete = append(toDelete, decimal.NewFromFloat(offerIDFloat).IntPart())
		}

		return nil
	})

	var inserted, updated, deleted int64
	switch {
	case len(toUpsert) != 0 && len(toDelete) != 0:
		inserted, updated, deleted, err = db.UpsertAndDelete(ctx, toUpsert, merchantID, toDelete)
	case len(toUpsert) != 0:
		inserted, updated, err = db.Upsert(ctx, toUpsert)
	case len(toDelete) != 0:
		deleted, err = db.Delete(ctx, merchantID, toDelete)
	}

	if err != nil {
		logger.Error("", zap.Error(err))
		close(abortChannel)
		return
	}

	result := taskResult{
		data: dataPayload{
			added:   inserted,
			updated: updated,
			removed: deleted,
			ignored: total - (inserted + updated + deleted),
		},
		error: nil,
	}

	if ctx.Err() == nil {
		resultChannel <- result
		close(resultChannel)
		close(abortChannel)

		return
	}

	return
}
