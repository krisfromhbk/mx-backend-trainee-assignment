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

func (s *Storage) UpsertAndDelete(ctx context.Context, toUpsert []Product, merchantID int64, toDelete []int64) (int64, int64, int64, error) {
	var inserted, updated, deleted int64
	var err error

	s.logger.Debug("Starting parent transaction")

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return 0, 0, 0, err
	}
	defer tx.Rollback(context.Background())

	if len(toUpsert) != 0 {
		inserted, updated, err = s.Upsert(ctx, toUpsert, asNestedTo(tx))
		if err != nil {
			return 0, 0, 0, err
		}
	}

	if len(toDelete) != 0 {
		deleted, err = s.Delete(ctx, merchantID, toDelete, asNestedTo(tx))
		if err != nil {
			return 0, 0, 0, err
		}
	}

	ctxErr := ctx.Err()
	if ctxErr != nil {
		switch {
		case errors.Is(ctxErr, context.DeadlineExceeded):
			s.logger.Info("Task deadline exceeded")
			return 0, 0, 0, ctxErr

		case errors.Is(ctxErr, context.Canceled):
			s.logger.Info("Task is canceled")
			return 0, 0, 0, ctxErr
		}
	}

	err = tx.Commit(ctx)
	if err != nil {
		s.logger.Error("Commit transaction", zap.Error(err))
		return 0, 0, 0, err
	}

	return inserted, updated, deleted, nil
}
