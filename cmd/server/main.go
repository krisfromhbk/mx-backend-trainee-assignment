package main

import (
	"context"
	"github.com/joho/godotenv"
	"go.uber.org/zap"
	"log"
	"math/rand"
	"mx/internal/server"
	"mx/internal/storage/postgresql"
	"mx/internal/task"
	"time"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("failed to load .env: %v\n", err)
	}

	rand.Seed(time.Now().Unix())

	logger, _ := zap.NewDevelopment()
	defer logger.Sync()

	db, err := postgresql.NewStorage(context.Background(), logger)
	if err != nil {
		logger.Fatal("failed to create storage", zap.Error(err))
	}

	scheduler, err := task.NewScheduler(logger, db)
	if err != nil {
		logger.Fatal("failed to create scheduler", zap.Error(err))
	}

	srv, err := server.NewServer(logger, scheduler, db)
	if err != nil {
		logger.Fatal("failed to create server", zap.Error(err))
	}

	srv.RegisterAfterShutdown(func() error {
		db.Close()
		return nil
	})

	err = srv.Start()
	if err != nil {
		logger.Fatal("failed to start server", zap.Error(err))
	}
}
