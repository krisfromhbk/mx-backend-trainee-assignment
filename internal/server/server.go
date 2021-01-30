package server

import (
	"context"
	"errors"
	"fmt"
	"go.uber.org/zap"
	"mx/internal/storage/postgresql"
	"mx/internal/task"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)

// Server defines fields used in HTTP processing
type Server struct {
	logger        *zap.Logger
	httpServer    *http.Server
	afterShutdown func() error
}

// NewServer constructs a Server
func NewServer(logger *zap.Logger, scheduler *task.Scheduler, db *postgresql.Storage) (*Server, error) {
	if logger == nil {
		return nil, errors.New("no logger provided")
	}

	h := handler{
		logger:    logger,
		scheduler: scheduler,
		db:        db,
	}

	mux := http.NewServeMux()
	mux.Handle("/upload", http.HandlerFunc(h.handleUpload))
	mux.Handle("/tasks", http.HandlerFunc(h.handleTaskStatus))
	mux.Handle("/list", http.HandlerFunc(h.listProducts))

	httpServer := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	return &Server{
		logger:     logger,
		httpServer: httpServer,
	}, nil
}

// Start calls ListenAndServe on http.Server instance inside Server struct
// and implements graceful shutdown via goroutine waiting for signals
func (s *Server) Start() error {
	idleConnsClosed := make(chan struct{})

	go func() {
		sigint := make(chan os.Signal, 1)
		signal.Notify(sigint, syscall.SIGINT, syscall.SIGTERM)
		<-sigint

		s.logger.Info("Shutting down HTTP server")

		if err := s.httpServer.Shutdown(context.Background()); err != nil {
			s.logger.Error("srv.Shutdown: %v", zap.Error(err))
		}
		s.logger.Info("HTTP server is stopped")

		close(idleConnsClosed)
	}()

	s.logger.Info("Starting HTTP server")
	if err := s.httpServer.ListenAndServe(); err != http.ErrServerClosed {
		return fmt.Errorf("s.httpServer.ListenAndServe: %v", err)
	}

	<-idleConnsClosed

	return s.afterShutdown()
}

// RegisterAfterShutdown registers provided function to be called after Server shutdown
func (s *Server) RegisterAfterShutdown(f func() error) {
	s.afterShutdown = f
}
