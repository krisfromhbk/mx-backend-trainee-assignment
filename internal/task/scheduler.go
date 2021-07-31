package task

import (
	"context"
	"errors"
	"github.com/rs/xid"
	"go.uber.org/zap"
	"mx/internal/storage/postgresql"
	"sync"
	"time"
)

var (
	ErrCanNotCancel = errors.New("task can not be canceled due to its current state")
	ErrBadTaskID    = errors.New("no such task")
)

type store struct {
	rw    sync.RWMutex
	tasks map[xid.ID]task
}

type cancelChannels struct {
	rw             sync.Mutex
	cancelChannels map[xid.ID]chan struct{}
	stopChannels   map[xid.ID]chan struct{}
}

type Scheduler struct {
	logger         *zap.Logger
	taskTimeout    time.Duration
	taskStore      *store
	cancelChannels *cancelChannels
	db             *postgresql.Storage
}

func NewScheduler(logger *zap.Logger, db *postgresql.Storage) (*Scheduler, error) {
	if logger == nil {
		return nil, errors.New("no logger provided")
	}

	taskStore := &store{
		rw:    sync.RWMutex{},
		tasks: make(map[xid.ID]task),
	}

	cancelChannels := &cancelChannels{
		rw:             sync.Mutex{},
		cancelChannels: make(map[xid.ID]chan struct{}),
		stopChannels:   make(map[xid.ID]chan struct{}),
	}

	scheduler := &Scheduler{
		logger:         logger,
		taskTimeout:    20 * time.Second,
		taskStore:      taskStore,
		cancelChannels: cancelChannels,
		db:             db,
	}

	return scheduler, nil
}

func (s *Scheduler) NewTask(taskID xid.ID, merchantID int64, filePath string) {
	logger := s.logger.With(zap.String("task_id", taskID.String()))
	logger.Info("creating new task")

	t := task{
		state: Processing,
		result: taskResult{
			data: dataPayload{
				added:   0,
				updated: 0,
				removed: 0,
				ignored: 0,
			},
			error: nil,
		},
	}

	logger.Info("saving initial task state to memory")

	s.taskStore.rw.Lock()
	s.taskStore.tasks[taskID] = t
	s.taskStore.rw.Unlock()

	go s.schedule(context.Background(), logger, taskID, merchantID, filePath)
}

func (s *Scheduler) ReadTaskStatus(stringID string) (string, error) {
	id, err := xid.FromString(stringID)
	if err != nil {
		return "", ErrBadTaskID
	}

	s.taskStore.rw.RLock()
	task, ok := s.taskStore.tasks[id]
	s.taskStore.rw.RUnlock()

	if !ok {
		return "", ErrBadTaskID
	}

	if task.state == Done {
		return "State: " + task.state.String() + "\nStats: " + task.result.data.String(), nil
	}

	return "State: " + task.state.String(), nil
}

func (s *Scheduler) CancelTask(stringID string) error {
	id, err := xid.FromString(stringID)
	if err != nil {
		return ErrBadTaskID
	}

	s.taskStore.rw.RLock()
	task, ok := s.taskStore.tasks[id]
	s.taskStore.rw.RUnlock()

	if !ok {
		return ErrBadTaskID
	}

	if task.state != Processing {
		return ErrCanNotCancel
	}

	s.cancelChannels.rw.Lock()
	//select {
	//case <-s.cancelChannels.stopChannels[id]:
	//	err = ErrCanNotCancel
	//default:
	//	s.cancelChannels.cancelChannels[id] <- struct{}{}
	//}
	// TODO: test if next two lines can lead to concurrent writing to closed channel
	s.cancelChannels.cancelChannels[id] <- struct{}{}
	close(s.cancelChannels.cancelChannels[id])
	s.cancelChannels.rw.Unlock()

	return err
}

// schedule prepares and starts goroutines that process task
// only this function is responsible for changing task state
// signals for such updates come through cancelChannels
func (s *Scheduler) schedule(ctx context.Context, logger *zap.Logger, id xid.ID, merchantID int64, filePath string) {
	logger.Info("scheduling task")
	ctx, cancel := context.WithTimeout(ctx, s.taskTimeout)
	defer cancel()

	resultCh := make(chan taskResult)
	abortCh := make(chan struct{})
	cancelCh := make(chan struct{})
	stopCh := make(chan struct{})

	s.cancelChannels.rw.Lock()
	s.cancelChannels.cancelChannels[id] = cancelCh
	s.cancelChannels.stopChannels[id] = stopCh
	s.cancelChannels.rw.Unlock()

	go processTask(ctx, logger, resultCh, abortCh, s.db, merchantID, filePath)

	select {
	// processing timing out
	case <-ctx.Done():
		logger.Info("task is timed out")
		s.updateTaskState(id, TimedOut)

	// processing cancellation
	case <-cancelCh:
		logger.Info("task is canceled")
		// any schedule goroutine is the only sender for this channel
		// while any http-request calling CancelTask is a receiver
		close(stopCh)
		s.updateTaskState(id, Canceled)

	// processing "in-task" error
	case <-abortCh:
		logger.Info("task is aborted")
		s.updateTaskState(id, Aborted)

	// processing successful finishing
	case result := <-resultCh:
		logger.Info("task is done")
		s.taskStore.rw.Lock()
		t := s.taskStore.tasks[id]
		t.state = Done
		t.result = result
		s.taskStore.tasks[id] = t
		s.taskStore.rw.Unlock()
		s.updateTaskState(id, Done)
	}

	s.cancelChannels.rw.Lock()
	delete(s.cancelChannels.cancelChannels, id)
	delete(s.cancelChannels.stopChannels, id)
	s.cancelChannels.rw.Unlock()
}

func (s *Scheduler) updateTaskState(id xid.ID, state taskState) {
	s.taskStore.rw.Lock()
	t := s.taskStore.tasks[id]
	t.state = state
	s.taskStore.tasks[id] = t
	s.taskStore.rw.Unlock()
}
