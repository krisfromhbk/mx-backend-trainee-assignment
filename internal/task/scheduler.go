package task

import (
	"context"
	"errors"
	"github.com/rs/xid"
	"go.uber.org/zap"
	"math/rand"
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

type scheduler struct {
	logger         *zap.Logger
	taskTimeout    time.Duration
	taskStore      *store
	cancelChannels *cancelChannels
}

func NewScheduler(logger *zap.Logger) *scheduler {
	taskStore := &store{
		rw:    sync.RWMutex{},
		tasks: make(map[xid.ID]task),
	}

	cancelChannels := &cancelChannels{
		rw:             sync.Mutex{},
		cancelChannels: make(map[xid.ID]chan struct{}),
		stopChannels:   make(map[xid.ID]chan struct{}),
	}

	scheduler := &scheduler{
		logger:         logger,
		taskTimeout:    20 * time.Second,
		taskStore:      taskStore,
		cancelChannels: cancelChannels,
	}

	return scheduler
}

func (s *scheduler) NewTask() string {
	id := xid.New()
	logger := s.logger.With(zap.String("ID", id.String()))
	logger.Info("Creating new task")

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

	logger.Info("Saving task state to memory")

	s.taskStore.rw.Lock()
	s.taskStore.tasks[id] = t
	s.taskStore.rw.Unlock()

	go s.schedule(context.Background(), logger, id)

	return id.String()
}

func (s *scheduler) ReadTaskStatus(stringID string) (string, error) {
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

	return task.state.String(), nil
}

func (s *scheduler) CancelTask(stringID string) error {
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
	// TODO: test if next two lines can lead to concurrent writing to close channel
	s.cancelChannels.cancelChannels[id] <- struct{}{}
	close(s.cancelChannels.cancelChannels[id])
	s.cancelChannels.rw.Unlock()

	return err
}

// schedule prepares and starts goroutines that process task
// only this function is responsible for changing task state
// signals for such updates come through cancelChannels
func (s *scheduler) schedule(ctx context.Context, logger *zap.Logger, id xid.ID) {
	logger.Info("Scheduling task")
	ctx, cancel := context.WithTimeout(ctx, s.taskTimeout)
	defer cancel()

	doneCh := make(chan struct{})
	abortCh := make(chan struct{})
	cancelCh := make(chan struct{})
	stopCh := make(chan struct{})

	s.cancelChannels.rw.Lock()
	s.cancelChannels.cancelChannels[id] = cancelCh
	s.cancelChannels.stopChannels[id] = stopCh
	s.cancelChannels.rw.Unlock()

	go processTask(ctx, logger, doneCh, abortCh)

	select {
	// processing timing out
	case <-ctx.Done():
		logger.Info("Task is timed out")
		s.updateTaskState(id, TimedOut)

	// processing cancellation
	case <-cancelCh:
		logger.Info("Task is canceled")
		// any schedule goroutine is the only sender for this channel
		// while any http-request calling CancelTask is a receiver
		close(stopCh)
		s.updateTaskState(id, Canceled)

	// processing "in-task" error
	case <-abortCh:
		logger.Info("Task is aborted")
		s.updateTaskState(id, Aborted)

	// processing successful finishing
	case <-doneCh:
		logger.Info("Task is done")
		s.updateTaskState(id, Done)
	}

	s.cancelChannels.rw.Lock()
	delete(s.cancelChannels.cancelChannels, id)
	delete(s.cancelChannels.stopChannels, id)
	s.cancelChannels.rw.Unlock()
}

func (s *scheduler) updateTaskState(id xid.ID, state taskState) {
	s.taskStore.rw.Lock()
	t := s.taskStore.tasks[id]
	t.state = state
	s.taskStore.tasks[id] = t
	s.taskStore.rw.Unlock()
}

func processTask(ctx context.Context, logger *zap.Logger, doneChannel, abortChannel chan<- struct{}) {
	toAbort := rand.Intn(2)
	if toAbort == 1 {
		logger.Info("Task will be aborted immediately")
		abortChannel <- struct{}{}
		close(abortChannel)

		return
	}

	d := rand.Intn(40)
	logger.Info("Task will be suspended", zap.Int("seconds", d))
	time.Sleep(time.Second * time.Duration(d))

	if ctx.Err() == nil {
		doneChannel <- struct{}{}
		close(doneChannel)

		return
	}

	return
}
