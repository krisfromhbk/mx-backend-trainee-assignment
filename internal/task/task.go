package task

import "github.com/vmihailenco/msgpack/v5"

// taskState defines helper type to describe different task states
// one should probably think of state guarantees taking into account situations
// when updating task state in some database may result in an error
//go:generate stringer -type=taskState
type taskState int

const (
	// Processing defines task state right after .xlsx file was uploaded and begun processed
	Processing taskState = iota
	// Done defines task state when all processing is finished and result data is available
	Done
	// TimedOut defines task state when its processing took longer than specified timeout
	TimedOut
	// Canceled defines task state when it was explicitly canceled by user
	Canceled
	// Aborted defines task state when it was implicitly canceled by error while processing e.g. some IO operation
	Aborted
)

// taskResult defines fields used for processing task results
type taskResult struct {
	// data defines lines that were added, updated, removed and ignored respectively during .xlsx file processing
	data struct {
		added, updated, removed, ignored uint
	}
	// error corresponds to potential error that might occur during task processing
	error error
}

// task defines fields used for general task processing including its state and result
type task struct {
	state  taskState
	result taskResult
}

// taskToBytes returns byte slice representation of task struct
func taskToBytes(t task) ([]byte, error) {
	return msgpack.Marshal(t)
}

// bytesToTask returns task represented by its byte slice
func bytesToTask(b []byte) (task, error) {
	var t task

	err := msgpack.Unmarshal(b, &t)
	if err != nil {
		return task{}, err
	}

	return t, err
}
