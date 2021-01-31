package task

import (
	"fmt"
)

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

// dataPayload defines lines that were added, updated, removed and ignored respectively during .xlsx file processing
type dataPayload struct {
	added, updated, removed, ignored int64
}

// String returns string representation of dataPayload struct
func (d dataPayload) String() string {
	result := fmt.Sprintf(
		"Added: %d, Updated: %d, Removed: %d, Ignored: %d",
		d.added,
		d.updated,
		d.removed,
		d.ignored,
	)

	return result
}

// taskResult defines fields used for processing task results
// error corresponds to potential error that might occur during task processing
type taskResult struct {
	data  dataPayload
	error error
}

// task defines fields used for general task processing including its state and result
type task struct {
	state  taskState
	result taskResult
}
