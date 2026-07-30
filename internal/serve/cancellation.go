package serve

import (
	"context"
	"fmt"
	"time"

	"mecha.im/internal/logs"
)

// persistTaskCancellation makes forced shutdown terminal. Database writes must
// not reuse the cancelled dispatch context, or the task can remain dispatched
// and be replayed after restart.
func (s *Server) persistTaskCancellation(
	ctx context.Context,
	taskID, eventID, worker string,
	attempt int,
) bool {
	if ctx.Err() == nil {
		return false
	}

	stateCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	errMsg := fmt.Sprintf("task cancelled: %v", ctx.Err())
	if err := s.tasks.Fail(stateCtx, taskID, errMsg); err != nil {
		s.logger.Error("persist cancelled task", "id", taskID, "err", err)
	} else {
		tasksFailed.Add(1)
		s.record(logs.Entry{
			TraceID: eventID,
			TaskID:  taskID,
			Worker:  worker,
			Action:  logs.TaskDeadLetter,
			Outcome: logs.Fail,
			Attempt: attempt,
			Error:   errMsg,
		})
	}
	s.completeEvent(stateCtx, eventID, false)
	return true
}
