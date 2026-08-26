package boltrepo

import (
	"encoding/binary"
	"fmt"
	"sort"
	"time"

	bolt "go.etcd.io/bbolt"

	"simq/internal/queue"
)

func (r *Repository) ListDeadLetterSources(command queue.ListDeadLetterSourcesCommand) (queue.ListDeadLetterSourcesPage, error) {
	result := queue.ListDeadLetterSourcesPage{Queues: []queue.Queue{}}
	err := r.view(func(tx *bolt.Tx) error {
		if _, found, err := getQueueByRef(tx, command.Target); err != nil {
			return err
		} else if !found {
			return queue.ErrQueueDoesNotExist
		}
		revision, err := redriveRevision(tx)
		if err != nil {
			return err
		}
		if command.ExpectedRevision != nil && *command.ExpectedRevision != revision {
			return queue.ErrInvalidPaginationToken
		}
		result.RedriveRevision = revision
		limit := command.Limit
		if limit < 1 {
			limit = 1000
		}
		cursor := tx.Bucket(redrivePoliciesBucket).Cursor()
		for key, encoded := cursor.First(); key != nil; key, encoded = cursor.Next() {
			if string(key) <= command.After {
				continue
			}
			var policy redrivePolicyRecord
			if err := decodeRecord(encoded, &policy); err != nil {
				return err
			}
			if policy.TargetName != command.Target.Name || policy.TargetID != command.Target.ID {
				continue
			}
			if len(result.Queues) == limit {
				result.HasMore = true
				break
			}
			value, found, err := getQueue(tx, string(key))
			if err != nil {
				return err
			}
			if !found {
				return corruptf("redrive policy refers to a missing source")
			}
			result.Queues = append(result.Queues, value)
		}
		return nil
	})
	if err != nil {
		return queue.ListDeadLetterSourcesPage{}, wrapOperationError("list dead-letter sources", err)
	}
	return result, nil
}

func (r *Repository) StartMoveTask(command queue.StartMoveTaskCommand) (queue.MessageMoveTask, error) {
	var result queue.MessageMoveTask
	err := r.update(func(tx *bolt.Tx) error {
		if tx.Bucket(moveTasksBucket).Get([]byte(command.Handle)) != nil {
			return queue.ErrMessageIDExists
		}
		source, found, err := getQueueByRef(tx, command.Source)
		if err != nil {
			return err
		} else if !found {
			return queue.ErrQueueDoesNotExist
		}
		isDLQ := false
		if err := tx.Bucket(redrivePoliciesBucket).ForEach(func(key, encoded []byte) error {
			var policy redrivePolicyRecord
			if err := decodeRecord(encoded, &policy); err != nil {
				return err
			}
			if policy.TargetName == command.Source.Name && policy.TargetID == command.Source.ID {
				isDLQ = true
			}
			return nil
		}); err != nil {
			return err
		}
		if !isDLQ {
			return queue.ErrQueueIsNotDeadLetter
		}
		if command.Destination != nil {
			if *command.Destination == command.Source {
				return &queue.InvalidRequestError{Message: "move destination must differ from source"}
			}
			destination, found, err := getQueueByRef(tx, *command.Destination)
			if err != nil {
				return err
			} else if !found {
				return queue.ErrQueueDoesNotExist
			}
			if destination.FIFO != source.FIFO {
				return &queue.InvalidRequestError{Message: "move source and destination must have the same queue type"}
			}
		}
		if err := tx.Bucket(moveTasksBucket).ForEach(func(key, encoded []byte) error {
			var record moveTaskRecord
			if err := decodeRecord(encoded, &record); err != nil {
				return err
			}
			task, err := record.domain()
			if err != nil {
				return err
			}
			if task.Source == command.Source && task.Status == queue.MoveTaskRunning {
				return queue.ErrMoveTaskAlreadyRunning
			}
			return nil
		}); err != nil {
			return err
		}
		ordered := tx.Bucket(messageOrderBucket).Bucket([]byte(command.Source.Name))
		if ordered == nil {
			return corruptf("move source is missing its message order bucket")
		}
		boundary := ordered.Sequence()
		var count uint64
		if err := ordered.ForEach(func(key, value []byte) error {
			if len(key) != 8 || binary.BigEndian.Uint64(key) == 0 || value == nil {
				return corruptf("message order entry is malformed")
			}
			if binary.BigEndian.Uint64(key) <= boundary {
				count++
			}
			return nil
		}); err != nil {
			return err
		}
		status := queue.MoveTaskRunning
		if count == 0 {
			status = queue.MoveTaskCompleted
		}
		result = queue.MessageMoveTask{Handle: command.Handle, Source: command.Source, Destination: cloneQueueRef(command.Destination), MaxNumberOfMessagesPerSecond: command.MaxMessagesPerSecond, StartedAtMillis: command.Now.UnixMilli(), Status: status, ApproximateNumberOfMessagesToMove: count, Boundary: boundary}
		encoded, err := encodeRecord(newMoveTaskRecord(result))
		if err != nil {
			return err
		}
		return tx.Bucket(moveTasksBucket).Put([]byte(result.Handle), encoded)
	})
	if err != nil {
		return queue.MessageMoveTask{}, wrapOperationError("start message move task", err)
	}
	return result, nil
}

func (r *Repository) ListMoveTasks(source queue.QueueRef, limit int) ([]queue.MessageMoveTask, error) {
	result := []queue.MessageMoveTask{}
	err := r.view(func(tx *bolt.Tx) error {
		if _, found, err := getQueueByRef(tx, source); err != nil {
			return err
		} else if !found {
			return queue.ErrQueueDoesNotExist
		}
		return tx.Bucket(moveTasksBucket).ForEach(func(key, encoded []byte) error {
			var record moveTaskRecord
			if err := decodeRecord(encoded, &record); err != nil {
				return err
			}
			task, err := record.domain()
			if err != nil {
				return err
			}
			if task.Source == source {
				result = append(result, task)
			}
			return nil
		})
	})
	if err != nil {
		return nil, wrapOperationError("list message move tasks", err)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].StartedAtMillis == result[j].StartedAtMillis {
			return result[i].Handle > result[j].Handle
		}
		return result[i].StartedAtMillis > result[j].StartedAtMillis
	})
	if len(result) > limit {
		result = result[:limit]
	}
	return result, nil
}

func (r *Repository) CancelMoveTask(handle string, now time.Time) (queue.MessageMoveTask, error) {
	var result queue.MessageMoveTask
	err := r.update(func(tx *bolt.Tx) error {
		encoded := tx.Bucket(moveTasksBucket).Get([]byte(handle))
		if encoded == nil {
			return queue.ErrMoveTaskDoesNotExist
		}
		var record moveTaskRecord
		if err := decodeRecord(encoded, &record); err != nil {
			return err
		}
		task, err := record.domain()
		if err != nil {
			return err
		}
		if task.Status != queue.MoveTaskRunning {
			return queue.ErrMoveTaskNotRunning
		}
		task.Status = queue.MoveTaskCancelled
		result = task
		updated, err := encodeRecord(newMoveTaskRecord(task))
		if err != nil {
			return err
		}
		return tx.Bucket(moveTasksBucket).Put([]byte(handle), updated)
	})
	if err != nil {
		return queue.MessageMoveTask{}, wrapOperationError("cancel message move task", err)
	}
	return result, nil
}

func (r *Repository) RunningMoveTasks() ([]queue.MessageMoveTask, error) {
	result := []queue.MessageMoveTask{}
	err := r.view(func(tx *bolt.Tx) error {
		return tx.Bucket(moveTasksBucket).ForEach(func(key, encoded []byte) error {
			var record moveTaskRecord
			if err := decodeRecord(encoded, &record); err != nil {
				return err
			}
			task, err := record.domain()
			if err != nil {
				return err
			}
			if task.Status == queue.MoveTaskRunning {
				result = append(result, task)
			}
			return nil
		})
	})
	if err != nil {
		return nil, wrapOperationError("list running move tasks", err)
	}
	return result, nil
}

func (r *Repository) MoveTaskStep(handle string, now time.Time) (queue.MoveTaskStepResult, error) {
	var result queue.MoveTaskStepResult
	err := r.update(func(tx *bolt.Tx) error {
		encoded := tx.Bucket(moveTasksBucket).Get([]byte(handle))
		if encoded == nil {
			return queue.ErrMoveTaskDoesNotExist
		}
		var taskRecord moveTaskRecord
		if err := decodeRecord(encoded, &taskRecord); err != nil {
			return err
		}
		task, err := taskRecord.domain()
		if err != nil {
			return err
		}
		if task.Status != queue.MoveTaskRunning {
			result = queue.MoveTaskStepResult{Task: task, Terminal: true}
			return nil
		}
		sourceQueue, found, err := getQueueByRef(tx, task.Source)
		if err != nil {
			return err
		} else if !found {
			return failMoveTask(tx, &result, task, "source queue generation no longer exists")
		}
		sourceOrder := tx.Bucket(messageOrderBucket).Bucket([]byte(task.Source.Name))
		if sourceOrder == nil {
			return corruptf("move source is missing its message order bucket")
		}
		var orderKey, messageID []byte
		cursor := sourceOrder.Cursor()
		for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
			if len(key) != 8 || binary.BigEndian.Uint64(key) == 0 || value == nil {
				return corruptf("message order entry is malformed")
			}
			if binary.BigEndian.Uint64(key) <= task.Boundary {
				orderKey, messageID = append([]byte(nil), key...), append([]byte(nil), value...)
				break
			}
		}
		if orderKey == nil {
			task.Status = queue.MoveTaskCompleted
			updated, err := encodeRecord(newMoveTaskRecord(task))
			if err != nil {
				return err
			}
			if err := tx.Bucket(moveTasksBucket).Put([]byte(handle), updated); err != nil {
				return err
			}
			result = queue.MoveTaskStepResult{Task: task, Terminal: true}
			return nil
		}
		encodedMessage := tx.Bucket(messagesBucket).Get(messageID)
		if encodedMessage == nil {
			return corruptf("move order refers to a missing message")
		}
		var message messageRecord
		if err := decodeRecord(encodedMessage, &message); err != nil {
			return err
		}
		var destination queue.QueueRef
		if task.Destination != nil {
			destination = *task.Destination
		} else {
			originValue := tx.Bucket(redriveOriginsBucket).Get(messageID)
			if originValue == nil {
				return failMoveTask(tx, &result, task, "message original destination is unavailable")
			}
			var origin redriveOriginRecord
			if err := decodeRecord(originValue, &origin); err != nil {
				return err
			}
			if origin.Version != redriveRecordVersion || origin.MessageID != string(messageID) {
				return corruptf("redrive origin record is invalid")
			}
			destination = queue.QueueRef{Name: origin.SourceName, ID: origin.SourceID}
		}
		destinationQueue, found, err := getQueueByRef(tx, destination)
		if err != nil {
			return err
		}
		if !found {
			return failMoveTask(tx, &result, task, "message original destination is unavailable")
		}
		if destinationQueue.FIFO != sourceQueue.FIFO {
			return failMoveTask(tx, &result, task, "message destination queue type does not match source")
		}
		if sourceQueue.FIFO {
			encodedFIFO := tx.Bucket(fifoMessagesBucket).Get(messageID)
			if encodedFIFO == nil {
				return corruptf("FIFO message metadata is missing")
			}
			var metadata fifoMessageRecord
			if err := decodeRecord(encodedFIFO, &metadata); err != nil {
				return err
			}
			if metadata.Version != fifoRecordVersion || metadata.MessageID != string(messageID) || metadata.QueueID != sourceQueue.ID || metadata.GroupID == "" || metadata.DeduplicationID == "" || metadata.Sequence == 0 {
				return corruptf("FIFO message metadata is invalid")
			}
			metadata.QueueID = destinationQueue.ID
			metadata.Sequence, err = allocateFIFOSequence(tx, destinationQueue)
			if err != nil {
				return err
			}
			updatedFIFO, err := encodeRecord(metadata)
			if err != nil {
				return err
			}
			if err := tx.Bucket(fifoMessagesBucket).Put(messageID, updatedFIFO); err != nil {
				return err
			}
		}
		message.QueueName = destination.Name
		message.SentAtUnixMillis = now.UnixMilli()
		message.AvailableAtUnixNanos = now.UnixNano()
		message.ExpiresAtUnixNanos = now.Add(time.Duration(destinationQueue.MessageRetentionPeriod) * time.Second).UnixNano()
		message.ReceiptHandle = ""
		message.ReceiveCount = 0
		message.FirstReceivedAtUnixMillis = 0
		message.VisibilityDeadlineUnixNanos = 0
		messageValue, err := encodeRecord(message)
		if err != nil {
			return err
		}
		destinationOrder := tx.Bucket(messageOrderBucket).Bucket([]byte(destination.Name))
		if destinationOrder == nil {
			return corruptf("move destination is missing its message order bucket")
		}
		sequence, err := destinationOrder.NextSequence()
		if err != nil || sequence == 0 {
			return fmt.Errorf("allocate move destination order: %w", err)
		}
		sequenceKey := make([]byte, 8)
		binary.BigEndian.PutUint64(sequenceKey, sequence)
		if err := tx.Bucket(messagesBucket).Put(messageID, messageValue); err != nil {
			return err
		}
		if err := sourceOrder.Delete(orderKey); err != nil {
			return err
		}
		if err := destinationOrder.Put(sequenceKey, messageID); err != nil {
			return err
		}
		if err := tx.Bucket(redriveOriginsBucket).Delete(messageID); err != nil {
			return err
		}
		task.ApproximateNumberOfMessagesMoved++
		updatedTask, err := encodeRecord(newMoveTaskRecord(task))
		if err != nil {
			return err
		}
		if err := tx.Bucket(moveTasksBucket).Put([]byte(handle), updatedTask); err != nil {
			return err
		}
		result = queue.MoveTaskStepResult{Task: task, Destination: &destination}
		return nil
	})
	if err != nil {
		return queue.MoveTaskStepResult{}, wrapOperationError("process message move task", err)
	}
	return result, nil
}

func failMoveTask(tx *bolt.Tx, result *queue.MoveTaskStepResult, task queue.MessageMoveTask, reason string) error {
	task.Status, task.FailureReason = queue.MoveTaskFailed, reason
	encoded, err := encodeRecord(newMoveTaskRecord(task))
	if err != nil {
		return err
	}
	if err := tx.Bucket(moveTasksBucket).Put([]byte(task.Handle), encoded); err != nil {
		return err
	}
	*result = queue.MoveTaskStepResult{Task: task, Terminal: true}
	return nil
}

func cloneQueueRef(value *queue.QueueRef) *queue.QueueRef {
	if value == nil {
		return nil
	}
	result := *value
	return &result
}
