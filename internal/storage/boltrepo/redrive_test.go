package boltrepo

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"simq/internal/queue"
)

func TestRunningMoveTaskSurvivesRepositoryRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "simq.db")
	repository, err := Open(Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	service := queue.NewService(repository)
	source, err := service.CreateQueue(queue.CreateQueueInput{QueueName: "source"})
	if err != nil {
		t.Fatal(err)
	}
	dlq, err := service.CreateQueue(queue.CreateQueueInput{QueueName: "dead"})
	if err != nil {
		t.Fatal(err)
	}
	policy := `{"deadLetterTargetArn":"` + queue.QueueARN(queue.QueueRef{Name: dlq.Name, ID: dlq.ID}) + `","maxReceiveCount":"1"}`
	if err := service.SetQueueAttributes(queue.SetQueueAttributesInput{QueueName: source.Name, QueueID: source.ID, Attributes: map[string]string{"RedrivePolicy": policy}}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.SendMessage(queue.SendMessageInput{QueueName: dlq.Name, QueueID: dlq.ID, MessageBody: "recover"}); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 25, 14, 0, 0, 0, time.UTC)
	task, err := repository.StartMoveTask(queue.StartMoveTaskCommand{Handle: "mt_restart", Source: queue.QueueRef{Name: dlq.Name, ID: dlq.ID}, Destination: &queue.QueueRef{Name: source.Name, ID: source.ID}, MaxMessagesPerSecond: 500, Now: now})
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.Close(); err != nil {
		t.Fatal(err)
	}

	repository, err = Open(Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	running, err := repository.RunningMoveTasks()
	if err != nil || len(running) != 1 || running[0].Handle != task.Handle {
		t.Fatalf("running after restart = %#v, %v", running, err)
	}
	recoveredService := queue.NewService(repository)
	defer recoveredService.ShutdownLongPolling()
	if err := recoveredService.ResumeMessageMoveTasks(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		tasks, err := repository.ListMoveTasks(queue.QueueRef{Name: dlq.Name, ID: dlq.ID}, 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(tasks) == 1 && tasks[0].Status == queue.MoveTaskCompleted {
			if tasks[0].ApproximateNumberOfMessagesMoved != 1 {
				t.Fatalf("recovered task = %#v", tasks[0])
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("task did not resume: %#v", tasks)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestMoveTaskStepCommitFailureRollsBackMessageAndProgress(t *testing.T) {
	path := filepath.Join(t.TempDir(), "simq.db")
	repository, err := Open(Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	service := queue.NewService(repository)
	source, err := service.CreateQueue(queue.CreateQueueInput{QueueName: "source"})
	if err != nil {
		t.Fatal(err)
	}
	dlq, err := service.CreateQueue(queue.CreateQueueInput{QueueName: "dead"})
	if err != nil {
		t.Fatal(err)
	}
	policy := `{"deadLetterTargetArn":"` + queue.QueueARN(queue.QueueRef{Name: dlq.Name, ID: dlq.ID}) + `","maxReceiveCount":"1"}`
	if err := service.SetQueueAttributes(queue.SetQueueAttributesInput{QueueName: source.Name, QueueID: source.ID, Attributes: map[string]string{"RedrivePolicy": policy}}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.SendMessage(queue.SendMessageInput{QueueName: dlq.Name, QueueID: dlq.ID, MessageBody: "rollback"}); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 25, 15, 0, 0, 0, time.UTC)
	task, err := repository.StartMoveTask(queue.StartMoveTaskCommand{Handle: "mt_rollback", Source: queue.QueueRef{Name: dlq.Name, ID: dlq.ID}, Destination: &queue.QueueRef{Name: source.Name, ID: source.ID}, MaxMessagesPerSecond: 500, Now: now})
	if err != nil {
		t.Fatal(err)
	}
	originalUpdate := repository.updateFn
	repository.updateFn = func(func(*bolt.Tx) error) error { return errors.New("injected commit failure") }
	if _, err := repository.MoveTaskStep(task.Handle, now); !errors.Is(err, queue.ErrRepositoryUnavailable) {
		t.Fatalf("step failure = %v", err)
	}
	repository.updateFn = originalUpdate
	tasks, err := repository.ListMoveTasks(task.Source, 1)
	if err != nil || len(tasks) != 1 || tasks[0].ApproximateNumberOfMessagesMoved != 0 || tasks[0].Status != queue.MoveTaskRunning {
		t.Fatalf("task after rollback = %#v, %v", tasks, err)
	}
	step, err := repository.MoveTaskStep(task.Handle, now)
	if err != nil || step.Task.ApproximateNumberOfMessagesMoved != 1 {
		t.Fatalf("retry = %#v, %v", step, err)
	}
}

func TestSchemaV4MigrationCreatesRedriveBucketsAndBackup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "simq.db")
	repository, err := Open(Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		if err := deleteM8Buckets(tx); err != nil {
			return err
		}
		if err := tx.DeleteBucket(tenantUsageBucket); err != nil {
			return err
		}
		if err := tx.DeleteBucket(replicationProposalsBucket); err != nil {
			return err
		}
		for _, name := range [][]byte{redrivePoliciesBucket, redriveOriginsBucket, moveTasksBucket, fifoQueuesBucket, fifoSequencesBucket, fifoMessagesBucket, fifoDedupBucket, fifoAttemptsBucket} {
			if err := tx.DeleteBucket(name); err != nil {
				return err
			}
		}
		if err := tx.Bucket(metadataBucket).Delete(redriveRevisionKey); err != nil {
			return err
		}
		if err := tx.Bucket(metadataBucket).Delete(appliedRaftIndexKey); err != nil {
			return err
		}
		if err := tx.Bucket(metadataBucket).Delete(commandProtocolVersionKey); err != nil {
			return err
		}
		version := make([]byte, 4)
		binary.BigEndian.PutUint32(version, schemaVersionV4)
		return tx.Bucket(metadataBucket).Put(schemaVersionKey, version)
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	repository, err = Open(Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path + schemaV4BackupSuffix)
	if err != nil || info.Size() == 0 {
		t.Fatalf("schema-v4 backup = %#v, %v", info, err)
	}
}
