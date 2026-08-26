package boltrepo

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"simq/internal/queue"
)

type failureClock struct {
	now time.Time
}

func (c *failureClock) Now() time.Time {
	return c.now
}

func TestUpdateCallbackErrorRollsBackPartialMutation(t *testing.T) {
	repository, path := openFailureTestRepository(t)
	sentinel := errors.New("injected callback failure")
	err := repository.db.Update(func(tx *bolt.Tx) error {
		if err := tx.Bucket(queuesBucket).Put([]byte("partial"), []byte("partial")); err != nil {
			return err
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("Update error = %v, want injected failure", err)
	}
	if err := repository.db.View(func(tx *bolt.Tx) error {
		if value := tx.Bucket(queuesBucket).Get([]byte("partial")); value != nil {
			t.Fatalf("partial value committed: %q", value)
		}
		return nil
	}); err != nil {
		t.Fatalf("View: %v", err)
	}
	if err := repository.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("database disappeared: %v", err)
	}
}

func TestSendMessageBatchCommitFailureCannotProduceFalseSuccess(t *testing.T) {
	repository, path := openFailureTestRepository(t)
	service := queue.NewService(repository)
	if _, err := service.CreateQueue(queue.CreateQueueInput{QueueName: "orders"}); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	originalUpdate := repository.updateFn
	updates := 0
	repository.updateFn = func(callback func(*bolt.Tx) error) error {
		updates++
		if updates == 2 {
			return errors.New("injected batch commit failure")
		}
		return originalUpdate(callback)
	}
	result, err := service.SendMessageBatch(queue.SendMessageBatchInput{QueueName: "orders", Entries: []queue.SendMessageBatchEntry{{ID: "first", MessageBody: "one"}, {ID: "failed", MessageBody: "secret-body"}, {ID: "third", MessageBody: "three"}}})
	if err != nil {
		t.Fatalf("SendMessageBatch request: %v", err)
	}
	if len(result.Successful) != 2 || result.Successful[0].ID != "first" || result.Successful[1].ID != "third" || len(result.Failed) != 1 || result.Failed[0].ID != "failed" || !errors.Is(result.Failed[0].Error, queue.ErrRepositoryUnavailable) {
		t.Fatalf("batch result = %#v", result)
	}
	repository.updateFn = originalUpdate
	if err := repository.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	reopened, err := Open(Config{Path: path, OpenTimeout: time.Second})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	messages := queue.NewService(reopened)
	received, err := messages.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders", MaxNumberOfMessages: intPointerForFailure(10)})
	if err != nil || len(received) != 2 || received[0].Body != "one" || received[1].Body != "three" {
		t.Fatalf("recovered committed batch entries = %#v, %v", received, err)
	}
}

func intPointerForFailure(value int) *int { return &value }

func TestUpdateCallbackPanicBecomesErrorAndRollsBack(t *testing.T) {
	repository, _ := openFailureTestRepository(t)
	defer repository.Close()
	err := repository.update(func(tx *bolt.Tx) error {
		if err := tx.Bucket(queuesBucket).Put([]byte("partial"), []byte("partial")); err != nil {
			return err
		}
		panic("injected panic")
	})
	if !errors.Is(err, queue.ErrRepositoryCorrupt) {
		t.Fatalf("update error = %v, want fail-closed corruption error", err)
	}
	if err := repository.db.View(func(tx *bolt.Tx) error {
		if value := tx.Bucket(queuesBucket).Get([]byte("partial")); value != nil {
			t.Fatalf("partial value committed after panic: %q", value)
		}
		return nil
	}); err != nil {
		t.Fatalf("View: %v", err)
	}
}

func TestInjectedCommitFailureDoesNotCreateQueue(t *testing.T) {
	repository, _ := openFailureTestRepository(t)
	defer repository.Close()
	commitFailure := errors.New("injected commit failure")
	repository.updateFn = func(callback func(*bolt.Tx) error) error {
		tx, err := repository.db.Begin(true)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if err := callback(tx); err != nil {
			return err
		}
		if err := tx.Rollback(); err != nil {
			return err
		}
		return commitFailure
	}

	_, err := repository.Create(queue.Queue{Name: "orders", VisibilityTimeout: 30})
	if !errors.Is(err, commitFailure) || !errors.Is(err, queue.ErrRepositoryUnavailable) {
		t.Fatalf("Create error = %v, want commit and unavailable errors", err)
	}
	stored, found, err := repository.Get("orders")
	if err != nil || found {
		t.Fatalf("Get after failed commit = %#v, %v, %v", stored, found, err)
	}
}

func TestInjectedCommitFailuresRollBackPurgeAndDeleteQueue(t *testing.T) {
	repository, _ := openFailureTestRepository(t)
	defer repository.Close()
	service := queue.NewService(repository)
	created, err := service.CreateQueue(queue.CreateQueueInput{QueueName: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.SendMessage(queue.SendMessageInput{QueueName: created.Name, QueueID: created.ID, MessageBody: "preserved"}); err != nil {
		t.Fatal(err)
	}
	if err := service.TagQueue(queue.TagQueueInput{QueueName: created.Name, QueueID: created.ID, Tags: map[string]string{"env": "prod"}}); err != nil {
		t.Fatal(err)
	}
	original := repository.updateFn
	commitFailure := errors.New("injected administration commit failure")
	injected := func(callback func(*bolt.Tx) error) error {
		tx, err := repository.db.Begin(true)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if err := callback(tx); err != nil {
			return err
		}
		if err := tx.Rollback(); err != nil {
			return err
		}
		return commitFailure
	}
	repository.updateFn = injected
	if err := service.PurgeQueue(queue.QueueRef{Name: created.Name, ID: created.ID}); !errors.Is(err, commitFailure) || !errors.Is(err, queue.ErrRepositoryUnavailable) {
		t.Fatalf("PurgeQueue = %v", err)
	}
	repository.updateFn = original
	messages, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: created.Name, QueueID: created.ID})
	if err != nil || len(messages) != 1 || messages[0].Body != "preserved" {
		t.Fatalf("message after failed purge = %#v, %v", messages, err)
	}

	repository.updateFn = injected
	if err := service.DeleteQueue(queue.QueueRef{Name: created.Name, ID: created.ID}); !errors.Is(err, commitFailure) || !errors.Is(err, queue.ErrRepositoryUnavailable) {
		t.Fatalf("DeleteQueue = %v", err)
	}
	repository.updateFn = original
	if value, found, err := repository.GetByRef(queue.QueueRef{Name: created.Name, ID: created.ID}); err != nil || !found || value.ID != created.ID {
		t.Fatalf("queue after failed delete = %#v, %v, %v", value, found, err)
	}
	tags, err := service.ListQueueTags(queue.QueueRef{Name: created.Name, ID: created.ID})
	if err != nil || tags["env"] != "prod" {
		t.Fatalf("tags after failed delete = %#v, %v", tags, err)
	}
}

func TestInjectedUpdateFailureRollsBackAttributeBearingEnqueue(t *testing.T) {
	repository, _ := openFailureTestRepository(t)
	defer repository.Close()
	service := queue.NewService(repository,
		queue.WithClock(&failureClock{now: time.Unix(100, 123)}),
		queue.WithMessageIDGenerator(func() string { return "message-attributes-failure" }),
	)
	if _, err := service.CreateQueue(queue.CreateQueueInput{QueueName: "orders"}); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	commitFailure := errors.New("injected attribute enqueue commit failure")
	repository.updateFn = func(callback func(*bolt.Tx) error) error {
		tx, err := repository.db.Begin(true)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if err := callback(tx); err != nil {
			return err
		}
		if err := tx.Rollback(); err != nil {
			return err
		}
		return commitFailure
	}
	_, err := service.SendMessage(queue.SendMessageInput{
		QueueName:   "orders",
		MessageBody: "body-secret",
		MessageAttributes: map[string]queue.MessageAttribute{
			"Sensitive": {DataType: "Binary", BinaryValue: []byte("attribute-secret")},
		},
	})
	if !errors.Is(err, commitFailure) || !errors.Is(err, queue.ErrRepositoryUnavailable) || strings.Contains(err.Error(), "body-secret") || strings.Contains(err.Error(), "attribute-secret") {
		t.Fatalf("SendMessage error = %v", err)
	}
	repository.updateFn = repository.db.Update
	if err := repository.db.View(func(tx *bolt.Tx) error {
		if tx.Bucket(messagesBucket).Get([]byte("message-attributes-failure")) != nil || tx.Bucket(messageIDsBucket).Get([]byte("message-attributes-failure")) != nil {
			t.Fatal("failed enqueue committed message or ID history")
		}
		ordered := tx.Bucket(messageOrderBucket).Bucket([]byte("orders"))
		if key, _ := ordered.Cursor().First(); key != nil {
			t.Fatal("failed enqueue committed order entry")
		}
		return nil
	}); err != nil {
		t.Fatalf("View: %v", err)
	}
}

func TestInjectedUpdateFailureRollsBackAllQueueAttributeChanges(t *testing.T) {
	repository, _ := openFailureTestRepository(t)
	defer repository.Close()
	service := queue.NewService(repository)
	if _, err := service.CreateQueue(queue.CreateQueueInput{QueueName: "orders"}); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	commitFailure := errors.New("injected queue attributes commit failure")
	repository.updateFn = func(callback func(*bolt.Tx) error) error {
		tx, err := repository.db.Begin(true)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if err := callback(tx); err != nil {
			return err
		}
		if err := tx.Rollback(); err != nil {
			return err
		}
		return commitFailure
	}
	err := service.SetQueueAttributes(queue.SetQueueAttributesInput{QueueName: "orders", Attributes: map[string]string{
		"VisibilityTimeout": "60", "DelaySeconds": "5", "MessageRetentionPeriod": "120",
	}})
	if !errors.Is(err, commitFailure) || !errors.Is(err, queue.ErrRepositoryUnavailable) {
		t.Fatalf("SetQueueAttributes error = %v", err)
	}
	repository.updateFn = repository.db.Update
	stored, found, err := repository.Get("orders")
	if err != nil || !found || stored.VisibilityTimeout != 30 || stored.DelaySeconds != 0 || stored.MessageRetentionPeriod != 345600 {
		t.Fatalf("failed Set partially committed: %#v, %v, %v", stored, found, err)
	}
}

func TestChangeVisibilityUpdatesOnlyMessageDeadline(t *testing.T) {
	repository, _ := openFailureTestRepository(t)
	defer repository.Close()
	service, testClock, receiptHandle := setupClaimedFailureMessage(t, repository)
	beforeMessage, beforeReceipt, beforeOrder, beforeIDHistory := storedChangeState(t, repository, "message-change", receiptHandle)

	testClock.now = testClock.now.Add(5 * time.Second)
	if err := service.ChangeMessageVisibility(queue.ChangeMessageVisibilityInput{
		QueueName:         "orders",
		ReceiptHandle:     receiptHandle,
		VisibilityTimeout: 20,
	}); err != nil {
		t.Fatalf("ChangeMessageVisibility: %v", err)
	}
	afterMessage, afterReceipt, afterOrder, afterIDHistory := storedChangeState(t, repository, "message-change", receiptHandle)
	wantMessage := beforeMessage
	wantMessage.VisibilityDeadlineUnixNanos = testClock.now.Add(20 * time.Second).UnixNano()
	if !reflect.DeepEqual(afterMessage, wantMessage) {
		t.Fatalf("stored message = %#v, want deadline-only update %#v", afterMessage, wantMessage)
	}
	if string(afterReceipt) != string(beforeReceipt) || string(afterOrder) != string(beforeOrder) || string(afterIDHistory) != string(beforeIDHistory) {
		t.Fatal("visibility change modified receipt, order, or message-ID history")
	}
}

func TestInjectedUpdateFailureRollsBackVisibilityDeadline(t *testing.T) {
	repository, _ := openFailureTestRepository(t)
	defer repository.Close()
	service, testClock, receiptHandle := setupClaimedFailureMessage(t, repository)
	beforeMessage, beforeReceipt, beforeOrder, beforeIDHistory := storedChangeState(t, repository, "message-change", receiptHandle)

	commitFailure := errors.New("injected visibility commit failure")
	repository.updateFn = func(callback func(*bolt.Tx) error) error {
		tx, err := repository.db.Begin(true)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if err := callback(tx); err != nil {
			return err
		}
		if err := tx.Rollback(); err != nil {
			return err
		}
		return commitFailure
	}
	testClock.now = testClock.now.Add(time.Second)
	err := service.ChangeMessageVisibility(queue.ChangeMessageVisibilityInput{
		QueueName:         "orders",
		ReceiptHandle:     receiptHandle,
		VisibilityTimeout: 120,
	})
	if !errors.Is(err, commitFailure) || !errors.Is(err, queue.ErrRepositoryUnavailable) {
		t.Fatalf("ChangeMessageVisibility error = %v, want commit and unavailable errors", err)
	}
	afterMessage, afterReceipt, afterOrder, afterIDHistory := storedChangeState(t, repository, "message-change", receiptHandle)
	if !reflect.DeepEqual(afterMessage, beforeMessage) || string(afterReceipt) != string(beforeReceipt) || string(afterOrder) != string(beforeOrder) || string(afterIDHistory) != string(beforeIDHistory) {
		t.Fatal("failed visibility update partially changed durable state")
	}
}

func TestInjectedUpdateFailureRollsBackRetentionCleanup(t *testing.T) {
	repository, _ := openFailureTestRepository(t)
	defer repository.Close()
	testClock := &failureClock{now: time.Date(2026, time.August, 23, 10, 11, 12, 123456789, time.UTC)}
	service := queue.NewService(
		repository,
		queue.WithClock(testClock),
		queue.WithMessageIDGenerator(func() string { return "message-expire" }),
	)
	if _, err := service.CreateQueue(queue.CreateQueueInput{
		QueueName:  "orders",
		Attributes: map[string]string{"MessageRetentionPeriod": "60"},
	}); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	if _, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "body"}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	testClock.now = testClock.now.Add(60 * time.Second)
	commitFailure := errors.New("injected expiration commit failure")
	repository.updateFn = func(callback func(*bolt.Tx) error) error {
		tx, err := repository.db.Begin(true)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if err := callback(tx); err != nil {
			return err
		}
		if err := tx.Rollback(); err != nil {
			return err
		}
		return commitFailure
	}
	if expired, err := service.ExpireMessages(); expired != 0 || !errors.Is(err, commitFailure) || !errors.Is(err, queue.ErrRepositoryUnavailable) {
		t.Fatalf("ExpireMessages = %d, %v", expired, err)
	}
	repository.updateFn = repository.db.Update
	if err := repository.db.View(func(tx *bolt.Tx) error {
		if tx.Bucket(messagesBucket).Get([]byte("message-expire")) == nil {
			t.Fatal("failed expiration cleanup committed a partial delete")
		}
		return nil
	}); err != nil {
		t.Fatalf("View: %v", err)
	}
	messages, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders"})
	if err != nil || len(messages) != 0 {
		t.Fatalf("expired message became deliverable after cleanup failure = %#v, %v", messages, err)
	}
}

func TestChangeVisibilityFailsClosedOnCorruptReceiptWithoutMutation(t *testing.T) {
	repository, _ := openFailureTestRepository(t)
	defer repository.Close()
	service, testClock, receiptHandle := setupClaimedFailureMessage(t, repository)
	beforeMessage, _, beforeOrder, beforeIDHistory := storedChangeState(t, repository, "message-change", receiptHandle)
	if err := repository.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(receiptsBucket).Put([]byte(receiptHandle), []byte(`{"version":1,"handle":"`+receiptHandle+`"}`))
	}); err != nil {
		t.Fatalf("corrupt receipt: %v", err)
	}

	testClock.now = testClock.now.Add(time.Second)
	err := service.ChangeMessageVisibility(queue.ChangeMessageVisibilityInput{
		QueueName:         "orders",
		ReceiptHandle:     receiptHandle,
		VisibilityTimeout: 120,
	})
	if !errors.Is(err, queue.ErrRepositoryCorrupt) || !errors.Is(err, queue.ErrRepositoryUnavailable) {
		t.Fatalf("ChangeMessageVisibility error = %v, want corruption/unavailable", err)
	}
	afterMessage, _, afterOrder, afterIDHistory := storedChangeState(t, repository, "message-change", receiptHandle)
	if !reflect.DeepEqual(afterMessage, beforeMessage) || string(afterOrder) != string(beforeOrder) || string(afterIDHistory) != string(beforeIDHistory) {
		t.Fatal("corrupt receipt failure changed message or indexes")
	}
}

func TestOpenRejectsUnknownSchemaVersion(t *testing.T) {
	path := mutateFailureTestDatabase(t, func(tx *bolt.Tx) error {
		version := make([]byte, 4)
		binary.BigEndian.PutUint32(version, schemaVersion+1)
		return tx.Bucket(metadataBucket).Put(schemaVersionKey, version)
	})
	_, err := Open(Config{Path: path, OpenTimeout: time.Second})
	if !errors.Is(err, queue.ErrRepositoryCorrupt) || !strings.Contains(err.Error(), "unsupported schema version") {
		t.Fatalf("Open error = %v, want unsupported corrupt schema", err)
	}
}

func TestNewDatabaseUsesSchemaV4AndDeterministicV3AttributeRecords(t *testing.T) {
	repository, _ := openFailureTestRepository(t)
	defer repository.Close()
	service := queue.NewService(repository, queue.WithMessageIDGenerator(func() string { return "message-schema-v3" }))
	if _, err := service.CreateQueue(queue.CreateQueueInput{QueueName: "orders"}); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	sent, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "body", MessageAttributes: map[string]queue.MessageAttribute{
		"Zed":   {DataType: "Binary.data", BinaryValue: []byte{1, 2}},
		"Alpha": {DataType: "String.event", StringValue: "created"},
	}})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if err := repository.db.View(func(tx *bolt.Tx) error {
		version, err := transactionSchemaVersion(tx)
		if err != nil {
			return err
		}
		if version != schemaVersion {
			t.Fatalf("schema version = %d, want %d", version, schemaVersion)
		}
		var record messageRecord
		if err := decodeRecord(tx.Bucket(messagesBucket).Get([]byte(sent.ID)), &record); err != nil {
			return err
		}
		if record.Version != recordVersion || len(record.MessageAttributes) != 2 || record.MessageAttributes[0].Name != "Alpha" || record.MessageAttributes[1].Name != "Zed" || record.MessageAttributes[0].Version != recordVersion || record.MD5OfMessageAttributes != sent.MD5OfMessageAttributes {
			t.Fatalf("message schema v3 record = %#v", record)
		}
		return nil
	}); err != nil {
		t.Fatalf("inspect schema v3: %v", err)
	}
}

func TestOpenRejectsCorruptMessageAttributeDigest(t *testing.T) {
	repository, path := openFailureTestRepository(t)
	service := queue.NewService(repository, queue.WithMessageIDGenerator(func() string { return "message-corrupt-attributes" }))
	if _, err := service.CreateQueue(queue.CreateQueueInput{QueueName: "orders"}); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	if _, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "body", MessageAttributes: map[string]queue.MessageAttribute{
		"Sensitive": {DataType: "String", StringValue: "attribute-secret"},
	}}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if err := repository.db.Update(func(tx *bolt.Tx) error {
		var record messageRecord
		if err := decodeRecord(tx.Bucket(messagesBucket).Get([]byte("message-corrupt-attributes")), &record); err != nil {
			return err
		}
		record.MessageAttributes[0].StringValue = "changed-secret"
		encoded, err := encodeRecord(record)
		if err != nil {
			return err
		}
		return tx.Bucket(messagesBucket).Put([]byte(record.ID), encoded)
	}); err != nil {
		t.Fatalf("corrupt attribute: %v", err)
	}
	if err := repository.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	_, err := Open(Config{Path: path, OpenTimeout: time.Second})
	if err == nil || !errors.Is(err, queue.ErrRepositoryCorrupt) || strings.Contains(err.Error(), "attribute-secret") || strings.Contains(err.Error(), "changed-secret") {
		t.Fatalf("Open corrupt attributes = %v", err)
	}
}

func TestOpenRejectsMalformedRecord(t *testing.T) {
	path := mutateFailureTestDatabase(t, func(tx *bolt.Tx) error {
		return tx.Bucket(queuesBucket).Put([]byte("orders"), []byte(`{"version":1,"name":"orders"}`))
	})
	_, err := Open(Config{Path: path, OpenTimeout: time.Second})
	if !errors.Is(err, queue.ErrRepositoryCorrupt) {
		t.Fatalf("Open error = %v, want ErrRepositoryCorrupt", err)
	}
}

func TestHealthFailsWhenARecordBecomesCorrupt(t *testing.T) {
	repository, _ := openFailureTestRepository(t)
	defer repository.Close()
	if _, err := repository.Create(queue.Queue{Name: "orders", VisibilityTimeout: 30}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := repository.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(queuesBucket).Put([]byte("orders"), []byte(`{"version":1,"name":"orders","attributes":null}`))
	}); err != nil {
		t.Fatalf("corrupt record: %v", err)
	}
	if err := repository.Health(); !errors.Is(err, queue.ErrRepositoryCorrupt) {
		t.Fatalf("Health error = %v, want ErrRepositoryCorrupt", err)
	}
}

func TestOpenRejectsMissingRequiredBucket(t *testing.T) {
	path := mutateFailureTestDatabase(t, func(tx *bolt.Tx) error {
		return tx.DeleteBucket(receiptsBucket)
	})
	_, err := Open(Config{Path: path, OpenTimeout: time.Second})
	if !errors.Is(err, queue.ErrRepositoryCorrupt) || !strings.Contains(err.Error(), "required bucket") {
		t.Fatalf("Open error = %v, want missing bucket corruption", err)
	}
}

func TestOpenRejectsExistingEmptyFile(t *testing.T) {
	directory := secureTempDirectory(t)
	path := filepath.Join(directory, "simq.db")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	_, err := Open(Config{Path: path, OpenTimeout: time.Second})
	if !errors.Is(err, queue.ErrRepositoryCorrupt) {
		t.Fatalf("Open error = %v, want empty-file corruption", err)
	}
}

func TestOpenRejectsInvalidDatabaseBytes(t *testing.T) {
	directory := secureTempDirectory(t)
	path := filepath.Join(directory, "simq.db")
	if err := os.WriteFile(path, []byte("not a bbolt database"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	_, err := Open(Config{Path: path, OpenTimeout: time.Second})
	if !errors.Is(err, queue.ErrRepositoryCorrupt) {
		t.Fatalf("Open error = %v, want physical corruption", err)
	}
}

func TestOpenRejectsLockedDatabaseAfterTimeout(t *testing.T) {
	repository, path := openFailureTestRepository(t)
	defer repository.Close()
	started := time.Now()
	_, err := Open(Config{Path: path, OpenTimeout: 20 * time.Millisecond})
	if err == nil || !errors.Is(err, queue.ErrRepositoryUnavailable) {
		t.Fatalf("second Open error = %v, want unavailable lock error", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("locked Open took %v, want bounded timeout", elapsed)
	}
}

func TestOpenRejectsInvalidDataPath(t *testing.T) {
	directory := secureTempDirectory(t)
	parentFile := filepath.Join(directory, "not-a-directory")
	if err := os.WriteFile(parentFile, []byte("x"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	_, err := Open(Config{Path: filepath.Join(parentFile, "simq.db"), OpenTimeout: time.Second})
	if err == nil || !strings.Contains(err.Error(), "not a directory") {
		t.Fatalf("Open error = %v, want invalid directory error", err)
	}
}

func TestOpenRejectsUnsafeDirectoryAndFileModes(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows reports ACLs rather than enforceable POSIX mode bits")
	}
	t.Run("directory", func(t *testing.T) {
		directory := t.TempDir()
		if err := os.Chmod(directory, 0o755); err != nil {
			t.Fatalf("Chmod: %v", err)
		}
		_, err := Open(Config{Path: filepath.Join(directory, "simq.db"), OpenTimeout: time.Second})
		if err == nil || !strings.Contains(err.Error(), "mode must be 0700") {
			t.Fatalf("Open error = %v, want directory mode error", err)
		}
	})

	t.Run("file", func(t *testing.T) {
		repository, path := openFailureTestRepository(t)
		if err := repository.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if err := os.Chmod(path, 0o644); err != nil {
			t.Fatalf("Chmod: %v", err)
		}
		_, err := Open(Config{Path: path, OpenTimeout: time.Second})
		if err == nil || !strings.Contains(err.Error(), "mode must be 0600") {
			t.Fatalf("Open error = %v, want file mode error", err)
		}
	})
}

func openFailureTestRepository(t *testing.T) (*Repository, string) {
	t.Helper()
	path := filepath.Join(secureTempDirectory(t), "simq.db")
	repository, err := Open(Config{Path: path, OpenTimeout: time.Second})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return repository, path
}

func mutateFailureTestDatabase(t *testing.T, mutate func(*bolt.Tx) error) string {
	t.Helper()
	repository, path := openFailureTestRepository(t)
	if err := repository.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("raw Open: %v", err)
	}
	if err := db.Update(mutate); err != nil {
		_ = db.Close()
		t.Fatalf("mutate: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("raw Close: %v", err)
	}
	return path
}

func secureTempDirectory(t *testing.T) string {
	t.Helper()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatalf("Chmod: %v", err)
	}
	return directory
}

func setupClaimedFailureMessage(t *testing.T, repository *Repository) (*queue.Service, *failureClock, string) {
	t.Helper()
	testClock := &failureClock{now: time.Date(2026, time.August, 23, 9, 10, 11, 123456789, time.UTC)}
	receiptHandle := fmt.Sprintf("rh_%032x", 1)
	service := queue.NewService(
		repository,
		queue.WithClock(testClock),
		queue.WithMessageIDGenerator(func() string { return "message-change" }),
		queue.WithReceiptHandleGenerator(func() string { return receiptHandle }),
	)
	if _, err := service.CreateQueue(queue.CreateQueueInput{QueueName: "orders"}); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	if _, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "body"}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	messages, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders"})
	if err != nil || len(messages) != 1 {
		t.Fatalf("ReceiveMessage = %#v, %v", messages, err)
	}
	return service, testClock, receiptHandle
}

func storedChangeState(t *testing.T, repository *Repository, messageID, receiptHandle string) (messageRecord, []byte, []byte, []byte) {
	t.Helper()
	var message messageRecord
	var receipt []byte
	var order []byte
	var idHistory []byte
	if err := repository.db.View(func(tx *bolt.Tx) error {
		encodedMessage := tx.Bucket(messagesBucket).Get([]byte(messageID))
		if encodedMessage == nil {
			return errors.New("message record is missing")
		}
		if err := decodeRecord(encodedMessage, &message); err != nil {
			return err
		}
		receipt = append([]byte(nil), tx.Bucket(receiptsBucket).Get([]byte(receiptHandle))...)
		idHistory = append([]byte(nil), tx.Bucket(messageIDsBucket).Get([]byte(messageID))...)
		ordered := tx.Bucket(messageOrderBucket).Bucket([]byte("orders"))
		if ordered == nil {
			return errors.New("order bucket is missing")
		}
		if key, value := ordered.Cursor().First(); key != nil {
			order = append(append([]byte(nil), key...), value...)
		}
		return nil
	}); err != nil {
		t.Fatalf("read stored change state: %v", err)
	}
	return message, receipt, order, idHistory
}
