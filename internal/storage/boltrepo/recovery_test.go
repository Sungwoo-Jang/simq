package boltrepo_test

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"simq/internal/queue"
	"simq/internal/storage/boltrepo"
)

type recoveryClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *recoveryClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *recoveryClock) Set(value time.Time) {
	c.mu.Lock()
	c.now = value
	c.mu.Unlock()
}

func TestCloseReopenRecoversM2MetadataAndDeletedGeneration(t *testing.T) {
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "simq.db")
	repository, err := boltrepo.Open(boltrepo.Config{Path: path, OpenTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	service := queue.NewService(repository)
	created, err := service.CreateQueue(queue.CreateQueueInput{QueueName: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	if err := service.TagQueue(queue.TagQueueInput{QueueName: created.Name, QueueID: created.ID, Tags: map[string]string{"env": "prod"}}); err != nil {
		t.Fatal(err)
	}
	if err := service.AddPermission(queue.AddPermissionInput{QueueName: created.Name, QueueID: created.ID, Label: "writers", AWSAccountIDs: []string{"111111111111"}, Actions: []string{"SendMessage"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.SendMessage(queue.SendMessageInput{QueueName: created.Name, QueueID: created.ID, MessageBody: "durable"}); err != nil {
		t.Fatal(err)
	}
	if err := repository.Close(); err != nil {
		t.Fatal(err)
	}

	repository, err = boltrepo.Open(boltrepo.Config{Path: path, OpenTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	service = queue.NewService(repository)
	stored, found, err := repository.GetByRef(queue.QueueRef{Name: created.Name, ID: created.ID})
	if err != nil || !found || stored.ID != created.ID {
		t.Fatalf("reopened queue = %#v, %v, %v", stored, found, err)
	}
	tags, err := service.ListQueueTags(queue.QueueRef{Name: created.Name, ID: created.ID})
	if err != nil || tags["env"] != "prod" {
		t.Fatalf("reopened tags = %#v, %v", tags, err)
	}
	attributes, err := service.GetQueueAttributes(queue.GetQueueAttributesInput{QueueName: created.Name, QueueID: created.ID, AttributeNames: []string{"Policy"}})
	if err != nil || attributes["Policy"] == "" {
		t.Fatalf("reopened policy = %#v, %v", attributes, err)
	}
	messages, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: created.Name, QueueID: created.ID})
	if err != nil || len(messages) != 1 || messages[0].Body != "durable" {
		t.Fatalf("reopened messages = %#v, %v", messages, err)
	}
	if err := service.DeleteQueue(queue.QueueRef{Name: created.Name, ID: created.ID}); err != nil {
		t.Fatal(err)
	}
	if err := repository.Close(); err != nil {
		t.Fatal(err)
	}

	repository, err = boltrepo.Open(boltrepo.Config{Path: path, OpenTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	service = queue.NewService(repository)
	if _, found, err := repository.GetByRef(queue.QueueRef{Name: created.Name, ID: created.ID}); err != nil || found {
		t.Fatalf("deleted generation after reopen = %v, %v", found, err)
	}
	recreated, err := service.CreateQueue(queue.CreateQueueInput{QueueName: created.Name})
	if err != nil || recreated.ID == created.ID || recreated.LegacyURLAllowed {
		t.Fatalf("recreated queue = %#v, %v", recreated, err)
	}
}

func TestCloseReopenRecoversSentMessageBeforeFirstReceive(t *testing.T) {
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatalf("Chmod: %v", err)
	}
	path := filepath.Join(directory, "simq.db")
	testClock := &recoveryClock{now: time.Date(2026, time.August, 23, 2, 3, 4, 987654321, time.UTC)}

	repository, err := boltrepo.Open(boltrepo.Config{Path: path, OpenTimeout: time.Second})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	var messageSequence atomic.Uint64
	service := queue.NewService(
		repository,
		queue.WithClock(testClock),
		queue.WithMessageIDGenerator(func() string { return fmt.Sprintf("message-before-receive-%d", messageSequence.Add(1)) }),
	)
	for _, name := range []string{"orders", "empty", "payments"} {
		if _, err := service.CreateQueue(queue.CreateQueueInput{QueueName: name}); err != nil {
			t.Fatalf("CreateQueue(%s): %v", name, err)
		}
	}
	for _, item := range []struct{ queueName, body string }{{"orders", "order-1"}, {"payments", "payment"}, {"orders", "order-2"}} {
		if _, err := service.SendMessage(queue.SendMessageInput{QueueName: item.queueName, MessageBody: item.body}); err != nil {
			t.Fatalf("SendMessage(%s): %v", item.queueName, err)
		}
	}
	if err := repository.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	repository, err = boltrepo.Open(boltrepo.Config{Path: path, OpenTimeout: time.Second})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = repository.Close() })
	var receiptSequence atomic.Uint64
	service = queue.NewService(
		repository,
		queue.WithClock(testClock),
		queue.WithReceiptHandleGenerator(func() string { return fmt.Sprintf("rh_%032x", receiptSequence.Add(1)) }),
	)
	maximum := 2
	orders, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders", MaxNumberOfMessages: &maximum})
	if err != nil || len(orders) != 2 || orders[0].Body != "order-1" || orders[1].Body != "order-2" {
		t.Fatalf("orders after reopen = %#v, %v", orders, err)
	}
	payments, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "payments"})
	if err != nil || len(payments) != 1 || payments[0].Body != "payment" {
		t.Fatalf("payments after reopen = %#v, %v", payments, err)
	}
	empty, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "empty"})
	if err != nil || len(empty) != 0 {
		t.Fatalf("empty queue after reopen = %#v, %v", empty, err)
	}
}

func TestCloseReopenPreservesDeliveryAndRetentionDeadlines(t *testing.T) {
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatalf("Chmod: %v", err)
	}
	path := filepath.Join(directory, "simq.db")
	base := time.Date(2026, time.August, 23, 3, 4, 5, 123456789, time.UTC)
	testClock := &recoveryClock{now: base}
	openService := func(t *testing.T) (*queue.Service, queue.Repository) {
		t.Helper()
		repository, err := boltrepo.Open(boltrepo.Config{Path: path, OpenTimeout: time.Second})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		return queue.NewService(
			repository,
			queue.WithClock(testClock),
			queue.WithMessageIDGenerator(func() string { return "message-lifecycle-recovery" }),
			queue.WithReceiptHandleGenerator(func() string { return "rh_00000000000000000000000000000001" }),
		), repository
	}

	service, repository := openService(t)
	created, err := service.CreateQueue(queue.CreateQueueInput{
		QueueName: "orders",
		Attributes: map[string]string{
			"DelaySeconds":           "10",
			"MessageRetentionPeriod": "60",
		},
	})
	if err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	sent, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "durable-lifecycle"})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if err := repository.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	testClock.Set(base.Add(10*time.Second - time.Nanosecond))
	service, repository = openService(t)
	recoveredQueue, err := service.GetQueue(queue.GetQueueInput{QueueName: "orders"})
	if err != nil || recoveredQueue != created {
		t.Fatalf("recovered queue = %#v, %v; want %#v", recoveredQueue, err, created)
	}
	if before, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders"}); err != nil || len(before) != 0 {
		t.Fatalf("receive before recovered availability = %#v, %v", before, err)
	}
	testClock.Set(base.Add(10 * time.Second))
	wait := 20
	atDeadline, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders", WaitTimeSeconds: &wait})
	if err != nil || len(atDeadline) != 1 {
		t.Fatalf("receive at recovered availability = %#v, %v", atDeadline, err)
	}
	if !atDeadline[0].AvailableAt.Equal(sent.AvailableAt) || !atDeadline[0].ExpiresAt.Equal(sent.ExpiresAt) {
		t.Fatalf("recovered deadlines = %#v, want %#v", atDeadline[0], sent)
	}
	if err := repository.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	testClock.Set(base.Add(60 * time.Second))
	service, repository = openService(t)
	t.Cleanup(func() { _ = repository.Close() })
	if expired, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders"}); err != nil || len(expired) != 0 {
		t.Fatalf("receive at recovered expiration = %#v, %v", expired, err)
	}
}

func TestCloseReopenRecoversQueueMessageClaimReceiptsAndDelete(t *testing.T) {
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatalf("Chmod: %v", err)
	}
	path := filepath.Join(directory, "simq.db")
	base := time.Date(2026, time.August, 23, 4, 5, 6, 123456789, time.UTC)
	testClock := &recoveryClock{now: base}
	var receiptSequence atomic.Uint64
	newService := func(t *testing.T) (*queue.Service, queue.Repository) {
		t.Helper()
		repository, err := boltrepo.Open(boltrepo.Config{Path: path, OpenTimeout: time.Second})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		service := queue.NewService(
			repository,
			queue.WithClock(testClock),
			queue.WithMessageIDGenerator(func() string { return "message-recovery" }),
			queue.WithReceiptHandleGenerator(func() string {
				return fmt.Sprintf("rh_%032x", receiptSequence.Add(1))
			}),
		)
		return service, repository
	}

	service, repository := newService(t)
	for _, name := range []string{"orders", "empty", "payments"} {
		if _, err := service.CreateQueue(queue.CreateQueueInput{
			QueueName:  name,
			Attributes: map[string]string{"VisibilityTimeout": "10"},
		}); err != nil {
			t.Fatalf("CreateQueue(%s): %v", name, err)
		}
	}
	if _, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "durable-body"}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	first, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders"})
	if err != nil || len(first) != 1 {
		t.Fatalf("first ReceiveMessage = %#v, %v", first, err)
	}
	firstClaim := first[0]
	deadline := firstClaim.VisibilityDeadline
	if err := repository.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	testClock.Set(deadline.Add(-time.Nanosecond))
	service, repository = newService(t)
	for _, name := range []string{"orders", "empty", "payments"} {
		got, err := service.GetQueue(queue.GetQueueInput{QueueName: name})
		if err != nil {
			t.Fatalf("GetQueue(%s) after reopen: %v", name, err)
		}
		if got.VisibilityTimeout != 10 {
			t.Fatalf("GetQueue(%s) visibility = %d, want 10", name, got.VisibilityTimeout)
		}
	}
	hidden, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders"})
	if err != nil || len(hidden) != 0 {
		t.Fatalf("receive before recovered deadline = %#v, %v", hidden, err)
	}
	empty, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "empty"})
	if err != nil || len(empty) != 0 {
		t.Fatalf("empty queue after reopen = %#v, %v", empty, err)
	}
	testClock.Set(deadline)
	second, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders"})
	if err != nil || len(second) != 1 {
		t.Fatalf("receive at recovered deadline = %#v, %v", second, err)
	}
	secondClaim := second[0]
	if secondClaim.ReceiveCount != 2 || secondClaim.ReceiveGeneration != 2 {
		t.Fatalf("recovered receive metadata = %#v", secondClaim)
	}
	if secondClaim.FirstReceivedAtMillis != firstClaim.FirstReceivedAtMillis {
		t.Fatalf("first timestamp changed: %d != %d", secondClaim.FirstReceivedAtMillis, firstClaim.FirstReceivedAtMillis)
	}
	if secondClaim.ReceiptHandle == firstClaim.ReceiptHandle {
		t.Fatal("receipt handle was reused after reopen")
	}
	if err := repository.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	service, repository = newService(t)
	if err := service.DeleteMessage(queue.DeleteMessageInput{QueueName: "orders", ReceiptHandle: firstClaim.ReceiptHandle}); err != nil {
		t.Fatalf("stale DeleteMessage after reopen: %v", err)
	}
	hidden, err = service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders"})
	if err != nil || len(hidden) != 0 {
		t.Fatalf("stale receipt deleted latest generation: %#v, %v", hidden, err)
	}
	if err := service.DeleteMessage(queue.DeleteMessageInput{QueueName: "orders", ReceiptHandle: secondClaim.ReceiptHandle}); err != nil {
		t.Fatalf("current DeleteMessage: %v", err)
	}
	if err := repository.Close(); err != nil {
		t.Fatalf("third Close: %v", err)
	}

	service, repository = newService(t)
	t.Cleanup(func() { _ = repository.Close() })
	if err := service.DeleteMessage(queue.DeleteMessageInput{QueueName: "orders", ReceiptHandle: secondClaim.ReceiptHandle}); err != nil {
		t.Fatalf("repeated DeleteMessage after reopen: %v", err)
	}
	final, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders"})
	if err != nil || len(final) != 0 {
		t.Fatalf("deleted message after reopen = %#v, %v", final, err)
	}
	payments, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "payments"})
	if err != nil || len(payments) != 0 {
		t.Fatalf("payments isolation after reopen = %#v, %v", payments, err)
	}
}

func TestCloseReopenRecoversChangedVisibilityAndPreservesClaimMetadata(t *testing.T) {
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatalf("Chmod: %v", err)
	}
	path := filepath.Join(directory, "simq.db")
	base := time.Date(2026, time.August, 23, 7, 8, 9, 987654321, time.UTC)
	testClock := &recoveryClock{now: base}
	var receiptSequence atomic.Uint64
	newService := func(t *testing.T) (*queue.Service, queue.Repository) {
		t.Helper()
		repository, err := boltrepo.Open(boltrepo.Config{Path: path, OpenTimeout: time.Second})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		service := queue.NewService(
			repository,
			queue.WithClock(testClock),
			queue.WithMessageIDGenerator(func() string { return "message-change-recovery" }),
			queue.WithReceiptHandleGenerator(func() string {
				return fmt.Sprintf("rh_%032x", receiptSequence.Add(1))
			}),
		)
		return service, repository
	}

	service, repository := newService(t)
	if _, err := service.CreateQueue(queue.CreateQueueInput{
		QueueName:  "orders",
		Attributes: map[string]string{"VisibilityTimeout": "10"},
	}); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	if _, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "durable-change-body"}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	first, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders"})
	if err != nil || len(first) != 1 {
		t.Fatalf("first ReceiveMessage = %#v, %v", first, err)
	}
	firstClaim := first[0]
	testClock.Set(base.Add(5 * time.Second))
	if err := service.ChangeMessageVisibility(queue.ChangeMessageVisibilityInput{
		QueueName:         "orders",
		ReceiptHandle:     firstClaim.ReceiptHandle,
		VisibilityTimeout: 20,
	}); err != nil {
		t.Fatalf("extend ChangeMessageVisibility: %v", err)
	}
	changedDeadline := base.Add(25 * time.Second)
	if err := repository.Close(); err != nil {
		t.Fatalf("Close after extension: %v", err)
	}

	testClock.Set(changedDeadline.Add(-time.Nanosecond))
	service, repository = newService(t)
	hidden, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders"})
	if err != nil || len(hidden) != 0 {
		t.Fatalf("receive before recovered changed deadline = %#v, %v", hidden, err)
	}
	testClock.Set(changedDeadline)
	second, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders"})
	if err != nil || len(second) != 1 {
		t.Fatalf("receive at recovered changed deadline = %#v, %v", second, err)
	}
	secondClaim := second[0]
	if secondClaim.ReceiveCount != 2 || secondClaim.ReceiveGeneration != 2 || secondClaim.FirstReceivedAtMillis != firstClaim.FirstReceivedAtMillis {
		t.Fatalf("metadata after recovered extension = %#v", secondClaim)
	}
	if secondClaim.ReceiptHandle == firstClaim.ReceiptHandle {
		t.Fatal("recovered receive reused receipt handle")
	}
	if err := service.ChangeMessageVisibility(queue.ChangeMessageVisibilityInput{
		QueueName:         "orders",
		ReceiptHandle:     firstClaim.ReceiptHandle,
		VisibilityTimeout: 100,
	}); err != nil {
		t.Fatalf("stale ChangeMessageVisibility after reopen: %v", err)
	}
	secondDeadline := changedDeadline.Add(10 * time.Second)
	if err := repository.Close(); err != nil {
		t.Fatalf("Close after stale change: %v", err)
	}

	testClock.Set(secondDeadline.Add(-time.Nanosecond))
	service, repository = newService(t)
	hidden, err = service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders"})
	if err != nil || len(hidden) != 0 {
		t.Fatalf("stale handle changed latest recovered deadline: %#v, %v", hidden, err)
	}
	testClock.Set(secondDeadline)
	third, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders"})
	if err != nil || len(third) != 1 {
		t.Fatalf("receive at latest deadline after stale no-op = %#v, %v", third, err)
	}
	thirdClaim := third[0]
	if err := service.ChangeMessageVisibility(queue.ChangeMessageVisibilityInput{
		QueueName:         "orders",
		ReceiptHandle:     thirdClaim.ReceiptHandle,
		VisibilityTimeout: 0,
	}); err != nil {
		t.Fatalf("zero ChangeMessageVisibility: %v", err)
	}
	if err := repository.Close(); err != nil {
		t.Fatalf("Close after zero change: %v", err)
	}

	service, repository = newService(t)
	fourth, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders"})
	if err != nil || len(fourth) != 1 {
		t.Fatalf("immediate receive after zero-change reopen = %#v, %v", fourth, err)
	}
	fourthClaim := fourth[0]
	if fourthClaim.ReceiveCount != 4 || fourthClaim.ReceiveGeneration != 4 || fourthClaim.FirstReceivedAtMillis != firstClaim.FirstReceivedAtMillis {
		t.Fatalf("metadata after zero-change reopen = %#v", fourthClaim)
	}
	if err := service.ChangeMessageVisibility(queue.ChangeMessageVisibilityInput{
		QueueName:         "orders",
		ReceiptHandle:     fourthClaim.ReceiptHandle,
		VisibilityTimeout: 60,
	}); err != nil {
		t.Fatalf("final ChangeMessageVisibility: %v", err)
	}
	if err := service.DeleteMessage(queue.DeleteMessageInput{QueueName: "orders", ReceiptHandle: fourthClaim.ReceiptHandle}); err != nil {
		t.Fatalf("DeleteMessage after visibility change: %v", err)
	}
	if err := repository.Close(); err != nil {
		t.Fatalf("Close after delete: %v", err)
	}

	testClock.Set(secondDeadline.Add(24 * time.Hour))
	service, repository = newService(t)
	t.Cleanup(func() { _ = repository.Close() })
	if err := service.ChangeMessageVisibility(queue.ChangeMessageVisibilityInput{
		QueueName:         "orders",
		ReceiptHandle:     fourthClaim.ReceiptHandle,
		VisibilityTimeout: 60,
	}); err != nil {
		t.Fatalf("consumed ChangeMessageVisibility after reopen: %v", err)
	}
	final, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders"})
	if err != nil || len(final) != 0 {
		t.Fatalf("deleted message resurrected after reopen = %#v, %v", final, err)
	}
}

func TestCloseReopenPreservesMessageAttributesAndChangedQueueSettings(t *testing.T) {
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatalf("Chmod: %v", err)
	}
	path := filepath.Join(directory, "simq.db")
	base := time.Date(2026, time.August, 23, 13, 14, 15, 123456789, time.UTC)
	testClock := &recoveryClock{now: base}
	var receiptSequence atomic.Uint64
	openService := func(t *testing.T) (*queue.Service, queue.Repository) {
		t.Helper()
		repository, err := boltrepo.Open(boltrepo.Config{Path: path, OpenTimeout: time.Second})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		return queue.NewService(
			repository,
			queue.WithClock(testClock),
			queue.WithMessageIDGenerator(func() string { return "message-attributes-recovery" }),
			queue.WithReceiptHandleGenerator(func() string { return fmt.Sprintf("rh_%032x", receiptSequence.Add(1)) }),
		), repository
	}

	service, repository := openService(t)
	if _, err := service.CreateQueue(queue.CreateQueueInput{QueueName: "orders", Attributes: map[string]string{"VisibilityTimeout": "0", "MessageRetentionPeriod": "60"}}); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	sent, err := service.SendMessage(queue.SendMessageInput{
		QueueName:   "orders",
		MessageBody: "body",
		MessageAttributes: map[string]queue.MessageAttribute{
			"Kind":    {DataType: "String.event", StringValue: "created"},
			"Payload": {DataType: "Binary.data", BinaryValue: []byte{1, 2, 3}},
		},
	})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if err := service.SetQueueAttributes(queue.SetQueueAttributesInput{QueueName: "orders", Attributes: map[string]string{
		"VisibilityTimeout": "5", "DelaySeconds": "10", "MessageRetentionPeriod": "120",
	}}); err != nil {
		t.Fatalf("SetQueueAttributes: %v", err)
	}
	if err := repository.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	service, repository = openService(t)
	settings, err := service.GetQueueAttributes(queue.GetQueueAttributesInput{QueueName: "orders", AttributeNames: []string{"All"}})
	if err != nil || settings["VisibilityTimeout"] != "5" || settings["DelaySeconds"] != "10" || settings["MessageRetentionPeriod"] != "120" {
		t.Fatalf("recovered settings = %#v, %v", settings, err)
	}
	zero := 0
	first, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders", VisibilityTimeout: &zero, MessageAttributeNames: []string{"All"}})
	if err != nil || len(first) != 1 || first[0].MD5OfMessageAttributes != sent.MD5OfMessageAttributes || first[0].MessageAttributes["Payload"].BinaryValue[0] != 1 || !first[0].AvailableAt.Equal(sent.AvailableAt) || !first[0].ExpiresAt.Equal(sent.ExpiresAt) {
		t.Fatalf("recovered message attributes/lifecycle = %#v, %v", first, err)
	}
	first[0].MessageAttributes["Payload"].BinaryValue[0] = 9
	if err := repository.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	service, repository = openService(t)
	t.Cleanup(func() { _ = repository.Close() })
	second, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders", VisibilityTimeout: &zero, MessageAttributeNames: []string{"All"}})
	if err != nil || len(second) != 1 || second[0].MessageAttributes["Payload"].BinaryValue[0] != 1 || second[0].MD5OfMessageAttributes != sent.MD5OfMessageAttributes || !second[0].ExpiresAt.Equal(sent.ExpiresAt) {
		t.Fatalf("second recovered message = %#v, %v", second, err)
	}
}
