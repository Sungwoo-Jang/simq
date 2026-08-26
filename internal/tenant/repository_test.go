package tenant

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"simq/internal/queue"
	"simq/internal/storage/boltrepo"
)

func fixedQueueID(value int) func() string {
	return func() string { return fmt.Sprintf("q_%032x", value) }
}

func TestLegacyTenantCannotAddressOrListNamespacedQueues(t *testing.T) {
	underlying := queue.NewMemoryRepository()
	base := New(underlying, nil, "legacy")
	legacy := base.ForTenant("legacy").(*Repository)
	other := base.ForTenant("other").(*Repository)
	if _, err := legacy.Create(queue.Queue{ID: "q_00000000000000000000000000000001", Name: "legacy-orders", VisibilityTimeout: 30, MessageRetentionPeriod: queue.DefaultMessageRetentionPeriod}); err != nil {
		t.Fatal(err)
	}
	if _, err := other.Create(queue.Queue{ID: "q_00000000000000000000000000000002", Name: "private-orders", VisibilityTimeout: 30, MessageRetentionPeriod: queue.DefaultMessageRetentionPeriod}); err != nil {
		t.Fatal(err)
	}
	page, err := legacy.ListQueues(queue.ListQueuesCommand{Limit: 10})
	if err != nil || len(page.Queues) != 1 || page.Queues[0].Name != "legacy-orders" {
		t.Fatalf("legacy queues=%+v err=%v", page, err)
	}
	if _, _, err := legacy.Get(other.name("private-orders")); err == nil {
		t.Fatal("legacy tenant addressed a reserved tenant namespace")
	} else {
		var invalid *queue.InvalidRequestError
		if !errors.As(err, &invalid) {
			t.Fatalf("reserved-name error=%v", err)
		}
	}
}

func TestMoveTaskHandlesAreTenantScoped(t *testing.T) {
	underlying := queue.NewMemoryRepository()
	base := New(underlying, nil)
	for index, tenantID := range []string{"tenant-a", "tenant-b"} {
		bound := base.ForTenant(tenantID).(*Repository)
		sourceID := fmt.Sprintf("q_%032x", index*2+1)
		dlqID := fmt.Sprintf("q_%032x", index*2+2)
		if _, err := bound.Create(queue.Queue{ID: sourceID, Name: "source", VisibilityTimeout: 30, MessageRetentionPeriod: queue.DefaultMessageRetentionPeriod}); err != nil {
			t.Fatal(err)
		}
		if _, err := bound.Create(queue.Queue{ID: dlqID, Name: "dlq", VisibilityTimeout: 30, MessageRetentionPeriod: queue.DefaultMessageRetentionPeriod}); err != nil {
			t.Fatal(err)
		}
		policy := &queue.RedrivePolicy{DeadLetterTarget: queue.QueueRef{Name: "dlq", ID: dlqID}, MaxReceiveCount: 3}
		if _, err := bound.SetAttributes(queue.QueueAttributesCommand{QueueName: "source", QueueID: sourceID, RedrivePolicySet: true, RedrivePolicy: policy}); err != nil {
			t.Fatal(err)
		}
		task, err := bound.StartMoveTask(queue.StartMoveTaskCommand{Handle: "mt_same", Source: queue.QueueRef{Name: "dlq", ID: dlqID}, MaxMessagesPerSecond: 1, Now: time.Now()})
		if err != nil || task.Handle != "mt_same" {
			t.Fatalf("tenant %s task=%+v err=%v", tenantID, task, err)
		}
	}
}
func fixedMessageID(prefix int) func() string {
	next := 0
	return func() string { next++; return fmt.Sprintf("m_%016x%016x", prefix, next) }
}
func fixedReceipt() func() string {
	next := 0
	return func() string { next++; return fmt.Sprintf("rh_%032x", next) }
}

func TestTenantNamespacesIsolateSamePublicQueueName(t *testing.T) {
	underlying := queue.NewMemoryRepository()
	base := New(underlying, nil)
	tenantA := queue.NewService(base.ForTenant("tenant-a"), queue.WithQueueIDGenerator(fixedQueueID(1)), queue.WithMessageIDGenerator(fixedMessageID(1)), queue.WithReceiptHandleGenerator(fixedReceipt()))
	tenantB := queue.NewService(base.ForTenant("tenant-b"), queue.WithQueueIDGenerator(fixedQueueID(2)), queue.WithMessageIDGenerator(fixedMessageID(2)), queue.WithReceiptHandleGenerator(fixedReceipt()))
	queueA, err := tenantA.CreateQueue(queue.CreateQueueInput{QueueName: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	defer underlying.Close()
	queueB, err := tenantB.CreateQueue(queue.CreateQueueInput{QueueName: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tenantA.SendMessage(queue.SendMessageInput{QueueName: "orders", QueueID: queueA.ID, MessageBody: "alpha"}); err != nil {
		t.Fatal(err)
	}
	if _, err := tenantB.SendMessage(queue.SendMessageInput{QueueName: "orders", QueueID: queueB.ID, MessageBody: "beta"}); err != nil {
		t.Fatal(err)
	}
	one := 1
	messages, err := tenantA.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders", QueueID: queueA.ID, MaxNumberOfMessages: &one})
	if err != nil || len(messages) != 1 || messages[0].Body != "alpha" {
		t.Fatalf("tenant A messages=%+v err=%v", messages, err)
	}
	if err := tenantA.DeleteMessage(queue.DeleteMessageInput{QueueName: "orders", QueueID: queueA.ID, ReceiptHandle: messages[0].ReceiptHandle}); err != nil {
		t.Fatalf("tenant A delete: %v", err)
	}
	messages, err = tenantB.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders", QueueID: queueB.ID, MaxNumberOfMessages: &one})
	if err != nil || len(messages) != 1 || messages[0].Body != "beta" {
		t.Fatalf("tenant B messages=%+v err=%v", messages, err)
	}
}

func TestPayloadIsEncryptedAtRepositoryBoundaryAndOldKeyRemainsReadable(t *testing.T) {
	underlying := queue.NewMemoryRepository()
	key1 := make([]byte, 32)
	for i := range key1 {
		key1[i] = 1
	}
	cipher1, err := NewPayloadCipher("k1", map[string][]byte{"k1": key1})
	if err != nil {
		t.Fatal(err)
	}
	bound := New(underlying, cipher1).ForTenant("tenant-a").(*Repository)
	created, err := bound.Create(queue.Queue{ID: "q_00000000000000000000000000000001", Name: "orders", VisibilityTimeout: 30, MessageRetentionPeriod: queue.DefaultMessageRetentionPeriod})
	if err != nil {
		t.Fatal(err)
	}
	message := queue.Message{ID: "m_00000000000000000000000000000001", QueueName: "orders", Body: "secret-body", MessageAttributes: map[string]queue.MessageAttribute{"secret": {DataType: "String", StringValue: "attribute-secret"}}}
	if _, err := bound.Enqueue(queue.EnqueueCommand{QueueName: "orders", QueueID: created.ID, Now: time.Now(), Message: message}); err != nil {
		t.Fatal(err)
	}
	internal, err := underlying.Claim(queue.ClaimInput{QueueName: bound.name("orders"), QueueID: created.ID, Now: time.Now(), MaxNumberOfMessages: 1, ReceiptHandles: []string{"rh_00000000000000000000000000000001"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(internal.Messages) != 1 || !IsCiphertext(internal.Messages[0].Body) || internal.Messages[0].MessageAttributes != nil {
		t.Fatalf("stored message=%+v", internal.Messages)
	}
	key2 := make([]byte, 32)
	for i := range key2 {
		key2[i] = 2
	}
	cipher2, err := NewPayloadCipher("k2", map[string][]byte{"k1": key1, "k2": key2})
	if err != nil {
		t.Fatal(err)
	}
	rotated := New(underlying, cipher2).ForTenant("tenant-a").(*Repository)
	decoded, err := rotated.decrypt(internal.Messages[0])
	if err != nil || decoded.Body != "secret-body" || decoded.MessageAttributes["secret"].StringValue != "attribute-secret" {
		t.Fatalf("decoded=%+v err=%v", decoded, err)
	}
}

func TestEncryptedPayloadRoundTripsThroughBboltWithoutPlaintextAtRest(t *testing.T) {
	path := filepath.Join(t.TempDir(), "secure.db")
	underlying, err := boltrepo.Open(boltrepo.Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	defer underlying.Close()
	key := make([]byte, 32)
	for index := range key {
		key[index] = 7
	}
	payloadCipher, err := NewPayloadCipher("k1", map[string][]byte{"k1": key})
	if err != nil {
		t.Fatal(err)
	}
	service := queue.NewService(New(underlying, payloadCipher).ForTenant("tenant-a"), queue.WithQueueIDGenerator(fixedQueueID(1)), queue.WithMessageIDGenerator(fixedMessageID(1)), queue.WithReceiptHandleGenerator(fixedReceipt()))
	created, err := service.CreateQueue(queue.CreateQueueInput{QueueName: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	sent, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", QueueID: created.ID, MessageBody: "secret-body", MessageAttributes: map[string]queue.MessageAttribute{"secret": {DataType: "String", StringValue: "secret-attribute"}}})
	if err != nil || hasNamespacePrefix(sent.ID) {
		t.Fatalf("sent=%+v err=%v", sent, err)
	}
	one := 1
	messages, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders", QueueID: created.ID, MaxNumberOfMessages: &one, MessageAttributeNames: []string{"All"}})
	if err != nil || len(messages) != 1 || messages[0].Body != "secret-body" || messages[0].MessageAttributes["secret"].StringValue != "secret-attribute" || hasNamespacePrefix(messages[0].ID) || hasNamespacePrefix(messages[0].ReceiptHandle) {
		t.Fatalf("messages=%+v err=%v", messages, err)
	}
	if err := underlying.Health(); err != nil {
		t.Fatalf("health after encrypted round trip: %v", err)
	}
	if err := underlying.Close(); err != nil {
		t.Fatal(err)
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "secret-body") || strings.Contains(string(encoded), "secret-attribute") || !strings.Contains(string(encoded), "simqenc1:k1:") {
		t.Fatal("bbolt file did not preserve ciphertext-only payload storage")
	}
}

func TestEncryptedFIFODuplicatePreservesPublicAcknowledgement(t *testing.T) {
	underlying, err := boltrepo.Open(boltrepo.Config{Path: filepath.Join(t.TempDir(), "fifo.db")})
	if err != nil {
		t.Fatal(err)
	}
	defer underlying.Close()
	key := make([]byte, 32)
	payloadCipher, err := NewPayloadCipher("k1", map[string][]byte{"k1": key})
	if err != nil {
		t.Fatal(err)
	}
	service := queue.NewService(New(underlying, payloadCipher).ForTenant("tenant-a"), queue.WithQueueIDGenerator(fixedQueueID(1)), queue.WithMessageIDGenerator(fixedMessageID(1)))
	created, err := service.CreateQueue(queue.CreateQueueInput{QueueName: "orders.fifo", Attributes: map[string]string{"FifoQueue": "true"}})
	if err != nil {
		t.Fatal(err)
	}
	input := queue.SendMessageInput{QueueName: created.Name, QueueID: created.ID, MessageBody: "secret-body", MessageGroupID: "group", MessageDeduplicationID: "dedup"}
	first, err := service.SendMessage(input)
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.SendMessage(input)
	if err != nil {
		t.Fatal(err)
	}
	if second.ID != first.ID || second.MD5OfBody != first.MD5OfBody || second.SequenceNumber != first.SequenceNumber || second.Body != "" {
		t.Fatalf("first=%+v second=%+v", first, second)
	}
}
