package queue_test

import (
	"errors"
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

type repositoryFactory func(*testing.T) queue.Repository

func TestRepositoryConformance(t *testing.T) {
	backends := map[string]repositoryFactory{
		"memory": func(t *testing.T) queue.Repository {
			return queue.NewMemoryRepository()
		},
		"bbolt": func(t *testing.T) queue.Repository {
			directory := t.TempDir()
			if err := os.Chmod(directory, 0o700); err != nil {
				t.Fatalf("Chmod: %v", err)
			}
			repository, err := boltrepo.Open(boltrepo.Config{
				Path:        filepath.Join(directory, "simq.db"),
				OpenTimeout: time.Second,
			})
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			return repository
		},
	}

	for name, factory := range backends {
		t.Run(name, func(t *testing.T) {
			runRepositoryConformance(t, factory)
		})
	}
}

func runRepositoryConformance(t *testing.T, factory repositoryFactory) {
	t.Run("queue create and get", func(t *testing.T) {
		service, repository, _ := newConformanceService(t, factory)
		created, err := service.CreateQueue(queue.CreateQueueInput{QueueName: "orders"})
		if err != nil {
			t.Fatalf("CreateQueue: %v", err)
		}
		got, err := service.GetQueue(queue.GetQueueInput{QueueName: "orders"})
		if err != nil || got != created {
			t.Fatalf("GetQueue = %#v, %v; want %#v", got, err, created)
		}
		if err := repository.Health(); err != nil {
			t.Fatalf("Health: %v", err)
		}
	})

	t.Run("enqueue and empty receive", func(t *testing.T) {
		service, _, _ := newConformanceService(t, factory)
		mustCreateConformanceQueue(t, service, "orders", "30")
		empty, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders"})
		if err != nil || len(empty) != 0 {
			t.Fatalf("empty ReceiveMessage = %#v, %v", empty, err)
		}
		sent, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "body"})
		if err != nil || sent.Body != "body" {
			t.Fatalf("SendMessage = %#v, %v", sent, err)
		}
	})

	t.Run("message attributes and projection are immutable", func(t *testing.T) {
		service, _, _ := newConformanceService(t, factory)
		mustCreateConformanceQueue(t, service, "orders", "0")
		binaryValue := []byte{1, 2, 3}
		attributes := map[string]queue.MessageAttribute{
			"Kind":    {DataType: "String.event", StringValue: "created"},
			"Attempt": {DataType: "Number", StringValue: "1"},
			"Payload": {DataType: "Binary.data", BinaryValue: binaryValue},
		}
		sent, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "body", MessageAttributes: attributes})
		if err != nil || sent.MD5OfMessageAttributes == "" || len(sent.MessageAttributes) != 3 {
			t.Fatalf("SendMessage attributes = %#v, %v", sent, err)
		}
		binaryValue[0] = 9
		attributes["Kind"] = queue.MessageAttribute{DataType: "String", StringValue: "mutated"}
		first, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders", MessageAttributeNames: []string{"Kind", "Payload"}})
		if err != nil || len(first) != 1 || len(first[0].MessageAttributes) != 2 || first[0].MessageAttributes["Kind"].StringValue != "created" || first[0].MessageAttributes["Payload"].BinaryValue[0] != 1 || first[0].MD5OfMessageAttributes == "" {
			t.Fatalf("selected attributes = %#v, %v", first, err)
		}
		first[0].MessageAttributes["Payload"].BinaryValue[0] = 8
		all, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders", MessageAttributeNames: []string{"All"}})
		if err != nil || len(all) != 1 || len(all[0].MessageAttributes) != 3 || all[0].MessageAttributes["Payload"].BinaryValue[0] != 1 || all[0].MD5OfMessageAttributes != sent.MD5OfMessageAttributes {
			t.Fatalf("all attributes = %#v, %v", all, err)
		}
		if err := service.ChangeMessageVisibility(queue.ChangeMessageVisibilityInput{QueueName: "orders", ReceiptHandle: all[0].ReceiptHandle, VisibilityTimeout: 0}); err != nil {
			t.Fatalf("ChangeMessageVisibility: %v", err)
		}
		afterChange, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders", MessageAttributeNames: []string{"All"}})
		if err != nil || len(afterChange) != 1 || afterChange[0].MD5OfMessageAttributes != sent.MD5OfMessageAttributes || afterChange[0].MessageAttributes["Kind"].StringValue != "created" {
			t.Fatalf("attributes after visibility change = %#v, %v", afterChange, err)
		}
		if err := service.DeleteMessage(queue.DeleteMessageInput{QueueName: "orders", ReceiptHandle: afterChange[0].ReceiptHandle}); err != nil {
			t.Fatalf("DeleteMessage: %v", err)
		}
		if final, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders", MessageAttributeNames: []string{"All"}}); err != nil || len(final) != 0 {
			t.Fatalf("ReceiveMessage after attribute-bearing delete = %#v, %v", final, err)
		}
	})

	t.Run("retention preserves attributes until terminal expiration", func(t *testing.T) {
		service, _, testClock := newConformanceService(t, factory)
		if _, err := service.CreateQueue(queue.CreateQueueInput{QueueName: "orders", Attributes: map[string]string{
			"VisibilityTimeout": "0", "MessageRetentionPeriod": "60",
		}}); err != nil {
			t.Fatalf("CreateQueue: %v", err)
		}
		sent, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "body", MessageAttributes: map[string]queue.MessageAttribute{
			"Kind": {DataType: "String", StringValue: "retained"},
		}})
		if err != nil {
			t.Fatalf("SendMessage: %v", err)
		}
		testClock.Advance(60*time.Second - time.Nanosecond)
		before, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders", MessageAttributeNames: []string{"All"}})
		if err != nil || len(before) != 1 || before[0].MD5OfMessageAttributes != sent.MD5OfMessageAttributes || before[0].MessageAttributes["Kind"].StringValue != "retained" {
			t.Fatalf("message before expiration = %#v, %v", before, err)
		}
		testClock.Advance(time.Nanosecond)
		if expired, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders", MessageAttributeNames: []string{"All"}}); err != nil || len(expired) != 0 {
			t.Fatalf("message at expiration = %#v, %v", expired, err)
		}
	})

	t.Run("get and atomic partial set queue attributes", func(t *testing.T) {
		service, _, _ := newConformanceService(t, factory)
		if _, err := service.CreateQueue(queue.CreateQueueInput{QueueName: "orders"}); err != nil {
			t.Fatalf("CreateQueue: %v", err)
		}
		if err := service.SetQueueAttributes(queue.SetQueueAttributesInput{QueueName: "orders", Attributes: map[string]string{
			"VisibilityTimeout": "60", "DelaySeconds": "5",
		}}); err != nil {
			t.Fatalf("SetQueueAttributes: %v", err)
		}
		values, err := service.GetQueueAttributes(queue.GetQueueAttributesInput{QueueName: "orders", AttributeNames: []string{"All"}})
		if err != nil || values["VisibilityTimeout"] != "60" || values["DelaySeconds"] != "5" || values["MessageRetentionPeriod"] != "345600" {
			t.Fatalf("GetQueueAttributes = %#v, %v", values, err)
		}
		if err := service.SetQueueAttributes(queue.SetQueueAttributesInput{QueueName: "orders", Attributes: map[string]string{
			"VisibilityTimeout": "1", "DelaySeconds": "invalid",
		}}); err == nil {
			t.Fatal("invalid multi-value SetQueueAttributes succeeded")
		}
		values, err = service.GetQueueAttributes(queue.GetQueueAttributesInput{QueueName: "orders", AttributeNames: []string{"All"}})
		if err != nil || values["VisibilityTimeout"] != "60" || values["DelaySeconds"] != "5" {
			t.Fatalf("invalid update partially changed settings: %#v, %v", values, err)
		}
	})

	t.Run("set racing with send uses a complete configuration snapshot", func(t *testing.T) {
		service, _, testClock := newConformanceService(t, factory)
		const iterations = 20
		for iteration := 0; iteration < iterations; iteration++ {
			queueName := fmt.Sprintf("send-race-%d", iteration)
			if _, err := service.CreateQueue(queue.CreateQueueInput{QueueName: queueName, Attributes: map[string]string{
				"DelaySeconds": "1", "MessageRetentionPeriod": "60",
			}}); err != nil {
				t.Fatalf("iteration %d CreateQueue: %v", iteration, err)
			}
			start := make(chan struct{})
			type sendResult struct {
				message queue.Message
				err     error
			}
			sent := make(chan sendResult, 1)
			set := make(chan error, 1)
			go func() {
				<-start
				message, err := service.SendMessage(queue.SendMessageInput{QueueName: queueName, MessageBody: "body"})
				sent <- sendResult{message: message, err: err}
			}()
			go func() {
				<-start
				set <- service.SetQueueAttributes(queue.SetQueueAttributesInput{QueueName: queueName, Attributes: map[string]string{
					"DelaySeconds": "2", "MessageRetentionPeriod": "120",
				}})
			}()
			close(start)
			result := <-sent
			if result.err != nil || <-set != nil {
				t.Fatalf("iteration %d race errors: send=%v", iteration, result.err)
			}
			delay := result.message.AvailableAt.Sub(testClock.Now())
			retention := result.message.ExpiresAt.Sub(testClock.Now())
			if delay == time.Second && retention == 60*time.Second || delay == 2*time.Second && retention == 120*time.Second {
				continue
			}
			t.Fatalf("iteration %d torn send configuration: delay=%v retention=%v", iteration, delay, retention)
		}
	})

	t.Run("set racing with receive uses one visibility snapshot", func(t *testing.T) {
		service, _, testClock := newConformanceService(t, factory)
		const iterations = 20
		for iteration := 0; iteration < iterations; iteration++ {
			queueName := fmt.Sprintf("receive-race-%d", iteration)
			if _, err := service.CreateQueue(queue.CreateQueueInput{QueueName: queueName, Attributes: map[string]string{"VisibilityTimeout": "1"}}); err != nil {
				t.Fatalf("iteration %d CreateQueue: %v", iteration, err)
			}
			if _, err := service.SendMessage(queue.SendMessageInput{QueueName: queueName, MessageBody: "body"}); err != nil {
				t.Fatalf("iteration %d SendMessage: %v", iteration, err)
			}
			start := make(chan struct{})
			type receiveResult struct {
				messages []queue.Message
				err      error
			}
			received := make(chan receiveResult, 1)
			set := make(chan error, 1)
			go func() {
				<-start
				messages, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: queueName})
				received <- receiveResult{messages: messages, err: err}
			}()
			go func() {
				<-start
				set <- service.SetQueueAttributes(queue.SetQueueAttributesInput{QueueName: queueName, Attributes: map[string]string{"VisibilityTimeout": "2"}})
			}()
			close(start)
			result := <-received
			if result.err != nil || <-set != nil || len(result.messages) != 1 {
				t.Fatalf("iteration %d race result = %#v, %v", iteration, result.messages, result.err)
			}
			visibility := result.messages[0].VisibilityDeadline.Sub(testClock.Now())
			if visibility != time.Second && visibility != 2*time.Second {
				t.Fatalf("iteration %d torn/default visibility = %v", iteration, visibility)
			}
			if err := service.DeleteMessage(queue.DeleteMessageInput{QueueName: queueName, ReceiptHandle: result.messages[0].ReceiptHandle}); err != nil {
				t.Fatalf("iteration %d DeleteMessage: %v", iteration, err)
			}
		}
	})

	t.Run("atomic and batch claim", func(t *testing.T) {
		service, _, _ := newConformanceService(t, factory)
		mustCreateConformanceQueue(t, service, "orders", "30")
		for index := 0; index < 3; index++ {
			if _, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: fmt.Sprintf("body-%d", index)}); err != nil {
				t.Fatalf("SendMessage: %v", err)
			}
		}
		maximum := 2
		claimed, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders", MaxNumberOfMessages: &maximum})
		if err != nil || len(claimed) != 2 {
			t.Fatalf("batch claim = %#v, %v", claimed, err)
		}
		for _, message := range claimed {
			if message.ReceiveCount != 1 || message.ReceiveGeneration != 1 || message.ReceiptHandle == "" {
				t.Fatalf("claim metadata = %#v", message)
			}
		}
		next, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders", MaxNumberOfMessages: &maximum})
		if err != nil || len(next) != 1 {
			t.Fatalf("next claim = %#v, %v", next, err)
		}
	})

	t.Run("queue isolation", func(t *testing.T) {
		service, _, _ := newConformanceService(t, factory)
		mustCreateConformanceQueue(t, service, "orders", "30")
		mustCreateConformanceQueue(t, service, "payments", "30")
		for _, item := range []struct{ queueName, body string }{{"orders", "order"}, {"payments", "payment"}} {
			if _, err := service.SendMessage(queue.SendMessageInput{QueueName: item.queueName, MessageBody: item.body}); err != nil {
				t.Fatalf("SendMessage(%s): %v", item.queueName, err)
			}
		}
		orders, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders"})
		if err != nil || len(orders) != 1 || orders[0].Body != "order" {
			t.Fatalf("orders claim = %#v, %v", orders, err)
		}
		payments, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "payments"})
		if err != nil || len(payments) != 1 || payments[0].Body != "payment" {
			t.Fatalf("payments claim = %#v, %v", payments, err)
		}
	})

	t.Run("delivery delay exact boundary and standard queue bypass", func(t *testing.T) {
		service, _, testClock := newConformanceService(t, factory)
		if _, err := service.CreateQueue(queue.CreateQueueInput{
			QueueName: "orders",
			Attributes: map[string]string{
				"DelaySeconds":           "10",
				"MessageRetentionPeriod": "60",
			},
		}); err != nil {
			t.Fatalf("CreateQueue: %v", err)
		}
		delayed, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "delayed"})
		if err != nil {
			t.Fatalf("delayed SendMessage: %v", err)
		}
		zero := 0
		immediate, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "immediate", DelaySeconds: &zero})
		if err != nil {
			t.Fatalf("immediate SendMessage: %v", err)
		}
		if !delayed.AvailableAt.Equal(testClock.Now().Add(10*time.Second)) || !delayed.ExpiresAt.Equal(testClock.Now().Add(60*time.Second)) {
			t.Fatalf("delayed lifecycle = %#v", delayed)
		}
		first := mustReceiveOne(t, service, "orders")
		if first.ID != immediate.ID {
			t.Fatalf("first message = %q, want later eligible %q", first.ID, immediate.ID)
		}
		testClock.Advance(10*time.Second - time.Nanosecond)
		if before, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders"}); err != nil || len(before) != 0 {
			t.Fatalf("receive before availability = %#v, %v", before, err)
		}
		testClock.Advance(time.Nanosecond)
		atDeadline := mustReceiveOne(t, service, "orders")
		if atDeadline.ID != delayed.ID {
			t.Fatalf("message at availability = %q, want %q", atDeadline.ID, delayed.ID)
		}
	})

	t.Run("retention is terminal in every active state", func(t *testing.T) {
		service, _, testClock := newConformanceService(t, factory)
		if _, err := service.CreateQueue(queue.CreateQueueInput{
			QueueName:  "orders",
			Attributes: map[string]string{"MessageRetentionPeriod": "60"},
		}); err != nil {
			t.Fatalf("CreateQueue: %v", err)
		}
		delay := 900
		if _, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "delayed", DelaySeconds: &delay}); err != nil {
			t.Fatalf("delayed SendMessage: %v", err)
		}
		if _, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "in-flight"}); err != nil {
			t.Fatalf("in-flight SendMessage: %v", err)
		}
		claimed := mustReceiveOne(t, service, "orders")
		if _, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "visible"}); err != nil {
			t.Fatalf("visible SendMessage: %v", err)
		}
		testClock.Advance(60 * time.Second)
		if err := changeVisibility(service, "orders", claimed.ReceiptHandle, 120); err != nil {
			t.Fatalf("expired ChangeMessageVisibility: %v", err)
		}
		messages, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders"})
		if err != nil || len(messages) != 0 {
			t.Fatalf("ReceiveMessage at expiration = %#v, %v", messages, err)
		}
		if err := changeVisibility(service, "orders", claimed.ReceiptHandle, 120); err != nil {
			t.Fatalf("consumed expired handle: %v", err)
		}
	})

	t.Run("visibility and stale receipts cannot extend retention", func(t *testing.T) {
		service, _, testClock := newConformanceService(t, factory)
		if _, err := service.CreateQueue(queue.CreateQueueInput{
			QueueName: "orders",
			Attributes: map[string]string{
				"VisibilityTimeout":      "1",
				"MessageRetentionPeriod": "60",
			},
		}); err != nil {
			t.Fatalf("CreateQueue: %v", err)
		}
		if _, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "body"}); err != nil {
			t.Fatalf("SendMessage: %v", err)
		}
		first := mustReceiveOne(t, service, "orders")
		testClock.Advance(time.Second)
		second := mustReceiveOne(t, service, "orders")
		testClock.Advance(58 * time.Second)
		if err := changeVisibility(service, "orders", second.ReceiptHandle, 120); err != nil {
			t.Fatalf("pre-expiration ChangeMessageVisibility: %v", err)
		}
		testClock.Advance(time.Second)
		if err := changeVisibility(service, "orders", first.ReceiptHandle, 120); err != nil {
			t.Fatalf("stale change at expiration: %v", err)
		}
		if remaining, err := service.ExpireMessages(); err != nil || remaining != 0 {
			t.Fatalf("ExpireMessages after stale cleanup = %d, %v", remaining, err)
		}
		if messages, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders"}); err != nil || len(messages) != 0 {
			t.Fatalf("visibility resurrected expired message = %#v, %v", messages, err)
		}
	})

	t.Run("explicit expiration sweep reclaims idle messages", func(t *testing.T) {
		service, _, testClock := newConformanceService(t, factory)
		if _, err := service.CreateQueue(queue.CreateQueueInput{
			QueueName:  "orders",
			Attributes: map[string]string{"MessageRetentionPeriod": "60"},
		}); err != nil {
			t.Fatalf("CreateQueue: %v", err)
		}
		for _, body := range []string{"one", "two"} {
			if _, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: body}); err != nil {
				t.Fatalf("SendMessage: %v", err)
			}
		}
		testClock.Advance(60 * time.Second)
		expired, err := service.ExpireMessages()
		if err != nil || expired != 2 {
			t.Fatalf("ExpireMessages = %d, %v; want 2", expired, err)
		}
		if repeated, err := service.ExpireMessages(); err != nil || repeated != 0 {
			t.Fatalf("repeated ExpireMessages = %d, %v", repeated, err)
		}
	})

	t.Run("current repeated stale malformed and unissued delete", func(t *testing.T) {
		service, _, testClock := newConformanceService(t, factory)
		mustCreateConformanceQueue(t, service, "orders", "1")
		if _, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "body"}); err != nil {
			t.Fatalf("SendMessage: %v", err)
		}
		first := mustReceiveOne(t, service, "orders")
		for _, handle := range []string{"malformed", deterministicReceiptHandle(999999)} {
			err := service.DeleteMessage(queue.DeleteMessageInput{QueueName: "orders", ReceiptHandle: handle})
			if !errors.Is(err, queue.ErrReceiptHandleIsInvalid) {
				t.Fatalf("DeleteMessage(%q) = %v", handle, err)
			}
		}
		testClock.Advance(time.Second)
		second := mustReceiveOne(t, service, "orders")
		if err := service.DeleteMessage(queue.DeleteMessageInput{QueueName: "orders", ReceiptHandle: first.ReceiptHandle}); err != nil {
			t.Fatalf("stale DeleteMessage: %v", err)
		}
		if hidden, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders"}); err != nil || len(hidden) != 0 {
			t.Fatalf("stale delete changed current claim: %#v, %v", hidden, err)
		}
		current := queue.DeleteMessageInput{QueueName: "orders", ReceiptHandle: second.ReceiptHandle}
		if err := service.DeleteMessage(current); err != nil {
			t.Fatalf("current DeleteMessage: %v", err)
		}
		if err := service.DeleteMessage(current); err != nil {
			t.Fatalf("repeated DeleteMessage: %v", err)
		}
	})

	t.Run("visibility extension preserves metadata and exact boundary", func(t *testing.T) {
		service, _, testClock := newConformanceService(t, factory)
		mustCreateConformanceQueue(t, service, "orders", "10")
		sent, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "body"})
		if err != nil {
			t.Fatalf("SendMessage: %v", err)
		}
		first := mustReceiveOne(t, service, "orders")
		testClock.Advance(5 * time.Second)
		if err := changeVisibility(service, "orders", first.ReceiptHandle, 20); err != nil {
			t.Fatalf("ChangeMessageVisibility: %v", err)
		}
		testClock.Advance(20*time.Second - time.Nanosecond)
		if before, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders"}); err != nil || len(before) != 0 {
			t.Fatalf("receive before changed deadline = %#v, %v", before, err)
		}
		testClock.Advance(time.Nanosecond)
		second := mustReceiveOne(t, service, "orders")
		if second.ID != sent.ID || second.Body != sent.Body || second.MD5OfBody != sent.MD5OfBody || second.SentAtMillis != sent.SentAtMillis {
			t.Fatalf("visibility change mutated immutable message fields: %#v", second)
		}
		if second.ReceiveCount != 2 || second.ReceiveGeneration != 2 || second.FirstReceivedAtMillis != first.FirstReceivedAtMillis {
			t.Fatalf("receive metadata after visibility change = %#v", second)
		}
		if second.ReceiptHandle == first.ReceiptHandle {
			t.Fatal("re-receive reused receipt handle")
		}
	})

	t.Run("visibility shortening and repeated reset use command time", func(t *testing.T) {
		service, _, testClock := newConformanceService(t, factory)
		mustCreateConformanceQueue(t, service, "orders", "30")
		if _, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "body"}); err != nil {
			t.Fatalf("SendMessage: %v", err)
		}
		first := mustReceiveOne(t, service, "orders")
		testClock.Advance(5 * time.Second)
		if err := changeVisibility(service, "orders", first.ReceiptHandle, 10); err != nil {
			t.Fatalf("first ChangeMessageVisibility: %v", err)
		}
		testClock.Advance(2 * time.Second)
		if err := changeVisibility(service, "orders", first.ReceiptHandle, 2); err != nil {
			t.Fatalf("second ChangeMessageVisibility: %v", err)
		}
		testClock.Advance(2*time.Second - time.Nanosecond)
		if before, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders"}); err != nil || len(before) != 0 {
			t.Fatalf("receive before shortened deadline = %#v, %v", before, err)
		}
		testClock.Advance(time.Nanosecond)
		second := mustReceiveOne(t, service, "orders")
		if second.ReceiveGeneration != 2 || second.FirstReceivedAtMillis != first.FirstReceivedAtMillis {
			t.Fatalf("message at shortened deadline = %#v", second)
		}
	})

	t.Run("zero visibility is immediately receivable", func(t *testing.T) {
		service, _, _ := newConformanceService(t, factory)
		mustCreateConformanceQueue(t, service, "orders", "30")
		if _, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "body"}); err != nil {
			t.Fatalf("SendMessage: %v", err)
		}
		first := mustReceiveOne(t, service, "orders")
		if err := changeVisibility(service, "orders", first.ReceiptHandle, 0); err != nil {
			t.Fatalf("ChangeMessageVisibility(0): %v", err)
		}
		second := mustReceiveOne(t, service, "orders")
		if second.ReceiveCount != 2 || second.ReceiveGeneration != 2 || second.ReceiptHandle == first.ReceiptHandle {
			t.Fatalf("immediate re-receive = %#v", second)
		}
	})

	t.Run("expired current handle can rehide before rereceive", func(t *testing.T) {
		service, _, testClock := newConformanceService(t, factory)
		mustCreateConformanceQueue(t, service, "orders", "1")
		if _, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "body"}); err != nil {
			t.Fatalf("SendMessage: %v", err)
		}
		first := mustReceiveOne(t, service, "orders")
		testClock.Advance(time.Second)
		if err := changeVisibility(service, "orders", first.ReceiptHandle, 5); err != nil {
			t.Fatalf("ChangeMessageVisibility at expired deadline: %v", err)
		}
		if hidden, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders"}); err != nil || len(hidden) != 0 {
			t.Fatalf("message not rehidden by current expired handle: %#v, %v", hidden, err)
		}
		testClock.Advance(5 * time.Second)
		second := mustReceiveOne(t, service, "orders")
		if second.ReceiveGeneration != 2 || second.FirstReceivedAtMillis != first.FirstReceivedAtMillis {
			t.Fatalf("message after rehidden deadline = %#v", second)
		}
	})

	t.Run("visibility receipt safety and queue isolation", func(t *testing.T) {
		service, _, testClock := newConformanceService(t, factory)
		mustCreateConformanceQueue(t, service, "orders", "1")
		mustCreateConformanceQueue(t, service, "payments", "1")
		if _, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "body"}); err != nil {
			t.Fatalf("SendMessage: %v", err)
		}
		first := mustReceiveOne(t, service, "orders")

		for _, test := range []struct {
			name      string
			queueName string
			handle    string
			want      error
		}{
			{name: "malformed", queueName: "orders", handle: "malformed", want: queue.ErrReceiptHandleIsInvalid},
			{name: "unissued", queueName: "orders", handle: deterministicReceiptHandle(999999), want: queue.ErrReceiptHandleIsInvalid},
			{name: "corrupted", queueName: "orders", handle: first.ReceiptHandle[:len(first.ReceiptHandle)-1] + "f", want: queue.ErrReceiptHandleIsInvalid},
			{name: "cross queue", queueName: "payments", handle: first.ReceiptHandle, want: queue.ErrReceiptHandleIsInvalid},
			{name: "missing queue", queueName: "missing", handle: first.ReceiptHandle, want: queue.ErrQueueDoesNotExist},
		} {
			t.Run(test.name, func(t *testing.T) {
				err := changeVisibility(service, test.queueName, test.handle, 60)
				if !errors.Is(err, test.want) {
					t.Fatalf("ChangeMessageVisibility = %v, want %v", err, test.want)
				}
			})
		}

		testClock.Advance(time.Second)
		second := mustReceiveOne(t, service, "orders")
		if err := changeVisibility(service, "orders", first.ReceiptHandle, 100); err != nil {
			t.Fatalf("stale ChangeMessageVisibility: %v", err)
		}
		testClock.Advance(time.Second)
		third := mustReceiveOne(t, service, "orders")
		if third.ReceiveGeneration != 3 || third.ReceiptHandle == second.ReceiptHandle {
			t.Fatalf("stale handle changed latest deadline: %#v", third)
		}
		if err := service.DeleteMessage(queue.DeleteMessageInput{QueueName: "orders", ReceiptHandle: third.ReceiptHandle}); err != nil {
			t.Fatalf("DeleteMessage: %v", err)
		}
		if err := changeVisibility(service, "orders", third.ReceiptHandle, 100); err != nil {
			t.Fatalf("consumed ChangeMessageVisibility: %v", err)
		}
		if final, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders"}); err != nil || len(final) != 0 {
			t.Fatalf("consumed handle resurrected message: %#v, %v", final, err)
		}
	})

	t.Run("stale visibility change racing with rereceive is linearizable", func(t *testing.T) {
		service, _, testClock := newConformanceService(t, factory)
		mustCreateConformanceQueue(t, service, "orders", "1")
		const iterations = 20
		for iteration := 0; iteration < iterations; iteration++ {
			if _, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: fmt.Sprintf("body-%d", iteration)}); err != nil {
				t.Fatalf("iteration %d SendMessage: %v", iteration, err)
			}
			first := mustReceiveOne(t, service, "orders")
			testClock.Advance(time.Second)

			start := make(chan struct{})
			changeResult := make(chan error, 1)
			type receiveOutcome struct {
				messages []queue.Message
				err      error
			}
			receiveResult := make(chan receiveOutcome, 1)
			go func() {
				<-start
				changeResult <- changeVisibility(service, "orders", first.ReceiptHandle, 30)
			}()
			go func() {
				<-start
				messages, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders"})
				receiveResult <- receiveOutcome{messages: messages, err: err}
			}()
			close(start)

			if err := <-changeResult; err != nil {
				t.Fatalf("iteration %d ChangeMessageVisibility: %v", iteration, err)
			}
			outcome := <-receiveResult
			if outcome.err != nil {
				t.Fatalf("iteration %d ReceiveMessage: %v", iteration, outcome.err)
			}

			var latest queue.Message
			switch len(outcome.messages) {
			case 0:
				if err := changeVisibility(service, "orders", first.ReceiptHandle, 0); err != nil {
					t.Fatalf("iteration %d release winning change: %v", iteration, err)
				}
				latest = mustReceiveOne(t, service, "orders")
				if latest.ReceiveGeneration != first.ReceiveGeneration+1 {
					t.Fatalf("iteration %d generation after change-first order = %d", iteration, latest.ReceiveGeneration)
				}
			case 1:
				if outcome.messages[0].ReceiveGeneration != first.ReceiveGeneration+1 {
					t.Fatalf("iteration %d receive-first result = %#v", iteration, outcome.messages[0])
				}
				testClock.Advance(time.Second)
				latest = mustReceiveOne(t, service, "orders")
				if latest.ReceiveGeneration != first.ReceiveGeneration+2 {
					t.Fatalf("iteration %d stale change altered new deadline: %#v", iteration, latest)
				}
			default:
				t.Fatalf("iteration %d received %d messages", iteration, len(outcome.messages))
			}
			if err := service.DeleteMessage(queue.DeleteMessageInput{QueueName: "orders", ReceiptHandle: latest.ReceiptHandle}); err != nil {
				t.Fatalf("iteration %d cleanup DeleteMessage: %v", iteration, err)
			}
		}
	})

	t.Run("delete racing with visibility change remains terminal", func(t *testing.T) {
		service, _, testClock := newConformanceService(t, factory)
		mustCreateConformanceQueue(t, service, "orders", "30")
		const iterations = 20
		for iteration := 0; iteration < iterations; iteration++ {
			if _, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: fmt.Sprintf("body-%d", iteration)}); err != nil {
				t.Fatalf("iteration %d SendMessage: %v", iteration, err)
			}
			message := mustReceiveOne(t, service, "orders")
			start := make(chan struct{})
			changeResult := make(chan error, 1)
			deleteResult := make(chan error, 1)
			go func() {
				<-start
				changeResult <- changeVisibility(service, "orders", message.ReceiptHandle, 60)
			}()
			go func() {
				<-start
				deleteResult <- service.DeleteMessage(queue.DeleteMessageInput{QueueName: "orders", ReceiptHandle: message.ReceiptHandle})
			}()
			close(start)
			if err := <-changeResult; err != nil {
				t.Fatalf("iteration %d ChangeMessageVisibility: %v", iteration, err)
			}
			if err := <-deleteResult; err != nil {
				t.Fatalf("iteration %d DeleteMessage: %v", iteration, err)
			}
			testClock.Advance(61 * time.Second)
			if messages, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders"}); err != nil || len(messages) != 0 {
				t.Fatalf("iteration %d deleted message resurrected: %#v, %v", iteration, messages, err)
			}
		}
	})

	t.Run("twenty concurrent receive", func(t *testing.T) {
		service, _, _ := newConformanceService(t, factory)
		mustCreateConformanceQueue(t, service, "orders", "30")
		if _, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "body"}); err != nil {
			t.Fatalf("SendMessage: %v", err)
		}
		const workers = 20
		var wait sync.WaitGroup
		results := make(chan int, workers)
		errorsChannel := make(chan error, workers)
		for index := 0; index < workers; index++ {
			wait.Add(1)
			go func() {
				defer wait.Done()
				messages, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders"})
				if err != nil {
					errorsChannel <- err
					return
				}
				results <- len(messages)
			}()
		}
		wait.Wait()
		close(results)
		close(errorsChannel)
		for err := range errorsChannel {
			t.Error(err)
		}
		claims := 0
		for count := range results {
			claims += count
		}
		if claims != 1 {
			t.Fatalf("claims = %d, want 1", claims)
		}
	})

	t.Run("twenty concurrent delete", func(t *testing.T) {
		service, _, _ := newConformanceService(t, factory)
		mustCreateConformanceQueue(t, service, "orders", "30")
		if _, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "body"}); err != nil {
			t.Fatalf("SendMessage: %v", err)
		}
		message := mustReceiveOne(t, service, "orders")
		input := queue.DeleteMessageInput{QueueName: "orders", ReceiptHandle: message.ReceiptHandle}
		const workers = 20
		var wait sync.WaitGroup
		errorsChannel := make(chan error, workers)
		for index := 0; index < workers; index++ {
			wait.Add(1)
			go func() {
				defer wait.Done()
				errorsChannel <- service.DeleteMessage(input)
			}()
		}
		wait.Wait()
		close(errorsChannel)
		for err := range errorsChannel {
			if err != nil {
				t.Errorf("DeleteMessage: %v", err)
			}
		}
		messages, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders"})
		if err != nil || len(messages) != 0 {
			t.Fatalf("ReceiveMessage after delete = %#v, %v", messages, err)
		}
	})

	t.Run("close is idempotent and health fails closed", func(t *testing.T) {
		_, repository, _ := newConformanceService(t, factory)
		if err := repository.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if err := repository.Close(); err != nil {
			t.Fatalf("repeated Close: %v", err)
		}
		if err := repository.Health(); !errors.Is(err, queue.ErrRepositoryClosed) {
			t.Fatalf("Health after close = %v, want ErrRepositoryClosed", err)
		}
		if _, _, err := repository.Get("orders"); !errors.Is(err, queue.ErrRepositoryClosed) {
			t.Fatalf("Get after close = %v, want ErrRepositoryClosed", err)
		}
		if _, err := repository.SetAttributes(queue.QueueAttributesCommand{QueueName: "orders"}); !errors.Is(err, queue.ErrRepositoryClosed) {
			t.Fatalf("SetAttributes after close = %v, want ErrRepositoryClosed", err)
		}
		if err := repository.ChangeVisibility(queue.ChangeVisibilityCommand{QueueName: "orders"}); !errors.Is(err, queue.ErrRepositoryClosed) {
			t.Fatalf("ChangeVisibility after close = %v, want ErrRepositoryClosed", err)
		}
		if _, err := repository.Expire(queue.ExpireCommand{}); !errors.Is(err, queue.ErrRepositoryClosed) {
			t.Fatalf("Expire after close = %v, want ErrRepositoryClosed", err)
		}
	})
}

func newConformanceService(t *testing.T, factory repositoryFactory) (*queue.Service, queue.Repository, *manualClock) {
	t.Helper()
	repository := factory(t)
	t.Cleanup(func() {
		if err := repository.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	testClock := &manualClock{now: time.Date(2026, time.August, 23, 1, 2, 3, 456789123, time.UTC)}
	var messageSequence atomic.Uint64
	var receiptSequence atomic.Uint64
	service := queue.NewService(
		repository,
		queue.WithClock(testClock),
		queue.WithMessageIDGenerator(func() string { return fmt.Sprintf("message-%032x", messageSequence.Add(1)) }),
		queue.WithReceiptHandleGenerator(func() string { return deterministicReceiptHandle(receiptSequence.Add(1)) }),
	)
	return service, repository, testClock
}

func mustCreateConformanceQueue(t *testing.T, service *queue.Service, name, visibility string) {
	t.Helper()
	if _, err := service.CreateQueue(queue.CreateQueueInput{
		QueueName:  name,
		Attributes: map[string]string{"VisibilityTimeout": visibility},
	}); err != nil {
		t.Fatalf("CreateQueue(%s): %v", name, err)
	}
}

func mustReceiveOne(t *testing.T, service *queue.Service, name string) queue.Message {
	t.Helper()
	messages, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: name})
	if err != nil || len(messages) != 1 {
		t.Fatalf("ReceiveMessage(%s) = %#v, %v", name, messages, err)
	}
	return messages[0]
}

func changeVisibility(service *queue.Service, queueName, receiptHandle string, timeout int) error {
	return service.ChangeMessageVisibility(queue.ChangeMessageVisibilityInput{
		QueueName:         queueName,
		ReceiptHandle:     receiptHandle,
		VisibilityTimeout: timeout,
	})
}
