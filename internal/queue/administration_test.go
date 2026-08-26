package queue_test

import (
	"context"
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

func TestAdministrationConformance(t *testing.T) {
	factories := map[string]repositoryFactory{
		"memory": func(*testing.T) queue.Repository { return queue.NewMemoryRepository() },
		"bbolt": func(t *testing.T) queue.Repository {
			directory := t.TempDir()
			if err := os.Chmod(directory, 0o700); err != nil {
				t.Fatal(err)
			}
			repository, err := boltrepo.Open(boltrepo.Config{Path: filepath.Join(directory, "simq.db"), OpenTimeout: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			return repository
		},
	}
	for name, factory := range factories {
		t.Run(name, func(t *testing.T) { runAdministrationConformance(t, factory) })
	}
}

func runAdministrationConformance(t *testing.T, factory repositoryFactory) {
	repository := factory(t)
	t.Cleanup(func() {
		if err := repository.Close(); err != nil {
			t.Error(err)
		}
	})
	var ids atomic.Uint64
	service := queue.NewService(repository, queue.WithQueueIDGenerator(func() string { return fmt.Sprintf("q_%032x", ids.Add(1)) }))

	alpha := mustCreateAdminQueue(t, service, "alpha")
	orders := mustCreateAdminQueue(t, service, "orders")
	omega := mustCreateAdminQueue(t, service, "omega")
	page, token, err := service.ListQueues(queue.ListQueuesInput{MaxResults: adminIntPointer(2)})
	if err != nil || len(page.Queues) != 2 || page.Queues[0].Name != "alpha" || page.Queues[1].Name != "omega" || token == "" {
		t.Fatalf("first ListQueues = %#v, %q, %v", page, token, err)
	}
	second, next, err := service.ListQueues(queue.ListQueuesInput{MaxResults: adminIntPointer(2), NextToken: token})
	if err != nil || len(second.Queues) != 1 || second.Queues[0].Name != "orders" || next != "" {
		t.Fatalf("second ListQueues = %#v, %q, %v", second, next, err)
	}
	prefixed, _, err := service.ListQueues(queue.ListQueuesInput{QueueNamePrefix: "om"})
	if err != nil || len(prefixed.Queues) != 1 || prefixed.Queues[0].ID != omega.ID {
		t.Fatalf("prefix page = %#v, %v", prefixed, err)
	}
	mustCreateAdminQueue(t, service, "new")
	if _, _, err := service.ListQueues(queue.ListQueuesInput{MaxResults: adminIntPointer(2), NextToken: token}); !errors.Is(err, queue.ErrInvalidPaginationToken) {
		t.Fatalf("stale token error = %v", err)
	}

	if err := service.TagQueue(queue.TagQueueInput{QueueName: orders.Name, QueueID: orders.ID, Tags: map[string]string{"env": "prod", "team": "queue"}}); err != nil {
		t.Fatal(err)
	}
	if err := service.TagQueue(queue.TagQueueInput{QueueName: orders.Name, QueueID: orders.ID, Tags: map[string]string{"env": "stage"}}); err != nil {
		t.Fatal(err)
	}
	if err := service.UntagQueue(queue.UntagQueueInput{QueueName: orders.Name, QueueID: orders.ID, TagKeys: []string{"team", "missing"}}); err != nil {
		t.Fatal(err)
	}
	tags, err := service.ListQueueTags(queue.QueueRef{Name: orders.Name, ID: orders.ID})
	if err != nil || len(tags) != 1 || tags["env"] != "stage" {
		t.Fatalf("tags = %#v, %v", tags, err)
	}
	tooMany := make(map[string]string)
	for index := 0; index < 51; index++ {
		tooMany[fmt.Sprintf("k%d", index)] = "v"
	}
	if err := service.TagQueue(queue.TagQueueInput{QueueName: alpha.Name, QueueID: alpha.ID, Tags: tooMany}); !errors.Is(err, queue.ErrOverLimit) {
		t.Fatalf("tag limit = %v", err)
	}
	if tags, err := service.ListQueueTags(queue.QueueRef{Name: alpha.Name, ID: alpha.ID}); err != nil || len(tags) != 0 {
		t.Fatalf("failed tag mutation changed state = %#v, %v", tags, err)
	}

	if err := service.AddPermission(queue.AddPermissionInput{QueueName: orders.Name, QueueID: orders.ID, Label: "writers", AWSAccountIDs: []string{"222222222222", "111111111111"}, Actions: []string{"SendMessage", "ReceiveMessage"}}); err != nil {
		t.Fatal(err)
	}
	attributes, err := service.GetQueueAttributes(queue.GetQueueAttributesInput{QueueName: orders.Name, QueueID: orders.ID, AttributeNames: []string{"Policy"}})
	wantPolicy := `{"Version":"SimQ-2026-08-24","Statement":[{"Label":"writers","AWSAccountIds":["111111111111","222222222222"],"Actions":["ReceiveMessage","SendMessage"]}]}`
	if err != nil || attributes["Policy"] != wantPolicy {
		t.Fatalf("Policy = %q, %v", attributes["Policy"], err)
	}
	for index := 0; index < 19; index++ {
		if err := service.AddPermission(queue.AddPermissionInput{QueueName: orders.Name, QueueID: orders.ID, Label: fmt.Sprintf("label-%02d", index), AWSAccountIDs: []string{"111111111111"}, Actions: []string{"ReceiveMessage"}}); err != nil {
			t.Fatalf("permission %d: %v", index, err)
		}
	}
	if err := service.AddPermission(queue.AddPermissionInput{QueueName: orders.Name, QueueID: orders.ID, Label: "overflow", AWSAccountIDs: []string{"111111111111"}, Actions: []string{"ReceiveMessage"}}); !errors.Is(err, queue.ErrOverLimit) {
		t.Fatalf("permission limit = %v", err)
	}
	if err := service.AddPermission(queue.AddPermissionInput{QueueName: orders.Name, QueueID: orders.ID, Label: "writers", AWSAccountIDs: []string{"333333333333"}, Actions: []string{"DeleteMessage"}}); err != nil {
		t.Fatalf("permission replacement at limit: %v", err)
	}

	if _, err := service.SendMessage(queue.SendMessageInput{QueueName: orders.Name, QueueID: orders.ID, MessageBody: "purged"}); err != nil {
		t.Fatal(err)
	}
	claimed, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: orders.Name, QueueID: orders.ID})
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim = %#v, %v", claimed, err)
	}
	if _, err := service.SendMessage(queue.SendMessageInput{QueueName: alpha.Name, QueueID: alpha.ID, MessageBody: "preserved"}); err != nil {
		t.Fatal(err)
	}
	if err := service.PurgeQueue(queue.QueueRef{Name: orders.Name, ID: orders.ID}); err != nil {
		t.Fatal(err)
	}
	if messages, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: orders.Name, QueueID: orders.ID}); err != nil || len(messages) != 0 {
		t.Fatalf("after purge = %#v, %v", messages, err)
	}
	if err := service.DeleteMessage(queue.DeleteMessageInput{QueueName: orders.Name, QueueID: orders.ID, ReceiptHandle: claimed[0].ReceiptHandle}); !errors.Is(err, queue.ErrReceiptHandleIsInvalid) {
		t.Fatalf("purged receipt = %v", err)
	}
	if tags, _ := service.ListQueueTags(queue.QueueRef{Name: orders.Name, ID: orders.ID}); tags["env"] != "stage" {
		t.Fatalf("purge changed tags: %#v", tags)
	}
	if messages, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: alpha.Name, QueueID: alpha.ID}); err != nil || len(messages) != 1 {
		t.Fatalf("purge crossed queues = %#v, %v", messages, err)
	}

	old := orders
	if err := service.DeleteQueue(queue.QueueRef{Name: old.Name, ID: old.ID}); err != nil {
		t.Fatal(err)
	}
	recreated := mustCreateAdminQueue(t, service, old.Name)
	if recreated.ID == old.ID || recreated.LegacyURLAllowed {
		t.Fatalf("recreated queue = %#v", recreated)
	}
	for _, input := range []queue.SendMessageInput{{QueueName: old.Name, QueueID: old.ID, MessageBody: "stale"}, {QueueName: old.Name, MessageBody: "legacy-stale"}} {
		if _, err := service.SendMessage(input); !errors.Is(err, queue.ErrQueueDoesNotExist) {
			t.Fatalf("stale send = %v", err)
		}
	}
	if _, err := service.SendMessage(queue.SendMessageInput{QueueName: recreated.Name, QueueID: recreated.ID, MessageBody: "new"}); err != nil {
		t.Fatal(err)
	}
	newClaim, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: recreated.Name, QueueID: recreated.ID})
	if err != nil || len(newClaim) != 1 {
		t.Fatalf("new generation claim = %#v, %v", newClaim, err)
	}
	if err := service.DeleteMessage(queue.DeleteMessageInput{QueueName: recreated.Name, QueueID: recreated.ID, ReceiptHandle: claimed[0].ReceiptHandle}); !errors.Is(err, queue.ErrReceiptHandleIsInvalid) {
		t.Fatalf("old receipt on new generation = %v", err)
	}
	if err := service.DeleteMessage(queue.DeleteMessageInput{QueueName: recreated.Name, QueueID: recreated.ID, ReceiptHandle: newClaim[0].ReceiptHandle}); err != nil {
		t.Fatalf("new receipt after stale attempt = %v", err)
	}
}

func mustCreateAdminQueue(t *testing.T, service *queue.Service, name string) queue.Queue {
	t.Helper()
	value, err := service.CreateQueue(queue.CreateQueueInput{QueueName: name})
	if err != nil {
		t.Fatalf("CreateQueue(%s): %v", name, err)
	}
	return value
}

func adminIntPointer(value int) *int { return &value }

type secondClaimGateRepository struct {
	queue.Repository
	count   atomic.Int32
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (r *secondClaimGateRepository) Claim(input queue.ClaimInput) (queue.ClaimResult, error) {
	result, err := r.Repository.Claim(input)
	if r.count.Add(1) == 2 {
		r.once.Do(func() { close(r.entered) })
		<-r.release
	}
	return result, err
}

func TestDeleteQueueWakesGenerationBoundLongPoll(t *testing.T) {
	base := queue.NewMemoryRepository()
	gated := &secondClaimGateRepository{Repository: base, entered: make(chan struct{}), release: make(chan struct{})}
	service := queue.NewService(gated)
	created := mustCreateAdminQueue(t, service, "orders")
	result := make(chan error, 1)
	go func() {
		_, err := service.ReceiveMessageContext(context.Background(), queue.ReceiveMessageInput{QueueName: created.Name, QueueID: created.ID, WaitTimeSeconds: adminIntPointer(20)})
		result <- err
	}()
	select {
	case <-gated.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("long poll did not register and recheck")
	}
	if err := service.DeleteQueue(queue.QueueRef{Name: created.Name, ID: created.ID}); err != nil {
		t.Fatal(err)
	}
	close(gated.release)
	select {
	case err := <-result:
		if !errors.Is(err, queue.ErrQueueDoesNotExist) {
			t.Fatalf("long poll after delete = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("deleted queue did not wake long poll")
	}
}
