package shard

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"path/filepath"
	"testing"

	"simq/internal/ownership"
	"simq/internal/queue"
	"simq/internal/storage/boltrepo"
	"simq/internal/tenant"
)

func newMigrationManager(t *testing.T) (*Manager, map[string]*boltrepo.Repository) {
	t.Helper()
	repositories := make(map[string]*boltrepo.Repository, 2)
	configured := make(map[string]queue.Repository, 2)
	for _, shardID := range []string{"s0", "s1"} {
		repository, err := boltrepo.Open(boltrepo.Config{Path: filepath.Join(t.TempDir(), shardID+".db")})
		if err != nil {
			t.Fatal(err)
		}
		repositories[shardID], configured[shardID] = repository, repository
	}
	manager, err := New(Config{DefaultShard: "s0", CatalogRevision: "00112233445566778899aabbccddeeff", Repositories: configured})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	return manager, repositories
}

func digestForTenant(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func serviceForTenant(manager *Manager, tenantID string) *queue.Service {
	return queue.NewService(tenant.New(manager, nil, "legacy").ForTenant(tenantID))
}

func advanceUntil(t *testing.T, manager *Manager, id string, terminal ownership.Phase) ownership.Status {
	t.Helper()
	var last ownership.Status
	for attempt := 0; attempt < 20; attempt++ {
		status, err := manager.TenantMigrationStatus(id)
		if err != nil {
			t.Fatal(err)
		}
		if status.Migration.Phase == terminal {
			return status
		}
		last = status
		if _, err := manager.AdvanceTenantMigration(id); err != nil {
			t.Fatalf("advance phase %s: %v", status.Migration.Phase, err)
		}
	}
	t.Fatalf("migration %s did not reach %s; last=%+v", id, terminal, last)
	return ownership.Status{}
}

func TestTenantMigrationPreservesFIFOAndIsolatesOtherTenant(t *testing.T) {
	manager, repositories := newMigrationManager(t)
	tenantID := "tenant-move"
	digest := digestForTenant(tenantID)
	source := manager.hashedShardID(digest)
	destination := "s0"
	if source == destination {
		destination = "s1"
	}
	service := serviceForTenant(manager, tenantID)
	created, err := service.CreateQueue(queue.CreateQueueInput{QueueName: "orders.fifo", Attributes: map[string]string{"FifoQueue": "true"}})
	if err != nil {
		t.Fatal(err)
	}
	for index, body := range []string{"first", "second"} {
		if _, err := service.SendMessage(queue.SendMessageInput{QueueName: created.Name, QueueID: created.ID, MessageBody: body, MessageGroupID: "group", MessageDeduplicationID: "dedup-" + string(rune('a'+index))}); err != nil {
			t.Fatal(err)
		}
	}
	other := serviceForTenant(manager, "tenant-stay")
	otherQueue, err := other.CreateQueue(queue.CreateQueueInput{QueueName: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := other.SendMessage(queue.SendMessageInput{QueueName: otherQueue.Name, QueueID: otherQueue.ID, MessageBody: "untouched"}); err != nil {
		t.Fatal(err)
	}

	id := "tm_00000000000000000000000000000001"
	if _, err := manager.BeginTenantMigration(id, digest, destination); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.AdvanceTenantMigration(id); err != nil {
		t.Fatal(err)
	}
	if _, err := service.GetQueue(queue.GetQueueInput{QueueName: created.Name}); !errors.Is(err, queue.ErrTenantMigrating) {
		t.Fatalf("frozen tenant read error = %v", err)
	}
	completed := advanceUntil(t, manager, id, ownership.PhaseCompleted)
	if completed.Migration.DestinationEpoch != 2 || manager.shardIDForKey("tn_"+digest+"_orders.fifo") != destination {
		t.Fatalf("completed migration = %+v", completed)
	}
	for shardID, repository := range repositories {
		if err := repository.Health(); err != nil {
			t.Fatalf("shard %s health: %v", shardID, err)
		}
	}
	returnID := "tm_00000000000000000000000000000003"
	if _, err := manager.BeginTenantMigration(returnID, digest, source); err != nil {
		t.Fatalf("begin return migration: %v", err)
	}
	returned := advanceUntil(t, manager, returnID, ownership.PhaseCompleted)
	if returned.Migration.DestinationEpoch != 3 || manager.shardIDForKey("tn_"+digest+"_orders.fifo") != source {
		t.Fatalf("returned migration = %+v", returned)
	}
	one := 1
	first, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: created.Name, QueueID: created.ID, MaxNumberOfMessages: &one})
	if err != nil || len(first) != 1 || first[0].Body != "first" {
		t.Fatalf("first after migration=%+v err=%v", first, err)
	}
	if err := service.DeleteMessage(queue.DeleteMessageInput{QueueName: created.Name, QueueID: created.ID, ReceiptHandle: first[0].ReceiptHandle}); err != nil {
		t.Fatal(err)
	}
	second, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: created.Name, QueueID: created.ID, MaxNumberOfMessages: &one})
	if err != nil || len(second) != 1 || second[0].Body != "second" {
		t.Fatalf("second after migration=%+v err=%v", second, err)
	}
	remaining, err := other.ReceiveMessage(queue.ReceiveMessageInput{QueueName: otherQueue.Name, QueueID: otherQueue.ID, MaxNumberOfMessages: &one})
	if err != nil || len(remaining) != 1 || remaining[0].Body != "untouched" {
		t.Fatalf("other tenant=%+v err=%v", remaining, err)
	}
	internalName := "tn_" + digest + "_" + created.Name
	if _, found, err := repositories[destination].Get(internalName); err != nil || found {
		t.Fatalf("previous owner retained queue found=%v err=%v", found, err)
	}
	if _, found, err := repositories[source].Get(internalName); err != nil || !found {
		t.Fatalf("return destination queue found=%v err=%v", found, err)
	}
}

func TestTenantMigrationAbortRestoresSourceAndRemovesPreparedDestination(t *testing.T) {
	manager, repositories := newMigrationManager(t)
	tenantID := "tenant-abort"
	digest := digestForTenant(tenantID)
	source := manager.hashedShardID(digest)
	destination := "s0"
	if source == destination {
		destination = "s1"
	}
	service := serviceForTenant(manager, tenantID)
	created, err := service.CreateQueue(queue.CreateQueueInput{QueueName: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	id := "tm_00000000000000000000000000000002"
	if _, err := manager.BeginTenantMigration(id, digest, destination); err != nil {
		t.Fatal(err)
	}
	for {
		status, err := manager.TenantMigrationStatus(id)
		if err != nil {
			t.Fatal(err)
		}
		if status.Migration.Phase == ownership.PhasePreparing && status.NextAction == "record-prepared" {
			break
		}
		if _, err := manager.AdvanceTenantMigration(id); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := manager.AbortTenantMigration(id); err != nil {
		t.Fatal(err)
	}
	// Recovery must finish local cleanup even if the control record reached its
	// terminal phase before a previous coordinator observed both shard fences.
	control := repositories[manager.defaultShard]
	if _, err := control.TransitionMigration(ownership.TransitionCommand{MigrationID: id, ExpectedPhase: ownership.PhaseAborting, NextPhase: ownership.PhaseAborted}); err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 5; attempt++ {
		status, err := manager.TenantMigrationStatus(id)
		if err != nil {
			t.Fatal(err)
		}
		if status.Migration.Phase == ownership.PhaseAborted && status.NextAction == "" {
			break
		}
		if _, err := manager.AdvanceTenantMigration(id); err != nil {
			t.Fatal(err)
		}
		if attempt == 4 {
			t.Fatal("aborted migration retained local cleanup work")
		}
	}
	if _, err := service.GetQueue(queue.GetQueueInput{QueueName: created.Name}); err != nil {
		t.Fatalf("source did not resume: %v", err)
	}
	if manager.shardIDForKey("tn_"+digest+"_orders") != source {
		t.Fatal("abort changed tenant ownership")
	}
}
