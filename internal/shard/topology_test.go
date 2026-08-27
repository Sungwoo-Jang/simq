package shard

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"simq/internal/ownership"
	"simq/internal/queue"
	"simq/internal/storage/boltrepo"
	"simq/internal/tenant"
	"simq/internal/topology"
)

func TestElasticTopologyBackfillsActivatesAndDrainsWithoutImplicitMovement(t *testing.T) {
	root := t.TempDir()
	local := make(map[string]*boltrepo.Repository)
	repositories := make(map[string]queue.Repository)
	for _, id := range []string{"s0", "s1", "s2"} {
		repository, err := boltrepo.Open(boltrepo.Config{Path: filepath.Join(root, id+".db")})
		if err != nil {
			t.Fatal(err)
		}
		local[id], repositories[id] = repository, repository
	}
	catalog := topology.Catalog{Version: topology.RecordVersion, ClusterID: "00112233445566778899aabbccddeeff", Generation: 1, DefaultShard: "s0", DirectoryMode: topology.DirectoryLegacy, Shards: []topology.Shard{
		{ID: "s0", Incarnation: "11112222333344445555666677778888", State: topology.ShardReady, EntryAPIURL: "https://s0.example", Initial: true},
		{ID: "s1", Incarnation: "9999aaaabbbbccccddddeeeeffff0000", State: topology.ShardReady, EntryAPIURL: "https://s1.example", Initial: true},
		{ID: "s2", Incarnation: "abcdefabcdefabcdefabcdefabcdefab", State: topology.ShardCandidate, EntryAPIURL: "https://s2.example"},
	}}
	manager, err := New(Config{DefaultShard: "s0", Repositories: repositories, TopologyCatalog: &catalog})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close() })

	legacyTenant := "legacy-topology-tenant"
	legacyDigest := digestForTenant(legacyTenant)
	legacyShard := manager.hashedShardID(legacyDigest)
	legacyService := queue.NewService(tenant.New(local[legacyShard], nil)).WithTenant(legacyTenant)
	if _, err := legacyService.CreateQueue(queue.CreateQueueInput{QueueName: "orders"}); err != nil {
		t.Fatal(err)
	}
	if _, found, err := local["s0"].LocalOwnership(legacyDigest); err != nil || found {
		t.Fatalf("pre-backfill ownership found=%v err=%v", found, err)
	}

	backfillID := "to_00000000000000000000000000000001"
	if _, err := manager.BeginTopologyBackfill(backfillID); err != nil {
		t.Fatal(err)
	}
	advanceTopologyToCompletion(t, manager, backfillID)
	before, found, err := local["s0"].LocalOwnership(legacyDigest)
	if err != nil || !found || before.ShardID != legacyShard || before.Epoch != 1 {
		t.Fatalf("backfilled owner=%+v found=%v err=%v", before, found, err)
	}

	activateID := "to_00000000000000000000000000000002"
	if _, err := manager.BeginShardActivation(activateID, "s2"); err != nil {
		t.Fatal(err)
	}
	advanceTopologyToCompletion(t, manager, activateID)
	after, _, _ := local["s0"].LocalOwnership(legacyDigest)
	if after != before {
		t.Fatalf("activation moved existing tenant: before=%+v after=%+v", before, after)
	}

	newTenant := ""
	for index := 0; index < 10_000; index++ {
		candidate := fmt.Sprintf("candidate-tenant-%d", index)
		selected, selectErr := topology.SelectShard(digestForTenant(candidate), mustTopologyCatalog(t, manager), false, "")
		if selectErr == nil && selected == "s2" {
			newTenant = candidate
			break
		}
	}
	if newTenant == "" {
		t.Fatal("could not find a tenant assigned to activated shard")
	}
	service := queue.NewService(tenant.New(manager, nil)).WithTenant(newTenant)
	created, err := service.CreateQueue(queue.CreateQueueInput{QueueName: "events"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.SendMessage(queue.SendMessageInput{QueueName: created.Name, QueueID: created.ID, MessageBody: "preserve-me"}); err != nil {
		t.Fatal(err)
	}
	newDigest := digestForTenant(newTenant)
	owner, found, err := local["s0"].LocalOwnership(newDigest)
	if err != nil || !found || owner.ShardID != "s2" {
		t.Fatalf("new owner=%+v found=%v err=%v", owner, found, err)
	}

	drainID := "to_00000000000000000000000000000003"
	if _, err := manager.BeginShardDrain(drainID, "s2"); err != nil {
		t.Fatal(err)
	}
	advanceTopologyToCompletion(t, manager, drainID)
	owner, found, err = local["s0"].LocalOwnership(newDigest)
	if err != nil || !found || owner.ShardID == "s2" || owner.Epoch != 2 {
		t.Fatalf("drained owner=%+v found=%v err=%v", owner, found, err)
	}
	finalCatalog := mustTopologyCatalog(t, manager)
	retired, _ := topology.FindShard(finalCatalog, "s2")
	if retired.State != topology.ShardRetired {
		t.Fatalf("retired state=%s", retired.State)
	}
	if _, err := manager.BeginShardActivation("to_00000000000000000000000000000004", "s2"); err == nil {
		t.Fatal("retired shard ID was reusable")
	}

	destinationService := queue.NewService(tenant.New(manager, nil)).WithTenant(newTenant)
	messages, err := destinationService.ReceiveMessage(queue.ReceiveMessageInput{QueueName: created.Name, QueueID: created.ID})
	if err != nil || len(messages) != 1 || messages[0].Body != "preserve-me" {
		t.Fatalf("drained messages=%+v err=%v", messages, err)
	}
}

func TestSelectiveHostingReturnsEntryHintWithoutLocalFallback(t *testing.T) {
	root := t.TempDir()
	repositories := make(map[string]queue.Repository)
	for _, id := range []string{"s0", "s1"} {
		repository, err := boltrepo.Open(boltrepo.Config{Path: filepath.Join(root, id+".db")})
		if err != nil {
			t.Fatal(err)
		}
		repositories[id] = repository
	}
	catalog := topology.Catalog{Version: topology.RecordVersion, ClusterID: "00112233445566778899aabbccddeeff", Generation: 1, DefaultShard: "s0", DirectoryMode: topology.DirectoryExplicit, BackfillComplete: true, Shards: []topology.Shard{
		{ID: "s0", Incarnation: "11112222333344445555666677778888", State: topology.ShardReady, EntryAPIURL: "https://s0.example", Initial: true},
		{ID: "s1", Incarnation: "9999aaaabbbbccccddddeeeeffff0000", State: topology.ShardReady, EntryAPIURL: "https://s1.example", Initial: true},
		{ID: "s2", Incarnation: "abcdefabcdefabcdefabcdefabcdefab", State: topology.ShardReady, EntryAPIURL: "https://s2.example"},
	}}
	manager, err := New(Config{DefaultShard: "s0", Repositories: repositories, TopologyCatalog: &catalog})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	if _, err := manager.TopologyCatalog(); err != nil {
		t.Fatal(err)
	}
	digest := ""
	for index := 0; index < 10_000; index++ {
		candidate := digestForTenant(fmt.Sprintf("remote-%d", index))
		selected, _ := topology.SelectShard(candidate, catalog, false, "")
		if selected == "s2" {
			digest = candidate
			break
		}
	}
	control := repositories["s0"].(topology.ControlStore)
	if _, err := control.EnsureAssignment(digest); err != nil {
		t.Fatal(err)
	}
	clustered, leader, entry := manager.RouteForKey("tn_" + digest + "_orders")
	if !clustered || leader || entry != "https://s2.example" {
		t.Fatalf("route clustered=%v leader=%v entry=%q", clustered, leader, entry)
	}
	if _, err := manager.activeRepositoryForKey("tn_" + digest + "_orders"); err == nil {
		t.Fatal("selective node silently fell back to a local shard")
	} else {
		var remote *RemoteShardError
		if !errors.As(err, &remote) || remote.ShardID != "s2" {
			t.Fatalf("remote error=%v", err)
		}
	}
}

func advanceTopologyToCompletion(t *testing.T, manager *Manager, id string) topology.Status {
	t.Helper()
	var status topology.Status
	for step := 0; step < 100; step++ {
		var err error
		status, err = manager.AdvanceTopologyOperation(id)
		if err != nil {
			t.Fatalf("advance %s step %d: %v status=%+v", id, step, err, status)
		}
		if status.Operation.Phase == topology.PhaseCompleted {
			return status
		}
	}
	t.Fatalf("topology operation %s did not complete: %+v", id, status)
	return status
}

func mustTopologyCatalog(t *testing.T, manager *Manager) topology.Catalog {
	t.Helper()
	catalog, err := manager.TopologyCatalog()
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}

var _ ownership.ControlStore = (*boltrepo.Repository)(nil)
