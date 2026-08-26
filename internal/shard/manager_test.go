package shard

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"testing"

	"simq/internal/queue"
	"simq/internal/tenant"
)

type routingMemory struct {
	*queue.MemoryRepository
	leader bool
	url    string
}

func (r *routingMemory) Clustered() bool      { return true }
func (r *routingMemory) IsLeader() bool       { return r.leader }
func (r *routingMemory) LeaderAPIURL() string { return r.url }

type adminMemory struct {
	*routingMemory
	members []queue.ClusterMember
}

func (r *adminMemory) ClusterMembers() ([]queue.ClusterMember, error) {
	return append([]queue.ClusterMember(nil), r.members...), nil
}
func (r *adminMemory) AddClusterVoter(id, address string) error {
	for index := range r.members {
		if r.members[index].ID == id {
			r.members[index].Address = address
			r.members[index].Suffrage = "Voter"
			return nil
		}
	}
	r.members = append(r.members, queue.ClusterMember{ID: id, Address: address, Suffrage: "Voter"})
	return nil
}
func (r *adminMemory) AddClusterNonvoter(id, address string) error {
	r.members = append(r.members, queue.ClusterMember{ID: id, Address: address, Suffrage: "Nonvoter"})
	return nil
}
func (r *adminMemory) DemoteClusterVoter(id string) error {
	for index := range r.members {
		if r.members[index].ID == id {
			r.members[index].Suffrage = "Nonvoter"
		}
	}
	return nil
}
func (r *adminMemory) RemoveClusterServer(id string) error {
	for index := range r.members {
		if r.members[index].ID == id {
			r.members = append(r.members[:index], r.members[index+1:]...)
			break
		}
	}
	return nil
}
func (r *adminMemory) TriggerClusterSnapshot() error { return nil }

func namespaceForTenant(value string) string {
	digest := sha256.Sum256([]byte(value))
	return "tn_" + hex.EncodeToString(digest[:]) + "_"
}

func TestManagerPinsEachTenantToOneShardAndRoutesItsLeader(t *testing.T) {
	left := &routingMemory{MemoryRepository: queue.NewMemoryRepository(), leader: true, url: "https://left"}
	right := &routingMemory{MemoryRepository: queue.NewMemoryRepository(), leader: false, url: "https://right"}
	manager, err := New(Config{DefaultShard: "left", Repositories: map[string]queue.Repository{"left": left, "right": right}})
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()

	tenants := make(map[string]string)
	for index := 0; len(tenants) < 2 && index < 1000; index++ {
		id := fmt.Sprintf("tenant-%d", index)
		shardID := manager.shardIDForKey(namespaceForTenant(id))
		tenants[shardID] = id
	}
	if len(tenants) != 2 {
		t.Fatal("test could not find tenants on both rendezvous shards")
	}
	base := tenant.New(manager, nil)
	for shardID, tenantID := range tenants {
		service := queue.NewService(base.ForTenant(tenantID), queue.WithQueueIDGenerator(func() string { return fmt.Sprintf("q_%032x", len(tenantID)) }))
		if _, err := service.CreateQueue(queue.CreateQueueInput{QueueName: "orders"}); err != nil {
			t.Fatalf("create tenant %s: %v", tenantID, err)
		}
		clustered, leader, leaderURL := service.ClusterRoute()
		wantLeader := shardID == "left"
		if !clustered || leader != wantLeader || leaderURL != map[string]string{"left": "https://left", "right": "https://right"}[shardID] {
			t.Fatalf("tenant %s route=(%v,%v,%q)", tenantID, clustered, leader, leaderURL)
		}
		page, _, err := service.ListQueues(queue.ListQueuesInput{})
		if err != nil || len(page.Queues) != 1 || page.Queues[0].Name != "orders" {
			t.Fatalf("tenant %s queues=%+v err=%v", tenantID, page, err)
		}
	}
	leftPage, err := left.ListQueues(queue.ListQueuesCommand{Limit: 10})
	if err != nil || len(leftPage.Queues) != 1 {
		t.Fatalf("left queues=%+v err=%v", leftPage, err)
	}
	rightPage, err := right.ListQueues(queue.ListQueuesCommand{Limit: 10})
	if err != nil || len(rightPage.Queues) != 1 {
		t.Fatalf("right queues=%+v err=%v", rightPage, err)
	}
}

func TestManagerCatalogRevisionAndLegacyRouteAreStable(t *testing.T) {
	first, err := New(Config{DefaultShard: "a", Repositories: map[string]queue.Repository{"b": queue.NewMemoryRepository(), "a": queue.NewMemoryRepository()}})
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := New(Config{DefaultShard: "a", Repositories: map[string]queue.Repository{"a": queue.NewMemoryRepository(), "b": queue.NewMemoryRepository()}})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if first.catalogRevision != second.catalogRevision || first.shardIDForKey("legacy") != "a" {
		t.Fatalf("revisions %q/%q legacy=%q", first.catalogRevision, second.catalogRevision, first.shardIDForKey("legacy"))
	}
}

func TestPlacementChangesPreserveFailureDomainQuorumSafety(t *testing.T) {
	repository := &adminMemory{routingMemory: &routingMemory{MemoryRepository: queue.NewMemoryRepository(), leader: true}, members: []queue.ClusterMember{{ID: "n1", Address: "a1", Suffrage: "Voter"}, {ID: "n2", Address: "a2", Suffrage: "Voter"}, {ID: "n3", Address: "a3", Suffrage: "Voter"}}}
	placements := map[string]map[string]Placement{"s0": {"n1": {Address: "a1", FailureDomain: "az-a"}, "n2": {Address: "a2", FailureDomain: "az-b"}, "n3": {Address: "a3", FailureDomain: "az-c"}, "n4": {Address: "a4", FailureDomain: "az-a"}}}
	manager, err := New(Config{DefaultShard: "s0", Repositories: map[string]queue.Repository{"s0": repository}, Placements: placements})
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	if err := manager.AddShardNonvoter("s0", "n4", "wrong"); err == nil {
		t.Fatal("unplanned address accepted")
	}
	if err := manager.AddShardNonvoter("s0", "n4", "a4"); err != nil {
		t.Fatal(err)
	}
	if err := manager.AddShardVoter("s0", "n4", "a4"); err != nil {
		t.Fatal(err)
	}
	if err := manager.RemoveShardServer("s0", "n2"); err == nil {
		t.Fatal("removal placing quorum in one failure domain was accepted")
	}
	if err := manager.RemoveShardServer("s0", "n4"); err != nil {
		t.Fatalf("safe replacement rollback: %v", err)
	}
}
