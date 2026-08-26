package shard

import (
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"simq/internal/queue"
	"simq/internal/tenant"
)

func shardFreeAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	return address
}
func waitForShard(t *testing.T, description string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", description)
}

func TestThreeNodesHostTwoIndependentRaftShardsAndFailOver(t *testing.T) {
	root := t.TempDir()
	nodeIDs := []string{"n1", "n2", "n3"}
	shardIDs := []string{"s0", "s1"}
	manifest := Manifest{Version: 1, DefaultShard: "s0"}
	for shardIndex, shardID := range shardIDs {
		entry := ShardManifest{ID: shardID, BootstrapNode: "n1"}
		for nodeIndex, nodeID := range nodeIDs {
			entry.Replicas = append(entry.Replicas, ReplicaManifest{NodeID: nodeID, RaftAddress: shardFreeAddress(t), APIURL: fmt.Sprintf("https://%s.example", nodeID), FailureDomain: fmt.Sprintf("az-%d", nodeIndex+1), InitialVoter: true})
		}
		manifest.Shards = append(manifest.Shards, entry)
		_ = shardIndex
	}
	if err := manifest.Validate("n1"); err != nil {
		t.Fatal(err)
	}
	nodes := make([]*Manager, 3)
	// Start followers first so the two static bootstrap configurations can
	// immediately contact a quorum when n1 starts.
	for _, index := range []int{1, 2, 0} {
		manager, err := OpenClustered(manifest, ClusterNodeConfig{NodeID: nodeIDs[index], DataDir: filepath.Join(root, nodeIDs[index]), OpenTimeout: time.Second})
		if err != nil {
			t.Fatalf("open %s: %v", nodeIDs[index], err)
		}
		nodes[index] = manager
	}
	defer func() {
		for _, node := range nodes {
			if node != nil {
				_ = node.Close()
			}
		}
	}()
	waitForShard(t, "leaders for both shards", func() bool {
		leaders := map[string]int{}
		for _, node := range nodes {
			for _, status := range node.ShardStatuses() {
				if status.Leader {
					leaders[status.ID]++
				}
			}
		}
		return leaders["s0"] == 1 && leaders["s1"] == 1
	})

	tenantByShard := map[string]string{}
	for index := 0; len(tenantByShard) < 2 && index < 1000; index++ {
		id := fmt.Sprintf("tenant-%d", index)
		tenantByShard[nodes[0].shardIDForKey(namespaceForTenant(id))] = id
	}
	queues := make(map[string]queue.Queue)
	for shardID, tenantID := range tenantByShard {
		leaderIndex := -1
		for index, node := range nodes {
			for _, status := range node.ShardStatuses() {
				if status.ID == shardID && status.Leader {
					leaderIndex = index
				}
			}
		}
		if leaderIndex < 0 {
			t.Fatalf("no leader for %s", shardID)
		}
		messageIndex := 0
		service := queue.NewService(tenant.New(nodes[leaderIndex], nil), queue.WithQueueIDGenerator(func() string { return fmt.Sprintf("q_%032x", len(shardID)) }), queue.WithMessageIDGenerator(func() string { messageIndex++; return fmt.Sprintf("m_%032x", messageIndex) })).WithTenant(tenantID)
		created, err := service.WithOperationID("create-" + shardID).CreateQueue(queue.CreateQueueInput{QueueName: "orders.fifo", Attributes: map[string]string{"FifoQueue": "true"}})
		if err != nil {
			t.Fatalf("create on %s: %v", shardID, err)
		}
		for index, body := range []string{"first", "second"} {
			_, err := service.WithOperationID(fmt.Sprintf("send-%s-%d", shardID, index)).SendMessage(queue.SendMessageInput{QueueName: created.Name, QueueID: created.ID, MessageBody: body, MessageGroupID: "group", MessageDeduplicationID: body})
			if err != nil {
				t.Fatalf("send on %s: %v", shardID, err)
			}
		}
		queues[shardID] = created
	}

	failedIndex := -1
	for index, node := range nodes {
		for _, status := range node.ShardStatuses() {
			if status.ID == "s0" && status.Leader {
				failedIndex = index
			}
		}
	}
	if failedIndex < 0 {
		t.Fatal("s0 leader missing")
	}
	if err := nodes[failedIndex].Close(); err != nil {
		t.Fatal(err)
	}
	nodes[failedIndex] = nil
	waitForShard(t, "both shards after node loss", func() bool {
		leaders := map[string]int{}
		for _, node := range nodes {
			if node == nil {
				continue
			}
			for _, status := range node.ShardStatuses() {
				if status.Leader {
					leaders[status.ID]++
				}
			}
		}
		return leaders["s0"] == 1 && leaders["s1"] == 1
	})
	for shardID, tenantID := range tenantByShard {
		for _, node := range nodes {
			if node == nil {
				continue
			}
			leader := false
			for _, status := range node.ShardStatuses() {
				leader = leader || status.ID == shardID && status.Leader
			}
			if !leader {
				continue
			}
			receiptIndex := 0
			service := queue.NewService(tenant.New(node, nil), queue.WithReceiptHandleGenerator(func() string { receiptIndex++; return fmt.Sprintf("rh_%032x", receiptIndex) })).WithTenant(tenantID)
			value, err := service.GetQueue(queue.GetQueueInput{QueueName: "orders.fifo"})
			if err != nil || value.ID != queues[shardID].ID {
				t.Fatalf("recovered %s queue=%+v err=%v", shardID, value, err)
			}
			one := 1
			for index, want := range []string{"first", "second"} {
				messages, err := service.WithOperationID(fmt.Sprintf("receive-%s-%d", shardID, index)).ReceiveMessage(queue.ReceiveMessageInput{QueueName: value.Name, QueueID: value.ID, MaxNumberOfMessages: &one})
				if err != nil || len(messages) != 1 || messages[0].Body != want {
					t.Fatalf("%s FIFO receive %d=%+v err=%v", shardID, index, messages, err)
				}
				if err := service.WithOperationID(fmt.Sprintf("delete-%s-%d", shardID, index)).DeleteMessage(queue.DeleteMessageInput{QueueName: value.Name, QueueID: value.ID, ReceiptHandle: messages[0].ReceiptHandle}); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
}
