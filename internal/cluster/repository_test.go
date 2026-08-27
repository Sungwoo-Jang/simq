package cluster

import (
	"errors"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"simq/internal/queue"
)

func freeAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	return address
}
func waitFor(t *testing.T, description string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", description)
}
func openNode(t *testing.T, root, id, address string, bootstrap bool) *Repository {
	t.Helper()
	repository, err := Open(Config{NodeID: id, BindAddress: address, AdvertiseAddress: address, DataDir: filepath.Join(root, id), Bootstrap: bootstrap, ApplyTimeout: 2 * time.Second, ReadTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return repository
}

func TestThreeNodeQuorumFailoverAndMinorityFencing(t *testing.T) {
	root := t.TempDir()
	addresses := []string{freeAddress(t), freeAddress(t), freeAddress(t)}
	nodes := []*Repository{openNode(t, root, "n1", addresses[0], true), openNode(t, root, "n2", addresses[1], false), openNode(t, root, "n3", addresses[2], false)}
	defer func() {
		for _, node := range nodes {
			_ = node.Close()
		}
	}()
	waitFor(t, "bootstrap leader", func() bool { return nodes[0].State() == raft.Leader })
	if err := nodes[0].AddVoter("n2", addresses[1]); err != nil {
		t.Fatal(err)
	}
	if err := nodes[0].AddVoter("n3", addresses[2]); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "three voters", func() bool {
		configuration, err := nodes[0].Configuration()
		return err == nil && len(configuration.Servers) == 3
	})
	candidate := queue.Queue{ID: "q_00000000000000000000000000000001", Name: "orders.fifo", VisibilityTimeout: 30, MessageRetentionPeriod: queue.DefaultMessageRetentionPeriod, FIFO: true}
	created, err := nodes[0].ForOperation("create-orders").Create(candidate)
	if err != nil {
		t.Fatal(err)
	}
	if created.Name != "orders.fifo" {
		t.Fatalf("created queue = %+v", created)
	}
	ids := []string{"message-1", "message-2"}
	nextID := 0
	service := queue.NewService(nodes[0].ForOperation("ordered-send"), queue.WithMessageIDGenerator(func() string { value := ids[nextID]; nextID++; return value }))
	for _, body := range []string{"first", "second"} {
		if _, err := service.SendMessage(queue.SendMessageInput{QueueName: created.Name, QueueID: created.ID, MessageBody: body, MessageGroupID: "group-1", MessageDeduplicationID: body}); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, "followers apply queue", func() bool { return nodes[1].core.local.Health() == nil && nodes[2].core.local.Health() == nil })
	if err := nodes[0].Close(); err != nil {
		t.Fatal(err)
	}
	var leader *Repository
	waitFor(t, "failover leader", func() bool {
		for _, node := range nodes[1:] {
			if node.State() == raft.Leader {
				leader = node
				return true
			}
		}
		return false
	})
	replayed, err := leader.ForOperation("create-orders").Create(candidate)
	if err != nil {
		t.Fatal(err)
	}
	if replayed.Name != "orders.fifo" {
		t.Fatalf("replayed queue = %+v", replayed)
	}
	receipts := []string{"rh_00000000000000000000000000000001", "rh_00000000000000000000000000000002"}
	nextReceipt := 0
	one := 1
	failoverService := queue.NewService(leader.ForOperation("ordered-receive"), queue.WithReceiptHandleGenerator(func() string { value := receipts[nextReceipt]; nextReceipt++; return value }))
	first, err := failoverService.ReceiveMessage(queue.ReceiveMessageInput{QueueName: created.Name, QueueID: created.ID, MaxNumberOfMessages: &one})
	if err != nil || len(first) != 1 || first[0].Body != "first" {
		t.Fatalf("first FIFO receive after failover = %+v, %v", first, err)
	}
	if err := failoverService.DeleteMessage(queue.DeleteMessageInput{QueueName: created.Name, QueueID: created.ID, ReceiptHandle: first[0].ReceiptHandle}); err != nil {
		t.Fatal(err)
	}
	second, err := failoverService.ReceiveMessage(queue.ReceiveMessageInput{QueueName: created.Name, QueueID: created.ID, MaxNumberOfMessages: &one})
	if err != nil || len(second) != 1 || second[0].Body != "second" {
		t.Fatalf("second FIFO receive after failover = %+v, %v", second, err)
	}
	var follower *Repository
	if leader == nodes[1] {
		follower = nodes[2]
	} else {
		follower = nodes[1]
	}
	if _, _, err := follower.Get("orders.fifo"); !errors.Is(err, queue.ErrRepositoryUnavailable) {
		t.Fatalf("follower read error = %v", err)
	}
	if err := follower.Close(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "leader loses quorum", func() bool {
		_, err := leader.ForOperation("no-quorum").Create(queue.Queue{ID: "q_00000000000000000000000000000002", Name: "blocked", VisibilityTimeout: 30, MessageRetentionPeriod: queue.DefaultMessageRetentionPeriod})
		return errors.Is(err, queue.ErrRepositoryUnavailable)
	})
}

func TestDurableRaftAndFSMRestart(t *testing.T) {
	root := t.TempDir()
	address := freeAddress(t)
	node := openNode(t, root, "n1", address, true)
	waitFor(t, "leader", func() bool { return node.State() == raft.Leader })
	candidate := queue.Queue{ID: "q_00000000000000000000000000000001", Name: "orders", VisibilityTimeout: 30, MessageRetentionPeriod: queue.DefaultMessageRetentionPeriod}
	if _, err := node.ForOperation("create-orders").Create(candidate); err != nil {
		t.Fatal(err)
	}
	if err := node.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	node = openNode(t, root, "n1", address, false)
	defer node.Close()
	waitFor(t, "restarted leader", func() bool { return node.State() == raft.Leader })
	if value, found, err := node.Get("orders"); err != nil || !found || value.Name != "orders" {
		t.Fatalf("restored queue = %+v found=%v err=%v", value, found, err)
	}
}

func TestBootstrapRetainsStableDNSAdvertiseAddress(t *testing.T) {
	address := freeAddress(t)
	_, port, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatal(err)
	}
	advertise := net.JoinHostPort("localhost", port)
	node, err := Open(Config{
		NodeID:           "n1",
		BindAddress:      address,
		AdvertiseAddress: advertise,
		DataDir:          t.TempDir(),
		Bootstrap:        true,
		InitialVoters:    map[string]string{"n1": advertise},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	waitFor(t, "DNS-advertised leader", func() bool { return node.State() == raft.Leader })
	configuration, err := node.Configuration()
	if err != nil {
		t.Fatal(err)
	}
	if len(configuration.Servers) != 1 || configuration.Servers[0].Address != raft.ServerAddress(advertise) {
		t.Fatalf("configuration=%+v want address %q", configuration, advertise)
	}
}

func TestReplicatedCommandCarriesLeaderStorageQuota(t *testing.T) {
	root := t.TempDir()
	address := freeAddress(t)
	node := openNode(t, root, "n1", address, true)
	defer node.Close()
	waitFor(t, "leader", func() bool { return node.State() == raft.Leader })

	node.SetStorageQuota(queue.StorageQuota{MaxQueues: 2, MaxMessages: 10, MaxPayloadBytes: 1024})
	// Simulate a follower that has a stale local environment value. Apply must
	// replace it with the value carried by the leader's command.
	node.core.local.SetStorageQuota(queue.StorageQuota{MaxQueues: 1, MaxMessages: 10, MaxPayloadBytes: 1024})
	ids := []string{"q_00000000000000000000000000000001", "q_00000000000000000000000000000002"}
	for index, name := range []string{"one", "two"} {
		candidate := queue.Queue{ID: ids[index], Name: name, VisibilityTimeout: 30, MessageRetentionPeriod: queue.DefaultMessageRetentionPeriod}
		if _, err := node.ForOperation("create-" + name).Create(candidate); err != nil {
			t.Fatalf("create %s with leader quota: %v", name, err)
		}
	}

	node.SetStorageQuota(queue.StorageQuota{MaxQueues: 2, MaxMessages: 10, MaxPayloadBytes: 1024})
	node.core.local.SetStorageQuota(queue.StorageQuota{MaxQueues: 100, MaxMessages: 100, MaxPayloadBytes: 1 << 20})
	third := queue.Queue{ID: "q_00000000000000000000000000000003", Name: "three", VisibilityTimeout: 30, MessageRetentionPeriod: queue.DefaultMessageRetentionPeriod}
	if _, err := node.ForOperation("create-three").Create(third); !errors.Is(err, queue.ErrQuotaExceeded) {
		t.Fatalf("third create error=%v, want quota exceeded", err)
	}
}
