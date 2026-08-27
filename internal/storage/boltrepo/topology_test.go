package boltrepo

import (
	"path/filepath"
	"sync"
	"testing"

	"simq/internal/ownership"
	"simq/internal/topology"
)

func TestConcurrentFirstUseCommitsOneExplicitOwner(t *testing.T) {
	repository, err := Open(Config{Path: filepath.Join(t.TempDir(), "simq.db")})
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	catalog := topology.Catalog{Version: topology.RecordVersion, ClusterID: "00112233445566778899aabbccddeeff", Generation: 1, DefaultShard: "s0", DirectoryMode: topology.DirectoryExplicit, BackfillComplete: true, Shards: []topology.Shard{
		{ID: "s0", Incarnation: "11112222333344445555666677778888", State: topology.ShardReady, EntryAPIURL: "https://s0.example", Initial: true},
		{ID: "s1", Incarnation: "9999aaaabbbbccccddddeeeeffff0000", State: topology.ShardReady, EntryAPIURL: "https://s1.example", Initial: true},
	}}
	if _, err := repository.InitializeCatalog(catalog); err != nil {
		t.Fatal(err)
	}
	digest := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	const workers = 32
	results := make(chan ownership.Record, workers)
	errors := make(chan error, workers)
	var wait sync.WaitGroup
	for index := 0; index < workers; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			record, assignErr := repository.EnsureAssignment(digest)
			results <- record
			errors <- assignErr
		}()
	}
	wait.Wait()
	close(results)
	close(errors)
	for assignErr := range errors {
		if assignErr != nil {
			t.Fatal(assignErr)
		}
	}
	var first ownership.Record
	for record := range results {
		if first.Version == 0 {
			first = record
		}
		if record != first {
			t.Fatalf("owners differ: first=%+v next=%+v", first, record)
		}
	}
	stored, found, err := repository.LocalOwnership(digest)
	if err != nil || !found || stored != first || stored.Epoch != 1 {
		t.Fatalf("stored=%+v found=%v err=%v", stored, found, err)
	}
}
