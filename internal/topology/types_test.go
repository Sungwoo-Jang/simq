package topology

import (
	"testing"
)

func testCatalog() Catalog {
	return Catalog{Version: RecordVersion, ClusterID: "00112233445566778899aabbccddeeff", Generation: 1, DefaultShard: "s0", DirectoryMode: DirectoryExplicit, BackfillComplete: true, Shards: []Shard{
		{ID: "s0", Incarnation: "11112222333344445555666677778888", State: ShardReady, EntryAPIURL: "https://n1.example", Initial: true},
		{ID: "s1", Incarnation: "9999aaaabbbbccccddddeeeeffff0000", State: ShardReady, EntryAPIURL: "https://n2.example", Initial: true},
		{ID: "s2", Incarnation: "abcdefabcdefabcdefabcdefabcdefab", State: ShardCandidate, EntryAPIURL: "https://n3.example"},
	}}
}

func TestSelectShardIgnoresCandidateAndPreservesLegacySet(t *testing.T) {
	catalog := testCatalog()
	digest := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	legacy, err := SelectShard(digest, catalog, true, "")
	if err != nil || legacy == "s2" {
		t.Fatalf("legacy=%q err=%v", legacy, err)
	}
	catalog.Shards[2].State = ShardReady
	explicit, err := SelectShard(digest, catalog, false, "")
	if err != nil {
		t.Fatal(err)
	}
	if explicit == "" {
		t.Fatal("explicit selection is empty")
	}
	legacyAgain, err := SelectShard(digest, catalog, true, "")
	if err != nil || legacyAgain != legacy {
		t.Fatalf("legacy changed from %q to %q: %v", legacy, legacyAgain, err)
	}
}

func TestCatalogRejectsRetiredDefaultAndMalformedEntryURL(t *testing.T) {
	catalog := testCatalog()
	catalog.Shards[0].State = ShardRetired
	if catalog.Canonicalize() == nil {
		t.Fatal("retired default shard was accepted")
	}
	catalog = testCatalog()
	catalog.Shards[1].EntryAPIURL = "https://user:secret@n2.example"
	if catalog.Canonicalize() == nil {
		t.Fatal("userinfo-bearing entry URL was accepted")
	}
}
