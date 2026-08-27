package shard

import (
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"simq/internal/cluster"
	"simq/internal/queue"
	"simq/internal/topology"
)

type ClusterNodeConfig struct {
	NodeID                                string
	DataDir                               string
	OpenTimeout                           time.Duration
	TLSCertificateFile, TLSPrivateKeyFile string
	TLSCAFile, TLSServerName              string
}

func OpenClustered(manifest Manifest, config ClusterNodeConfig) (*Manager, error) {
	repositories := make(map[string]queue.Repository, len(manifest.Shards))
	placements := make(map[string]map[string]Placement, len(manifest.Shards))
	catalog := topology.Catalog{Version: topology.RecordVersion, ClusterID: manifest.Revision, Generation: 1, DefaultShard: manifest.DefaultShard, DirectoryMode: topology.DirectoryLegacy}
	for _, shard := range manifest.Shards {
		local, ok := manifest.LocalReplica(shard, config.NodeID)
		if !ok {
			if shard.ID == manifest.DefaultShard {
				return nil, fmt.Errorf("local node is absent from default shard %q", shard.ID)
			}
		}
		apiURLs := make(map[string]string, len(shard.Replicas))
		initialVoters := make(map[string]string, len(shard.Replicas))
		explicitInitialVoters := false
		for _, replica := range shard.Replicas {
			explicitInitialVoters = explicitInitialVoters || replica.InitialVoter
		}
		placements[shard.ID] = make(map[string]Placement, len(shard.Replicas))
		entryURL := ""
		for _, replica := range shard.Replicas {
			apiURLs[replica.NodeID] = replica.APIURL
			if !explicitInitialVoters || replica.InitialVoter {
				initialVoters[replica.NodeID] = replica.RaftAddress
			}
			placements[shard.ID][replica.NodeID] = Placement{Address: replica.RaftAddress, FailureDomain: replica.FailureDomain}
			if replica.NodeID == shard.BootstrapNode {
				entryURL = replica.APIURL
			}
		}
		catalog.Shards = append(catalog.Shards, topology.Shard{ID: shard.ID, Incarnation: shard.Incarnation, State: shard.InitialState, EntryAPIURL: entryURL, Initial: shard.InitialState == topology.ShardReady})
		if !ok {
			continue
		}
		repository, err := cluster.Open(cluster.Config{
			NodeID:             config.NodeID,
			BindAddress:        local.RaftAddress,
			AdvertiseAddress:   local.RaftAddress,
			DataDir:            filepath.Join(config.DataDir, "shards", shard.ID),
			Bootstrap:          config.NodeID == shard.BootstrapNode,
			OpenTimeout:        config.OpenTimeout,
			APIURLs:            apiURLs,
			InitialVoters:      initialVoters,
			TLSCertificateFile: config.TLSCertificateFile,
			TLSPrivateKeyFile:  config.TLSPrivateKeyFile,
			TLSCAFile:          config.TLSCAFile,
			TLSServerName:      config.TLSServerName,
			CatalogRevision:    manifest.Revision,
		})
		if err != nil {
			closeErrors := []error{fmt.Errorf("open shard %s: %w", shard.ID, err)}
			for _, opened := range repositories {
				closeErrors = append(closeErrors, opened.Close())
			}
			return nil, errors.Join(closeErrors...)
		}
		repositories[shard.ID] = repository
	}
	return New(Config{DefaultShard: manifest.DefaultShard, CatalogRevision: manifest.Revision, Repositories: repositories, Placements: placements, TopologyCatalog: &catalog})
}
