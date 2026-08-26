# M7 shard operations

M7 is enabled by setting `SIMQ_SHARD_MANIFEST` on every node. It requires OIDC
security mode, bbolt storage, a stable `SIMQ_CLUSTER_NODE_ID`, the M6 encryption
key ring and HTTPS settings, and `SIMQ_CLUSTER_ADMIN_TOKEN`. Each listed Raft
address must be unique across all shards.

## Manifest

All nodes use the same fixed catalog and a placement plan that includes the
local node in every shard they host. This example uses three failure domains:

```json
{
  "version": 1,
  "default_shard": "s0",
  "shards": [
    {
      "id": "s0",
      "bootstrap_node": "n1",
      "replicas": [
        {"node_id":"n1","raft_address":"10.0.1.10:7100","api_url":"https://n1.example:9324","failure_domain":"az-a","initial_voter":true},
        {"node_id":"n2","raft_address":"10.0.2.10:7100","api_url":"https://n2.example:9324","failure_domain":"az-b","initial_voter":true},
        {"node_id":"n3","raft_address":"10.0.3.10:7100","api_url":"https://n3.example:9324","failure_domain":"az-c","initial_voter":true}
      ]
    },
    {
      "id": "s1",
      "bootstrap_node": "n1",
      "replicas": [
        {"node_id":"n1","raft_address":"10.0.1.10:7200","api_url":"https://n1.example:9324","failure_domain":"az-a","initial_voter":true},
        {"node_id":"n2","raft_address":"10.0.2.10:7200","api_url":"https://n2.example:9324","failure_domain":"az-b","initial_voter":true},
        {"node_id":"n3","raft_address":"10.0.3.10:7200","api_url":"https://n3.example:9324","failure_domain":"az-c","initial_voter":true}
      ]
    }
  ]
}
```

Start non-bootstrap nodes first, then each shard's bootstrap node. Readiness
becomes healthy after all local shard repositories open. Inspect the resulting
leader and catalog revision with `GET /v1/cluster/shards` using the cluster
administrator token.

## Routing and ordering

The verified tenant claim is hashed to one shard before leader routing. A
follower returns the API URL of that selected shard's leader. All queues and
dependent state for the tenant remain within that shard, so FIFO order and
queue/redrive/quota transactions retain their M1-M6 meaning.

## Administrative endpoints

- `GET /v1/cluster/shards` lists bounded shard status and leader URLs.
- `GET /v1/cluster/shards/members?ShardId=s0` lists one shard's Raft members.
- `POST /v1/cluster/shards/members` applies `add-nonvoter`, `add-voter`,
  `demote-voter`, or `remove-server` to the named shard.
- `POST /v1/cluster/shards/snapshot` triggers a snapshot for the named shard.

The membership body includes `ShardId`, `Action`, `ID`, and `Address`. The node
and address must exist in the manifest. Changes that leave fewer than three
voters or allow one failure domain to contain a quorum are rejected.

## Online replica relocation

Relocation moves a whole shard replica, never an individual tenant:

1. Add the candidate replica to the manifest with `initial_voter:false`, while
   retaining every current replica. The catalog revision remains unchanged.
2. Roll the placement-only manifest to current nodes and start the candidate.
3. Add the candidate as a nonvoter for each shard and wait until it catches up.
4. Promote the candidate to voter.
5. Demote or remove the old voter only after the placement safety check passes.
6. In a later rollout, mark the promoted candidate as an initial voter and
   remove the retired replica from the placement manifest, retaining an odd
   initial-voter set of at least three for disaster re-formation checks.

Do not add or remove shard IDs or change `default_shard` in this procedure.
Those edits change the catalog revision and existing databases reject them.

## Backup and recovery

Trigger and inventory snapshots per shard. A recoverable deployment also needs
the exact fixed catalog, placement manifest, payload encryption key ring, TLS
identity, and cluster operator secret. Restore a snapshot only into the same
catalog revision; the FSM validates it before publication. Prefer replacing a
lost replica by adding a planned nonvoter and letting Raft catch it up from the
surviving quorum.

M7 does not implement online tenant resharding. A future implementation must
use durable ownership epochs, source fencing, verified copy, atomic cutover,
and rollback; operators must not copy tenant buckets manually.
