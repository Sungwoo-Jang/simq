# M5 cluster operations

Cluster mode is opt-in and uses one Raft group for all queues. Run an odd number
of voters; three voters tolerate one unavailable voter. Queue HTTP traffic is
served only by the current leader. M6 OIDC mode serves the API with TLS 1.3 and
can protect Raft with mutual TLS; disabled mode retains the M5 plaintext
compatibility behavior.

For M7 fixed-catalog multi-Raft deployments, use
[`shard-operations.md`](shard-operations.md). The configuration and membership
procedures below remain the single-group M5 compatibility path.

## Node configuration

- `SIMQ_CLUSTER_NODE_ID`: stable, unique Raft server ID; setting it enables M5.
- `SIMQ_CLUSTER_BIND`: local Raft TCP bind address.
- `SIMQ_CLUSTER_ADVERTISE`: reachable Raft address; defaults to the bind address.
- `SIMQ_CLUSTER_DATA_DIR`: node-local directory containing `fsm.db`, `raft.db`,
  and `snapshots/`; it must never be shared by nodes.
- `SIMQ_CLUSTER_BOOTSTRAP=true`: create a cluster only when this node has no
  existing Raft state. Use it on exactly one node for first formation.
- `SIMQ_CLUSTER_INITIAL_VOTERS`: comma-separated `node=raft-address` entries.
  The bootstrapping node and its exact advertised address must be included.
- `SIMQ_CLUSTER_API_URLS`: comma-separated `node=https://api-url` entries used
  for `X-SimQ-Leader` hints.
- `SIMQ_CLUSTER_ADMIN_TOKEN`: enables the cluster administration endpoints.
  If unset, those endpoints are not registered.
- `SIMQ_CLUSTER_TLS_CERT_FILE`, `SIMQ_CLUSTER_TLS_KEY_FILE`, and
  `SIMQ_CLUSTER_TLS_CA_FILE`: optional, all-or-nothing mutual TLS identity and
  trust for Raft traffic.
- `SIMQ_CLUSTER_TLS_SERVER_NAME`: optional common certificate DNS identity;
  otherwise each advertised host is verified.

Start the non-bootstrapping nodes first, then the one bootstrapping node with
the same initial voter set. Once Raft state exists, restart every node with
bootstrap disabled; stored membership remains authoritative.

## Requests and leader changes

Every clustered mutating SQS request must include a client-chosen
`X-SimQ-Operation-Id`. Reuse it, with the exact same request, after a timeout or
leader change. A follower returns `503 NotLeader` and, when configured,
`X-SimQ-Leader`. Reads also fail closed on followers and minority partitions.

## Membership and snapshots

The following endpoints require `Authorization: Bearer <admin-token>` and are
leader-only:

- `GET /v1/cluster/members`
- `GET /v1/cluster/protocol`
- `POST /v1/cluster/members` with
  `{"Action":"add-nonvoter|add-voter|demote|remove","NodeId":"...","RaftAddress":"...","MinCommandVersion":2,"MaxCommandVersion":2}`.
  The version fields are required for add operations and must overlap version 2.
- `POST /v1/cluster/snapshot`

For replacement or expansion, add a nonvoter, wait until it is caught up, then
promote it with `add-voter`. Preserve an odd voter count and quorum throughout.
Demote or remove only after the replacement is a voter. SimQ delegates every
configuration change to HashiCorp Raft and never edits peer state directly.

M8 command protocol version 2 is the only supported clustered command version.
All nodes in an M8 cluster therefore need a build supporting version 2. A future
release must advertise an overlapping command-version range before mixed-version
rolling upgrades can be allowed.
