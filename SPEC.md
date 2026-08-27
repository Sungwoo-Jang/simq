# SimQ Specification

Status: Draft

Version: 0.2

Last updated: 2026-08-24

## 1. Purpose

SimQ is an open-source queue service written in Go. It provides queue
operations corresponding to Amazon SQS API action names through a simple JSON
HTTP API.

SimQ aims to reproduce the observable queue behavior of SQS, including
visibility timeout, delayed delivery, long polling, message retention, batch
operations, dead-letter queues, FIFO ordering, and deduplication. It does not
aim to be a wire-compatible replacement for AWS SQS.

## 2. Compatibility boundary

### 2.1 In scope

- HTTP endpoints whose final path component matches an SQS API action name.
- JSON request and response bodies using familiar SQS field names where useful.
- Standard and FIFO queue behavior.
- Queue and message attributes required for SQS-like behavior.
- Batch requests with per-entry success and failure results.
- Dead-letter queues and message redrive.
- A future durable, highly available clustered deployment mode.

### 2.2 Out of scope

- AWS CLI compatibility.
- AWS SDK compatibility.
- AWS JSON 1.0 and `X-Amz-Target` dispatch.
- AWS Query protocol and XML responses.
- AWS Signature Version 4.
- IAM, AWS account IDs, ARNs, resource policies, and cross-account access.
- AWS KMS integration.
- CloudWatch integration.
- Exact reproduction of AWS regional endpoints, quotas, throttling, or billing.

SimQ may add its own authentication, tenant isolation, encryption, metrics, and
quota mechanisms in a later productization phase. Those mechanisms are not part
of SQS protocol compatibility.

## 3. Normative language

The terms MUST, MUST NOT, SHOULD, SHOULD NOT, and MAY describe requirement
levels in this document.

## 4. HTTP API convention

### 4.1 Base path and action routing

All queue actions use the following route:

```text
POST /v1/sqs/{ActionName}
```

`ActionName` is case-sensitive and matches the corresponding SQS API action
name. Examples:

```text
POST /v1/sqs/CreateQueue
POST /v1/sqs/SendMessage
POST /v1/sqs/ReceiveMessage
POST /v1/sqs/DeleteMessage
```

All action requests and responses use:

```text
Content-Type: application/json
```

SimQ does not inspect `X-Amz-Target`, AWS credentials, or AWS signature headers.
Unknown action names return HTTP 404.

### 4.2 JSON field convention

- Public JSON field names use UpperCamelCase, such as `QueueUrl` and
  `MessageBody`.
- Unknown request fields MUST cause an `InvalidRequest` error during the initial
  development phase. This prevents misspelled fields from being silently
  ignored.
- Missing optional fields use the defaults defined by the relevant action.
- Identifiers and receipt handles are opaque, case-sensitive strings.
- Time duration inputs are integer seconds unless a field explicitly states
  otherwise.
- Timestamps returned as message system attributes are Unix epoch milliseconds
  encoded as decimal strings.

### 4.3 Queue URL

`CreateQueue` and `GetQueueUrl` return a stable, generation-bound queue
resource identifier:

```text
{PublicBaseURL}/queues/{QueueName}/{QueueId}
```

For example:

```text
http://localhost:9324/queues/orders/q_0123456789abcdef0123456789abcdef
```

The server MUST use a configured `PublicBaseURL` when constructing queue URLs.
It MUST NOT derive externally visible queue URLs solely from an untrusted `Host`
header.

`QueueUrl` is an opaque queue identifier carried in JSON request bodies. Queue
actions are still sent to `/v1/sqs/{ActionName}`; clients do not POST directly
to the queue URL.

`QueueId` is an opaque 34-character value beginning with `q_`. It is stable for
one live queue generation and changes if the same queue name is deleted and
  later recreated. A syntactically valid URL for an older generation behaves as
  a missing queue. For transition compatibility, a name-only URL may resolve a
  first live generation when the name has no deletion history, but is
  permanently invalid after that name is deleted and recreated.

### 4.4 Successful response

Every successful response MUST include an opaque request ID in both the header
and JSON body:

```http
HTTP/1.1 200 OK
Content-Type: application/json
X-SimQ-Request-Id: 01JEXAMPLE

{
  "RequestId": "01JEXAMPLE"
}
```

Action-specific fields are added alongside `RequestId`.

### 4.5 Error response

Errors use a stable JSON envelope:

```json
{
  "Error": {
    "Code": "QueueDoesNotExist",
    "Message": "The specified queue does not exist."
  },
  "RequestId": "01JEXAMPLE"
}
```

Initial HTTP status mapping:

| Status | Meaning |
|---|---|
| `400` | Invalid input or invalid operation for the queue type |
| `404` | Unknown action or queue not found |
| `409` | Resource state conflict |
| `413` | Message or request body too large |
| `429` | SimQ quota or rate limit exceeded |
| `500` | Internal failure |
| `503` | Service unavailable or durable commit unavailable |

Error codes are part of the public API and MUST remain stable after a release.

### 4.6 Request size and cancellation

- An individual message body MUST be between 1 byte and 1 MiB when encoded as
  UTF-8 bytes.
- The server MUST enforce a configurable maximum HTTP body size.
- Client cancellation MAY stop waiting for a response, but it does not prove
  that a mutating operation was not applied.
- Mutating operations will later support an explicit idempotency mechanism for
  safe retry after an ambiguous timeout.

## 5. Queue types and delivery contract

### 5.1 Standard queues

Standard queues provide at-least-once delivery and do not promise strict
ordering. Consumers MUST be able to process a message more than once.

SimQ MUST NOT intentionally duplicate messages merely to demonstrate
at-least-once delivery. Duplicate delivery may occur after retries, ambiguous
responses, visibility expiration, or failures.

### 5.2 FIFO queues

FIFO queues provide:

- Ordering within each `MessageGroupId`.
- Concurrent progress across independent message groups.
- `MessageDeduplicationId` support.
- Content-based deduplication.
- A five-minute send deduplication window.
- A new receipt handle for every successful receive.

The detailed M4 contract in section 12 defines validation, sequencing,
deduplication, receive-attempt replay, and transfer behavior.

## 6. Message lifecycle

A message moves through the following logical states:

```text
delayed -> visible -> in-flight -> deleted
              ^           |
              |-----------|
          visibility timeout
```

A non-delayed message enters `visible` immediately after a successful send. A
delayed message becomes visible when `AvailableAt` is reached. Receiving a
message atomically moves it from `visible` to `in-flight` and creates a
visibility deadline. Deleting it with its current receipt handle moves it to
`deleted`. If it is not deleted before the deadline, it becomes visible again.

Messages that reach their retention deadline are removed regardless of their
current visibility state.

## 7. Non-negotiable behavioral requirements

1. `ReceiveMessage` is a state-changing claim operation, not a read-only peek.
2. Selecting a message and marking it in-flight MUST be one atomic operation.
3. A message MUST NOT simultaneously exist in both the visible and in-flight
   indexes.
4. Every successful receive MUST issue a new receipt handle.
5. A delete using the current receipt handle MUST delete the message.
6. A structurally valid stale receipt handle MAY return success as a no-op, but
   MUST NOT delete a newer receive generation.
7. An unexpired in-flight message MUST NOT be selected by another normal claim.
8. Visibility expiration MUST make an undeleted message eligible for another
   receive.
9. Message delay, visibility, and retention decisions MUST be testable without
   real-time sleeps.
10. Once durable storage is introduced, a successful `SendMessage` response
    MUST survive process restart.
11. Once clustering is introduced, a successful mutating response MUST only be
    returned after the operation is durably committed and applied.
12. The service MUST never claim exactly-once consumer processing.

## 8. First executable milestone: M0

M0 is a single-process, in-memory Standard Queue implementation. It exists to
validate the API boundary and core state transitions before adding storage or
clustering.

### 8.1 Included actions

- `CreateQueue`
- `GetQueueUrl`
- `SendMessage`
- `ReceiveMessage`
- `DeleteMessage`

### 8.2 Included behavior

- A single global queue namespace.
- Standard queues only.
- Queue-level visibility timeout.
- Per-receive visibility timeout override.
- Atomic receive claims.
- Receipt handle generation and validation.
- Up to 10 messages per receive.
- At-least-once delivery contract.
- Concurrent producer and consumer safety.
- Injectable clock for deterministic tests.

### 8.3 Explicitly excluded from M0

- Persistence and restart recovery.
- Long polling.
- Per-message or queue-level delivery delay.
- Message retention cleanup.
- Message attributes.
- Batch actions.
- FIFO queues.
- Dead-letter queues and redrive.
- Tags and permissions.
- Clustering and replication.
- Authentication and multi-tenancy.

## 9. M0 action contracts

### 9.1 CreateQueue

Endpoint:

```text
POST /v1/sqs/CreateQueue
```

Request:

```json
{
  "QueueName": "orders",
  "Attributes": {
    "VisibilityTimeout": "30"
  }
}
```

Rules:

- `QueueName` is required.
- Queue names contain 1 to 80 ASCII letters, digits, hyphens, or underscores.
- Names ending in `.fifo` are rejected until FIFO support is implemented.
- `VisibilityTimeout` is optional, defaults to 30 seconds, and accepts 0 through
  43,200 seconds.
- In M0, `VisibilityTimeout` is the only accepted queue attribute.
- Creating a queue with the same name and identical attributes is idempotent.
- Creating a queue with the same name and different attributes returns
  `QueueAlreadyExists` with HTTP 409.

Response:

```json
{
  "QueueUrl": "http://localhost:9324/queues/orders",
  "RequestId": "01JEXAMPLE"
}
```

### 9.2 GetQueueUrl

Endpoint:

```text
POST /v1/sqs/GetQueueUrl
```

Request:

```json
{
  "QueueName": "orders"
}
```

Response:

```json
{
  "QueueUrl": "http://localhost:9324/queues/orders",
  "RequestId": "01JEXAMPLE"
}
```

If the queue does not exist, the server returns `QueueDoesNotExist` with HTTP
404.

### 9.3 SendMessage

Endpoint:

```text
POST /v1/sqs/SendMessage
```

Request:

```json
{
  "QueueUrl": "http://localhost:9324/queues/orders",
  "MessageBody": "order-created"
}
```

Rules:

- `QueueUrl` and `MessageBody` are required.
- The UTF-8 encoded message body is limited to 1 MiB.
- M0 does not accept message attributes or delay fields.
- A successful response means the message is visible in the current process.
- M0 does not promise survival across process restart.

Response:

```json
{
  "MessageId": "01JEXAMPLEMESSAGE",
  "MD5OfMessageBody": "bb493e6546e1863734c792e8ea97e3ba",
  "RequestId": "01JEXAMPLE"
}
```

`MessageId` is opaque and unique within the SimQ installation.

### 9.4 ReceiveMessage

Endpoint:

```text
POST /v1/sqs/ReceiveMessage
```

Request:

```json
{
  "QueueUrl": "http://localhost:9324/queues/orders",
  "MaxNumberOfMessages": 1,
  "VisibilityTimeout": 30
}
```

Rules:

- `QueueUrl` is required.
- `MaxNumberOfMessages` defaults to 1 and accepts 1 through 10.
- `VisibilityTimeout` defaults to the queue setting and accepts 0 through
  43,200 seconds.
- M0 performs short polling only and returns immediately.
- An empty queue returns HTTP 200 with an empty `Messages` array.
- Returned messages MUST be atomically marked in-flight before the response is
  made available to the client.

Response:

```json
{
  "Messages": [
    {
      "MessageId": "01JEXAMPLEMESSAGE",
      "ReceiptHandle": "opaque-receipt-handle",
      "MD5OfBody": "bb493e6546e1863734c792e8ea97e3ba",
      "Body": "order-created",
      "Attributes": {
        "ApproximateReceiveCount": "1",
        "SentTimestamp": "1787443200000",
        "ApproximateFirstReceiveTimestamp": "1787443201000"
      }
    }
  ],
  "RequestId": "01JEXAMPLE"
}
```

### 9.5 DeleteMessage

Endpoint:

```text
POST /v1/sqs/DeleteMessage
```

Request:

```json
{
  "QueueUrl": "http://localhost:9324/queues/orders",
  "ReceiptHandle": "opaque-receipt-handle"
}
```

Rules:

- `QueueUrl` and `ReceiptHandle` are required.
- The current valid receipt handle deletes the message.
- A malformed receipt handle returns `ReceiptHandleIsInvalid` with HTTP 400.
- A well-formed but stale or already-consumed receipt handle returns HTTP 200 as
  an idempotent no-op.

Response:

```json
{
  "RequestId": "01JEXAMPLE"
}
```

## 10. M0 acceptance criteria

M0 is complete only when automated tests prove all of the following:

1. A queue can be created and retrieved by name.
2. Repeating `CreateQueue` with the same attributes returns the same queue URL.
3. A sent message can be received and deleted through the public HTTP API.
4. A received message is not returned again before its visibility deadline.
5. An undeleted message becomes visible after its visibility deadline.
6. A re-received message has a different receipt handle and an increased receive
   count.
7. A stale receipt handle cannot delete the latest receive generation.
8. Deleting with the current receipt handle prevents future delivery.
9. When 20 consumers concurrently receive one visible message, at most one
   claim succeeds during that visibility window.
10. Invalid JSON, missing fields, invalid limits, oversized messages, and missing
    queues return stable error envelopes.
11. Tests use an injectable clock rather than sleep-based timing.
12. `go test ./...` and `go test -race ./...` pass.

## 11. Full action namespace

The final service intends to expose the following action URLs. An endpoint MUST
return `NotImplemented` with HTTP 501 until its behavior is implemented; it MUST
NOT return a false success response.

| Area | Action URL |
|---|---|
| Messages | `/v1/sqs/SendMessage` |
| Messages | `/v1/sqs/SendMessageBatch` |
| Messages | `/v1/sqs/ReceiveMessage` |
| Messages | `/v1/sqs/DeleteMessage` |
| Messages | `/v1/sqs/DeleteMessageBatch` |
| Messages | `/v1/sqs/ChangeMessageVisibility` |
| Messages | `/v1/sqs/ChangeMessageVisibilityBatch` |
| Queues | `/v1/sqs/CreateQueue` |
| Queues | `/v1/sqs/DeleteQueue` |
| Queues | `/v1/sqs/PurgeQueue` |
| Queues | `/v1/sqs/GetQueueUrl` |
| Queues | `/v1/sqs/GetQueueAttributes` |
| Queues | `/v1/sqs/SetQueueAttributes` |
| Queues | `/v1/sqs/ListQueues` |
| Tags | `/v1/sqs/TagQueue` |
| Tags | `/v1/sqs/UntagQueue` |
| Tags | `/v1/sqs/ListQueueTags` |
| Access metadata | `/v1/sqs/AddPermission` |
| Access metadata | `/v1/sqs/RemovePermission` |
| Dead-letter queues | `/v1/sqs/ListDeadLetterSourceQueues` |
| Redrive | `/v1/sqs/StartMessageMoveTask` |
| Redrive | `/v1/sqs/CancelMessageMoveTask` |
| Redrive | `/v1/sqs/ListMessageMoveTasks` |

Because AWS authentication and policy enforcement are out of scope,
`AddPermission` and `RemovePermission` will initially manage compatibility
metadata only. Their authorization effect, if any, will be defined with SimQ's
future tenant and authentication model.

## 12. Delivery milestones after M0

### M1: Complete durable Standard Queue core

- Durable local storage and restart recovery.
- `ChangeMessageVisibility`.
- Long polling.
- Delivery delay.
- Retention cleanup.
- Message attributes.
- Queue attributes.
- Batch message operations.

### M2: Queue control and administration

- List, delete, and purge queues.
- Tags.
- Permission metadata.
- Stable pagination and administrative errors.

#### Detailed queue control and administration contract

M2 activates `QUEUE-003` through `QUEUE-011`. Every mutating success is
returned only after its single-node durable transaction commits and
synchronizes, using the same failure mapping as M1.

##### Queue identity and URLs

- Every live queue has an opaque `QueueId`; names remain unique and
  case-sensitive in the global namespace.
- All queue-scoped operations bind both queue name and ID in the authoritative
  repository transaction. A stale generation MUST return `QueueDoesNotExist`
  with HTTP 404 and MUST NOT observe or mutate a recreated queue.
- Schema-v3 queues receive deterministic installation-local IDs during the
  schema-v4 migration. A first generation may accept its name-only compatibility
  alias until deletion; all returned M2 URLs include the ID.

##### ListQueues

Request:

```json
{
  "QueueNamePrefix": "ord",
  "MaxResults": 100,
  "NextToken": "opaque-token"
}
```

Response:

```json
{
  "QueueUrls": [
    "http://localhost:9324/queues/orders/q_0123456789abcdef0123456789abcdef"
  ],
  "NextToken": "opaque-token",
  "RequestId": "01JEXAMPLE"
}
```

- All fields are optional. Results are ordered by queue name as unsigned UTF-8
  bytes, which is ASCII lexical order for valid queue names.
- `QueueNamePrefix` is case-sensitive and may be empty. A non-empty prefix must
  be at most 80 characters and contain only valid queue-name characters.
- `MaxResults`, when supplied, accepts 1 through 1,000. If omitted, at most
  1,000 URLs are returned and `NextToken` is omitted.
- A token is returned only when `MaxResults` was supplied and another result
  exists. It binds the prefix, last returned name, and namespace revision.
  Malformed, mismatched, or revision-expired tokens return
  `InvalidPaginationToken` with HTTP 400. No page is returned from a stale
  token, preventing silent duplicate or skipped traversal after create/delete.

##### DeleteQueue

Request contains only `QueueUrl`; response contains only `RequestId`.

- The queue generation, its active messages, message-order index, receipt
  history, tags, and permission metadata are removed in one transaction.
- Historical message IDs remain reserved so a later generator collision cannot
  reuse an issued ID.
- A missing or already deleted generation returns `QueueDoesNotExist` with HTTP
  404. Client retry after an ambiguous response is therefore not claimed to be
  idempotent at the response level.
- A concurrent operation linearizes before deletion and is removed with the
  queue, or after deletion and fails against the stale generation.

##### PurgeQueue

Request contains only `QueueUrl`; response contains only `RequestId`.

- Purge atomically removes all active messages, message-order entries, and
  receipt history for the addressed generation.
- Queue identity, configuration, tags, permission metadata, and historical
  message IDs are preserved.
- A concurrent send linearizes before purge and is removed, or after purge and
  survives. Repeated purge of a live empty queue succeeds.

##### Queue tags

`TagQueue` accepts `QueueUrl` and a non-empty `Tags` string map. `UntagQueue`
accepts `QueueUrl` and a non-empty, duplicate-free `TagKeys` string array.
`ListQueueTags` accepts only `QueueUrl` and returns `Tags` plus `RequestId`.

- Tags are case-sensitive metadata with no service semantics. Adding an
  existing key replaces its value.
- A queue may have at most 50 tags after an update. Keys are valid UTF-8, 1 to
  128 bytes; values are valid UTF-8, 0 to 256 bytes. Keys beginning with
  `simq:` are reserved. Invalid input returns `InvalidRequest`; exceeding the
  total returns `OverLimit`, both with HTTP 400.
- Removing a missing key is a successful no-op. Every multi-tag mutation is
  validated fully and commits atomically.

##### Permission compatibility metadata

`AddPermission` accepts `QueueUrl`, `Label`, `AWSAccountIds`, and `Actions`.
`RemovePermission` accepts `QueueUrl` and `Label`. Successful responses contain
only `RequestId`.

- Labels use the queue-name character set and length range 1 through 80.
- `AWSAccountIds` contains 1 through 10 distinct 12-digit strings. `Actions`
  contains 1 through 7 distinct values, each either `*` or an action name in
  the full SimQ namespace.
- A queue may contain at most 20 permission statements. Re-adding an existing
  label replaces that complete statement atomically; removing a missing label
  succeeds as a no-op. Exceeding the total returns `OverLimit` with HTTP 400.
- `GetQueueAttributes` accepts `Policy`. Its value is a deterministic compact
  JSON document containing sorted statements, account IDs, and actions. `All`
  includes `Policy`; `SetQueueAttributes` does not accept it.
- This policy is compatibility metadata only. M2 has no authentication,
  principal, allow, or deny enforcement, and the document MUST NOT affect
  request authorization.

##### M2 acceptance criteria

M2 is complete only when automated tests cover strict JSON decoding; stable
pagination and token invalidation; old-generation isolation; delete/purge
atomicity and queue boundaries; tag and permission limits; policy determinism;
memory/bbolt conformance; schema-v3 migration and restart recovery; injected
commit failures; concurrent create/delete/purge/send/receive interactions;
long-poll wake-up on deletion; and the complete Go 1.26.7 verification suite.

### M3: Dead-letter queues and redrive

- Redrive policy and maximum receive count.
- Dead-letter source listing.
- Start, list, and cancel message move tasks.
- Crash-safe asynchronous move processing.

#### Queue ARN and `RedrivePolicy`

The generation-bound queue ARN is
`arn:simq:sqs:::<QueueName>/<QueueId>`. `GetQueueAttributes` supports the
read-only `QueueArn` attribute and the `RedrivePolicy` attribute. A configured
policy is returned as canonical compact JSON containing exactly
`deadLetterTargetArn` and `maxReceiveCount`.

`SetQueueAttributes` accepts that same shape. The target MUST be a live queue
generation, MUST differ from the source, and `maxReceiveCount` MUST be a decimal
string from 1 through 1000. An empty `RedrivePolicy` string removes the policy.
Policy cycles are rejected. `RedriveAllowPolicy` is not exposed in M3; SimQ's
single local namespace has an implicit allow-all policy. A referenced
dead-letter target cannot be deleted until all source policies are removed or
changed.

On an eligible receive attempt, a message with `ReceiveCount >=
maxReceiveCount` is moved instead of receiving a new receipt. The source removal,
destination insertion, and original-source record are one transaction. The
message keeps its ID, body, digests, attributes, original sent timestamp, and
expiration deadline. Its current receipt, receive count, first-receive time,
visibility deadline, and delay are reset. The internal receipt generation and
issued receipt history remain monotonic so a pre-move handle is a harmless stale
handle and cannot affect a post-move claim.

#### `ListDeadLetterSourceQueues`

The request contains `QueueUrl`, optional `MaxResults` from 1 through 1000, and
optional `NextToken`. Results are source `QueueUrls` in queue-name order. Tokens
are bound to the target queue generation, page size, and redrive revision;
changes to redrive relationships invalidate an outstanding token.

#### Message move tasks

`StartMessageMoveTask` requires `SourceArn`, accepts optional `DestinationArn`,
and accepts optional `MaxNumberOfMessagesPerSecond` from 1 through 500 (default
500). The source MUST currently be referenced as a DLQ and only one `RUNNING`
task may exist for a source generation. The response returns `TaskHandle`.

The task snapshots the current source-order high-water mark. With a destination,
all snapshot messages move there. Without one, each message moves to the live
original source generation recorded by automatic redrive. A missing origin or
deleted original source fails the task. New arrivals above the boundary are not
part of the task.

`ListMessageMoveTasks` requires `SourceArn` and accepts `MaxResults` from 1
through 10 (default 1). It returns newest tasks first. `CancelMessageMoveTask`
requires `TaskHandle`; only a `RUNNING` task can be cancelled. Cancellation is
durable, returns the number already moved, and does not reverse committed
transfers. Task status values are `RUNNING`, `COMPLETED`, `CANCELLED`, and
`FAILED`.

A manual move resets delivery state and creates a new enqueue lifetime using
the destination retention period, while preserving the message ID, body,
digests, and user attributes.

##### M3 acceptance criteria

M3 is complete only when automated tests cover strict policy and ARN parsing;
cycle and stale-generation rejection; exact receive thresholds; payload,
timestamp, retention, and receipt-history behavior; concurrent single-copy
transfer on memory and bbolt; stable source pagination; task boundary, progress,
cancel, complete, and fail states; custom and original destinations; injected
commit rollback; schema-v4 backup migration; close/reopen worker recovery;
strict HTTP errors; and the complete Go 1.26.7 verification suite.

### M4: FIFO queues

- Message groups and group-level ordering.
- Deduplication IDs and content-based deduplication.
- FIFO sequence numbers and receive-attempt behavior.
- FIFO concurrency and failure tests.

#### Queue configuration

A FIFO queue name MUST end in `.fifo` and creation MUST include `FifoQueue:
"true"`. A Standard queue MUST omit it or use `"false"`, and its name MUST not
end in `.fifo`. Queue type is immutable. `ContentBasedDeduplication` is a
FIFO-only boolean string and defaults to `"false"`.

#### FIFO send and sequence

Every FIFO send requires a non-empty `MessageGroupId` of at most 128 UTF-8 bytes.
It also requires `MessageDeduplicationId` unless content-based deduplication is
enabled. Content-based IDs are the lowercase SHA-256 of the body only; explicit
IDs take precedence. Deduplication is scoped to one queue generation and lasts
exactly five minutes from the first committed send. A duplicate returns the
original `MessageId` and `SequenceNumber` without enqueueing, even if the message
was deleted. Standard queues reject FIFO-only fields.

`SequenceNumber` is a decimal, queue-generation-scoped logical counter allocated
in the enqueue or destination-transfer commit. It is unique and strictly
increasing; delivery ordering remains scoped per message group.

#### FIFO receive

Only the earliest active message in each group is selectable. A delayed or
in-flight head blocks later group messages. Expiration, deletion, or automatic
DLQ transfer removes the head barrier atomically. Independent groups may fill a
single receive batch.

`ReceiveRequestAttemptId` is FIFO-only, non-empty, and at most 128 UTF-8 bytes.
For five minutes, retrying it returns the original committed message and receipt
snapshots without another claim or receive-count increment. Attempt state is
queue-generation bound and durable. FIFO responses include `MessageGroupId`,
`MessageDeduplicationId`, and decimal `SequenceNumber`; send responses include
`SequenceNumber`.

A long poll does not commit an empty receive-attempt snapshot during its internal
eligibility checks. It commits the empty snapshot only when the request itself
completes empty. Replaying an already committed empty or non-empty attempt returns
that snapshot immediately.

#### FIFO redrive and message moves

A redrive policy and an explicit message-move destination MUST connect queues of
the same type. FIFO transfer preserves `MessageGroupId` and
`MessageDeduplicationId`, removes the source group-head barrier, and atomically
allocates a new sequence in the destination queue generation. Transfer does not
create a destination send-deduplication record: it is a state transition of an
existing message, not a new `SendMessage` operation. The source generation's
existing deduplication record remains effective until its original deadline.

M4 is complete only when automated evidence covers queue-type validation and
immutability; group-head blocking and independent-group progress; concurrent
send deduplication; exact five-minute boundaries; content-based hashing;
sequence non-reuse across purge and restart; receive-attempt replay including
long polling; FIFO redrive and move resequencing; schema-v5 backup migration;
fail-closed corruption; injected commit rollback; strict single and batch HTTP
fields; and the complete Go 1.26.7 verification suite.

### M5: Clustered durability and availability

- Replicated deterministic state machine.
- Durable quorum commit before successful mutation responses.
- Leader routing and failover.
- Snapshots, restore, membership changes, and rolling upgrades.
- Fault-injection tests for process, disk, and network failures.

#### Cluster scope and consistency

M5 provides one opt-in Raft group containing all queues in that deployment.
Every voting node has a node-local Raft log, snapshot directory, and bbolt queue
state database. Sharding and queue rebalancing are not part of M5.

All queue actions are leader-consistent. A leader verifies leadership and
crosses a Raft barrier before serving a read. Mutations return success only after
quorum commit and leader apply. Followers, candidates, and minority partitions
return `NotLeader` or `ClusterUnavailable` and MUST NOT serve stale queue state.
A `NotLeader` response includes the known leader's API URL when configured.

#### Proposal identity

Clustered mutating HTTP requests require `X-SimQ-Operation-Id`, containing 1 to
128 UTF-8 bytes without control characters. The operation ID scopes deterministic
sub-proposal numbers for batch entries and internal multi-step request work. A
client retry after timeout or leader change MUST reuse the same value. SimQ
durably returns the first committed result for a repeated proposal ID without
reapplying its mutation. Standalone mode accepts but does not require the header.

#### Apply and snapshot contract

Replicated command format version 1 contains the operation kind, proposal ID,
and complete repository input. Queue mutation, encoded result, and applied Raft
index commit atomically in schema v7. A command with an unsupported version
fails the node closed.

Snapshots contain every authoritative queue bucket, FIFO and redrive state,
proposal replay records, and the applied index. Restore is performed by the Raft
FSM, validates the candidate before publication, and replaces no live state
until validation succeeds. Automatic snapshots and an explicit leader-only
snapshot trigger use the same format.

#### Membership and upgrades

Bootstrap creates a cluster only when no Raft state exists. Adding voters first
uses the consensus library's staging/catch-up behavior. Adding nonvoters,
promotion, demotion, and removal use the library APIs and are leader-only.
Node IDs are stable and unique; advertised Raft and API addresses are explicit.
Nodes with an unsupported SimQ command protocol are not admitted. Rolling
upgrades MUST retain overlapping command and Raft protocol compatibility.

### M6: Cloud productization

- SimQ-native authentication and tenant isolation.
- TLS and encryption at rest.
- Quotas and rate limiting.
- Metrics, tracing, audit logs, and usage accounting.
- Backup, restore, placement policy, and operational runbooks.

#### Security mode and identity

`SIMQ_SECURITY_MODE=oidc` enables M6 product security. Queue endpoints then
require an RS256 bearer JWT from the configured HTTPS issuer and audience.
Issuer, signature key, algorithm, audience, `exp`, optional `nbf`, non-empty
`sub`, configured tenant claim, and configured role claim are independently
validated. Missing or invalid authentication returns 401. A verified principal
without the action's role returns 403. Health is unauthenticated; metrics and
cluster administration use separate operator credentials.

Security mode `disabled` preserves the single legacy tenant for local
compatibility. Invalid OIDC, TLS, encryption, or quota configuration MUST stop
startup and MUST NOT silently select disabled mode.

OIDC mode requires bbolt, `SIMQ_TLS_CERT_FILE`, `SIMQ_TLS_KEY_FILE`, a versioned
`SIMQ_ENCRYPTION_KEYS` key ring with `SIMQ_ENCRYPTION_ACTIVE_KEY`, an explicit
`SIMQ_LEGACY_TENANT`, `SIMQ_METRICS_TOKEN`, and `SIMQ_AUDIT_PATH`. The direct
API listener uses TLS 1.3. Optional Raft mutual TLS requires cluster
certificate, key, and CA files together and also uses TLS 1.3.

#### Tenant and authorization contract

The verified tenant claim is normalized as 1 to 128 UTF-8 bytes without control
characters. Internally it maps to a SHA-256 namespace; public tenant strings do
not appear in storage keys or metric labels. Queue names are unique per tenant.
Every queue URL, receipt mutation, pagination token, redrive relationship, FIFO
deduplication record, move task, and replicated command is tenant-scoped.

Roles are fixed strings: `simq.admin`, `simq.reader`, `simq.producer`, and
`simq.consumer`. Admin authorizes all queue actions. Reader authorizes list/get
operations, producer authorizes send operations, and consumer authorizes
receive/delete/visibility operations. Queue creation, configuration, tagging,
permission metadata, purge, redrive, and move-task administration require admin.
M2 permission statements remain compatibility metadata and grant no authority.

#### Encryption and keys

In OIDC mode all message bodies and message attributes are encrypted before
they enter a repository or Raft command. AES-256-GCM envelopes include a
non-secret key ID and a new 96-bit random nonce. Tenant namespace and message ID
are authenticated additional data, allowing authorized redrive within a tenant.
New writes use the configured
active key; older configured keys remain readable during rotation. Missing keys,
authentication failure, malformed envelopes, or plaintext in secure mode make
the operation fail closed without returning partial message data.
Consequently, pre-M6 plaintext messages must be drained or purged before OIDC
mode is enabled; the configured legacy tenant retains its queue metadata, but
remaining plaintext payloads are not returned.

#### Quotas and observability

M6 supports per-tenant maximum queues, active messages, stored payload bytes,
requests per second, and burst. Storage quota checks and usage changes are
atomic with the mutation. Rate limiting is enforced by the current leader and
returns 429; a leader change may reset only the transient burst, never storage
usage. Batch entries consume and report quota independently.

`/metrics` uses bounded tenant hashes and fixed action/result labels. Structured
audit events record timestamp, request/trace ID, issuer-subject hash, tenant
hash, action, resource identifier, and result. Metrics, audit, traces, and logs
MUST NOT contain tokens, message bodies or attributes, receipt handles,
ciphertext, plaintext tenant IDs, or encryption keys.

### M7: Fixed-catalog multi-Raft sharding

M7 partitions authenticated tenants across two or more independent Raft groups.
It is enabled only by an explicit `SIMQ_SHARD_MANIFEST` and requires the M6 OIDC
security boundary, bbolt storage, a stable cluster node ID, and the cluster
administrator token. Disabled security mode and in-memory storage MUST reject a
shard manifest.

#### Catalog and routing

The manifest contains a positive version, a default shard, at least two unique
shard IDs, and a replica placement for every shard. The canonical catalog
revision is the first 16 bytes of SHA-256, lowercase hexadecimal encoded, over
the canonical version, default shard, and lexically sorted shard IDs. Replica
addresses, API URLs, failure domains, bootstrap choice, and voter roles do not
affect the revision and may change as placement metadata.

Each secure tenant already has a non-reversible SHA-256 namespace digest.
Rendezvous hashing over that digest and the fixed shard IDs selects exactly one
authoritative shard. Every queue, message, receipt, FIFO group and deduplication
record, redrive edge, move task, proposal replay result, quota counter, and
encrypted payload for that tenant MUST use that shard. Keys without an M6 tenant
namespace use the declared default shard for compatibility.

Catalog identity is durable metadata in every schema-v9-or-later shard database and
snapshot. Startup may bind an unbound migrated database once; a subsequently
different revision, malformed revision, or snapshot from another catalog MUST
fail before serving or publishing restored state. Adding or removing shard IDs
or changing the default shard is therefore not an online placement edit.

#### Hosting, commits, and leader routing

Each process hosts one independent M5 Raft repository and state machine per
configured shard under a separate data directory. A write is successful only
after the selected shard's quorum durably commits and applies it. Failure,
backpressure, or leadership in one shard does not authorize another shard to
serve that tenant.

Authentication and tenant scoping occur before cluster leader routing. A node
that is not leader for the selected tenant shard returns the selected shard's
configured leader API URL using the existing service-unavailable leader-hint
contract. Reads remain leader-fenced and linearizable within that shard.

#### Placement and administration

Each shard plan declares a bootstrap node and at least three odd initial voters.
Every node ID and Raft address is unique where required, the bootstrap node is
an initial voter, and every configured local process has a planned replica.
Initial and proposed voter sets MUST retain at least three voters and MUST NOT
allow any one failure domain to contain a quorum.

The existing cluster administrator token protects these M7 endpoints:

```text
GET  /v1/cluster/shards
GET  /v1/cluster/shards/members?ShardId=<id>
POST /v1/cluster/shards/members
POST /v1/cluster/shards/snapshot
```

The status response reports bounded shard ID, catalog revision, leader state,
and configured leader API URL. Membership bodies contain `ShardId`, `Action`,
`ID`, and `Address`; actions are `add-nonvoter`, `add-voter`, `demote-voter`,
and `remove-server`. The target node and exact Raft address MUST be present in
the placement plan, and the simulated result MUST pass voter and failure-domain
safety before the leader calls Raft. Snapshot triggers are shard-specific.

Online relocation changes a whole shard replica: add a planned nonvoter, allow
it to catch up, promote it, and only then demote or remove the old replica.
M7 provides no API for moving an existing tenant between shard IDs. Such
resharding requires a later durable ownership and fenced cutover protocol and
MUST fail closed rather than be emulated by copying storage keys.

### M8: Epoch-fenced tenant relocation

M8 moves one complete secure tenant between two shard IDs already present in
the bound M7 catalog. It does not change that catalog, add shard IDs, or split
one tenant's queues. The default shard's Raft FSM is the ownership control
authority. An absent ownership record means epoch 1 on the M7 rendezvous shard;
beginning a migration materializes that implicit assignment.

#### Ownership and routing

An ownership record contains the 64-character lowercase tenant digest, shard
ID, and positive epoch. Public administration never accepts or emits a plaintext
tenant ID. A routing override always wins over rendezvous hashing. Every queue
operation verifies the selected shard's local tenant fence before reading or
mutating data. Frozen, prepared, moved, wrong-shard, and wrong-epoch states
return retryable `ServiceUnavailable` and MUST NOT fall back to another shard.

Locally applied ownership may temporarily lag the control leader. Safety does
not depend on a fresh router: after source freeze the source rejects operations,
and before destination activation the destination rejects operations. Lag may
therefore reduce availability during cutover but cannot create two writers.

#### Migration protocol

A migration record contains a unique `tm_` plus 32 lowercase hexadecimal ID,
tenant digest, source and destination shard IDs, source epoch `E`, destination
epoch `E+1`, phase, bundle hash, and bounded failure information. The phases are
`FREEZING`, `PREPARING`, `CUTTING_OVER`, `ACTIVATING`, `CLEANING`, `ABORTING`, `COMPLETED`,
and pre-cutover `ABORTED`.

The source freeze and final bundle capture commit in one source-shard Raft
entry. No foreground operation, expiry sweep, or move worker may mutate the
tenant afterward. The bundle includes all queues, messages, order indexes,
receipts, tombstones, tags, permissions, redrive state, move-task state, FIFO
state, encrypted payload envelopes, and tenant usage. It excludes shard-global
metadata and other tenants. Tenant-scoped proposal replay results move with the
bundle so an ambiguous operation retry remains result-stable after cutover.
Canonical entry ordering and SHA-256 authenticate
the bundle, which may not exceed 16 MiB encoded in M8.

The destination imports the whole bundle atomically as PREPARED and serves none
of it. The control shard then compare-and-swaps `(source,E)` to
`(destination,E+1)`. Only a matching prepared destination may become ACTIVE.
The source is then removed atomically and marked MOVED. Repeating any phase with
the same migration data is idempotent; conflicting migration IDs, hashes,
epochs, or destinations fail closed.

Abort is allowed only before ownership cutover. It removes prepared destination
data if present and unfreezes the unchanged source epoch. After cutover, recovery
MUST finish activation and cleanup rather than restoring the old owner.

#### Administration

The cluster administrator token protects:

```text
GET  /v1/cluster/tenant-migrations
GET  /v1/cluster/tenant-migrations/status?MigrationId=<id>
POST /v1/cluster/tenant-migrations
POST /v1/cluster/tenant-migrations/advance
POST /v1/cluster/tenant-migrations/abort
```

Create accepts `MigrationId`, `TenantDigest`, and `DestinationShard`. Advance
and abort accept `MigrationId`. Status reports only bounded IDs, shard IDs,
epochs, phase, bundle hash, and the API URL of the leader required for the next
step. These APIs never return a bundle, queue name, message data, receipt,
plaintext tenant, encryption key, filesystem path, or internal error.

### M9: Elastic shard topology

M9 replaces the fixed active shard set with a replicated logical catalog while
retaining the M7 placement and M8 ownership safety boundaries. Manifest version
2 contains an immutable 32-character lowercase hexadecimal `cluster_id`, the
unchanged default shard, and all reviewed candidate replica plans. Each shard
declares initial state `READY` or `CANDIDATE`. Manifest version 1 remains a
compatibility input whose existing catalog revision is its cluster identity and
whose shards are all READY.

The default shard is immutable and remains local on every API node. A node may
omit non-default replicas. Every possible default-shard voter MUST remain a
replica of every data shard so any control leader has a replicated observation
path for M8 migration phases. An omitted shard is never opened locally. Requests
for it return retryable unavailability plus a configured hosting-node entry URL;
the hosting node returns the exact Raft leader when necessary.

#### Explicit tenant directory

The control FSM stores directory mode `LEGACY`, `BACKFILLING`, or `EXPLICIT`.
Before a tenant data operation, an absent ownership record is compare-and-set to
epoch 1. In LEGACY or BACKFILLING mode the selected shard is the M8 rendezvous
result over the initial READY set. In EXPLICIT mode a previously unseen tenant
is assigned deterministically across the current READY set. A retry returns the
first assignment and never recomputes it from a newer generation.

Backfill enumerates stored tenant digests in bounded pages, writes their legacy
assignments idempotently, and commits EXPLICIT only after every initial shard is
complete. Candidate activation MUST reject an incomplete backfill. Operations
that race the final barrier serialize through the control log: assignment before
the barrier uses the old set; assignment afterward may use the new READY set and
has no pre-existing data.

#### Catalog and shard lifecycle

The catalog has a positive monotonic generation, stable cluster ID and default
shard, directory mode, and lexically sorted shard records. Each shard has a
stable ID, 32-character incarnation, configured entry API URL, and state
`CANDIDATE`, `READY`, `DRAINING`, or `RETIRED`.

Only a reviewed CANDIDATE whose planned Raft group is already provisioned may
advance to READY. The generation increments in the same control transaction.
READY and DRAINING shards remain valid existing owners, but only READY shards
receive new tenants. Drain is resumable and selects one explicit owner at a
time, uses the M8 migration state machine to move it to a READY destination,
and continues after failures. Retirement requires zero owners and no unfinished
migration. It commits a permanent tombstone; the ID and incarnation cannot be
activated again. Physical replica removal and file deletion occur only after
retirement through the existing reviewed deployment and membership procedures.

Topology operations have unique `to_` plus 32 lowercase hexadecimal IDs and
phases `PLANNED`, `RUNNING`, `COMPLETED`, or pre-activation `ABORTED`. Retrying a
phase is idempotent. Abort may cancel candidate activation before READY or a
drain before its first tenant cutover; completed M8 cutovers are never rolled
back.

#### M9 administration

The cluster administrator token protects:

```text
GET  /v1/cluster/topology
GET  /v1/cluster/topology/operations
GET  /v1/cluster/topology/operations/status?OperationId=<id>
POST /v1/cluster/topology/backfill
POST /v1/cluster/topology/shards/activate
POST /v1/cluster/topology/shards/drain
POST /v1/cluster/topology/operations/advance
POST /v1/cluster/topology/operations/abort
```

Responses expose only bounded catalog, operation, shard, generation, and next
leader/entry metadata. They never expose tenant IDs, queue or message data,
receipts, bundle contents, secrets, filesystem paths, or internal errors.

## 13. Health endpoints

Health endpoints are operational APIs and do not use SQS action names.

```text
GET /healthz
GET /readyz
```

- `/healthz` reports whether the process is alive.
- `/readyz` reports whether the node can currently serve queue requests.
- These endpoints MUST NOT expose secrets or message data.

## 14. M1-A operational contract

M1-A changes the durability and operating contract without changing the M0 JSON
request or response shapes.

- A successful `CreateQueue`, `SendMessage`, `ReceiveMessage`, or
  `DeleteMessage` response is returned only after its single-node durable write
  transaction commits and synchronizes. A storage/commit failure returns
  `ServiceUnavailable` with HTTP 503 and MUST NOT expose internal paths or data.
- Successful queue creation, enqueue, receive state, receipt history, visibility
  deadline, and deletion survive process restart. Visibility is evaluated at
  nanosecond precision internally; public system timestamps remain Unix
  milliseconds encoded as decimal strings.
- `SIMQ_STORAGE` accepts `bbolt` or `memory` and defaults to `bbolt`. `memory` is
  an explicit development/test mode and provides only the original process-local
  M0 durability contract.
- `SIMQ_DATA_PATH` defaults to `./data/simq.db` when bbolt is selected.
- `SIMQ_BBOLT_OPEN_TIMEOUT` defaults to `1s` and must be a positive Go duration.
  A process that cannot acquire the exclusive data-file lock before that
  deadline fails startup.
- On Unix, newly created data directories use mode `0700`, existing data
  directories must already have mode `0700`, and database files use and retain
  mode `0600`. Windows does not expose equivalent POSIX mode bits through Go;
  Windows builds rely on the configured directory's NTFS ACL inheritance and
  do not claim to audit ACL policy. File contents and bbolt commits are synced,
  while directory-handle `fsync` is Unix-only.
- An existing zero-length, malformed, corrupt, schema-incomplete, or unknown
  schema-version data file causes startup failure. It is never automatically
  replaced, reinitialized, or partially skipped.
- The server does not begin listening until the repository is open and its full
  schema validates. `/healthz` is process liveness. `/readyz` returns HTTP 200
  only while a repository health transaction and schema validation succeed;
  otherwise it returns HTTP 503 without message data or storage paths.
- SIGINT and SIGTERM initiate HTTP graceful shutdown and then close the
  repository. SIGKILL safety relies on the last completed durable transaction.
- bbolt provides a single-writer database and an exclusive process lock. M1-A is
  single-process and single-node: clustering, replication, quorum commit, leader
  election, online compaction, backup/restore commands, and automatic schema
  migration are not provided.

## 15. M1-B ChangeMessageVisibility contract

### 15.1 Request and response

Endpoint:

```text
POST /v1/sqs/ChangeMessageVisibility
```

Request:

```json
{
  "QueueUrl": "http://localhost:9324/queues/orders",
  "ReceiptHandle": "opaque-receipt-handle",
  "VisibilityTimeout": 30
}
```

Response:

```json
{
  "RequestId": "01JEXAMPLE"
}
```

`QueueUrl` and `ReceiptHandle` are required strings. `VisibilityTimeout` is a
required JSON integer in the inclusive range 0 through 43,200 and is measured
in seconds. Unknown or duplicate fields, missing fields, wrong field types,
fractional numbers, malformed JSON, and trailing JSON values are rejected as
`InvalidRequest` without changing queue state. An oversized body is rejected as
`RequestTooLarge` with HTTP 413, also without changing state.

### 15.2 Deadline and receipt semantics

- The service obtains one command timestamp from its clock and sets the new
  deadline to that timestamp plus `VisibilityTimeout`. The timeout is not added
  to the previous deadline.
- A timeout of zero sets the deadline equal to the command timestamp, making an
  undeleted message immediately eligible for another receive.
- Only the receipt handle for the current receive generation may change the
  current deadline. A successful change does not replace the receipt handle or
  change the message ID, receive count, receive generation, sent timestamp,
  approximate first-receive timestamp, body, or body digest.
- Repeating the action with the same current handle resets the deadline from
  each call's command timestamp.
- A deadline may already have passed while its handle is still current. If the
  visibility change commits before another receive claims the message, the new
  deadline hides that same generation again. If receive commits first and
  creates a new generation, the old handle is stale and cannot change the new
  generation's deadline.
- A structurally valid, issued stale or consumed handle returns HTTP 200 as a
  no-op. A malformed, unissued, corrupted, unverifiable, or cross-queue handle
  returns `ReceiptHandleIsInvalid` with HTTP 400 and has no effect.
- A missing queue returns `QueueDoesNotExist` with HTTP 404.
- Selection of the binding, current-generation check, and deadline update are
  one repository transaction. Durable mode returns HTTP 200 only after that
  transaction commits and synchronizes. A storage, encoding, disk, commit, or
  sync failure returns `ServiceUnavailable` with HTTP 503.
- Unexpected internal failures return `InternalError` with HTTP 500. All
  responses use the existing JSON envelope and matching request ID header/body
  rules, and errors do not expose storage paths, message bodies, or receipt
  handles.

At the historical M1-B boundary,
`POST /v1/sqs/ChangeMessageVisibilityBatch` returned `NotImplemented` with HTTP
501. Section 19 supersedes that boundary for completed M1-F.

## 16. M1-C delivery delay and retention contract

M1-C extends the existing Standard Queue actions; it does not add a new action
URL. The M0 restrictions that rejected delay and retention fields are superseded
by this section once M1-C is complete.

### 16.1 Queue defaults

`CreateQueue.Attributes` accepts exactly these attributes in M1-C:

| Attribute | Default | Inclusive range | Encoding |
|---|---:|---:|---|
| `VisibilityTimeout` | 30 seconds | 0–43,200 | decimal string |
| `DelaySeconds` | 0 seconds | 0–900 | decimal string |
| `MessageRetentionPeriod` | 345,600 seconds | 60–1,209,600 | decimal string |

All attributes remain optional. Omitted attributes take their defaults before
queue identity is compared, so omitting a default and explicitly supplying that
default are equivalent. Recreating a queue is idempotent only when all three
effective values match. Any other attribute is rejected as `InvalidRequest`.
M1-D implements `GetQueueAttributes` and `SetQueueAttributes` for these three
attributes. Section 17 supersedes the earlier M1-C exclusion.

### 16.2 Per-message delay

`SendMessage` accepts one new optional JSON integer:

```json
{
  "QueueUrl": "http://localhost:9324/queues/orders",
  "MessageBody": "order-created",
  "DelaySeconds": 10
}
```

`DelaySeconds` accepts 0 through 900. When omitted, the queue's `DelaySeconds`
is used. The service obtains one command timestamp and records:

```text
AvailableAt = CommandTimestamp + EffectiveDelaySeconds
ExpiresAt   = CommandTimestamp + QueueMessageRetentionPeriod
```

The delay does not change the public `SentTimestamp`. A delayed message is not
eligible before `AvailableAt` and is eligible exactly at `AvailableAt` unless
it has reached `ExpiresAt`. A delay longer than the remaining retention period
is allowed; such a message expires without being delivered.

Missing `DelaySeconds` uses the queue default. A wrong type, fractional number,
value below 0 or above 900, unknown or duplicate field, malformed JSON, trailing
JSON, or oversized body is rejected without enqueuing a message.

### 16.3 Retention and cleanup

Retention begins at the send command timestamp. Receiving a message, changing
its visibility, process downtime, and restart MUST NOT extend or recompute
`ExpiresAt`. At `ExpiresAt <= operation time`, a message is terminal and MUST
not be returned, changed, or resurrected whether it was delayed, visible, or
in-flight.

Eligibility enforcement is authoritative and does not depend on background
cleanup running on time. `ReceiveMessage` removes expired messages encountered
in its queue transaction before selecting deliverable messages.
`ChangeMessageVisibility` removes an expired active message as a consumed-handle
no-op instead of changing its deadline. Issued receipt and message-ID history
remain available so stale or consumed handles preserve their documented
semantics.

The process also runs periodic best-effort physical cleanup across all queues.
`SIMQ_RETENTION_SWEEP_INTERVAL` is a positive Go duration and defaults to `1m`.
Every cleanup pass obtains one service-clock timestamp and submits an explicit
repository expiration command. A failed background pass is logged without
message bodies, receipt handles, or storage paths and is retried on a later
interval. Such failure cannot make an expired message deliverable.

### 16.4 Durability and schema transition

`AvailableAt` and `ExpiresAt` use Unix nanoseconds in durable storage and survive
close/reopen and process crashes. Queue delay and retention defaults are durable
queue metadata. Enqueue stores the message, both lifecycle deadlines, message-ID
history, and active-order entry in one synchronized transaction before returning
HTTP 200.

M1-C introduces on-disk schema and record version 2. Opening a valid version 1
M1-A database performs the documented schema migration before the HTTP listener
starts. Existing queues receive `DelaySeconds=0` and
`MessageRetentionPeriod=345600`. Existing messages receive `AvailableAt` equal
to their stored sent millisecond and `ExpiresAt` equal to that time plus the
default retention period. Unknown versions and failed or incomplete migrations
remain fail-closed.

### 16.5 M1-C exclusions

At the M1-C delivery boundary, long polling, message attributes,
`GetQueueAttributes`, `SetQueueAttributes`, batch actions, queue deletion or
purge, DLQs, FIFO queues, tenant isolation, encryption, and replication were not
implemented. Section 17 supersedes only the message- and queue-attribute parts
of that historical exclusion.

## 17. M1-D message and queue attributes contract

M1-D adds user message attributes and mutable access to the three queue
settings introduced before it. These are SimQ JSON APIs. AWS Query protocol,
AWS SDK/CLI wire compatibility, SigV4, system message attributes, list-valued
message attributes, and the other queue attributes accepted by AWS SQS are not
part of this contract.

### 17.1 Message attribute value and name model

`SendMessage` accepts an optional `MessageAttributes` object:

```json
{
  "QueueUrl": "http://localhost:9324/queues/orders",
  "MessageBody": "order-created",
  "MessageAttributes": {
    "EventType": {
      "DataType": "String.event",
      "StringValue": "OrderCreated"
    },
    "Attempt": {
      "DataType": "Number",
      "StringValue": "1"
    },
    "Payload": {
      "DataType": "Binary.application",
      "BinaryValue": "AQID"
    }
  }
}
```

The object may contain zero through ten entries. Omission and `{}` are
equivalent. `null`, an eleventh entry, a duplicate JSON member at any nesting
level, or an unknown field is `InvalidRequest`.

An attribute name is a case-sensitive ASCII string of 1 through 256 bytes. It
may contain only `A-Z`, `a-z`, `0-9`, `_`, `-`, and `.`. It must not begin with
`AWS.` or `Amazon.` in any letter casing, begin or end with `.`, or contain
`..`. Names are unique because they are JSON object keys.

`DataType` is a case-sensitive ASCII string of 1 through 256 bytes. It consists
of exactly one base type, `String`, `Number`, or `Binary`, optionally followed
by one or more custom suffix labels. Each suffix is introduced by `.` and
contains one or more `A-Z`, `a-z`, `0-9`, `_`, or `-` characters. SimQ stores
and hashes the complete type including every suffix but does not interpret a
suffix.

Each value object has a required `DataType` and exactly one non-empty value
field appropriate to its base type:

- `String` and `Number` use `StringValue` and reject `BinaryValue`.
- `Binary` uses `BinaryValue` and rejects `StringValue`.
- A string value must be valid UTF-8. A binary value is a standard padded or
  unpadded base64 JSON string and is decoded to its raw bytes before validation,
  hashing, and storage. Invalid base64 and a decoded empty value are rejected.
- A number uses ASCII decimal notation with an optional sign, decimal point,
  and base-10 exponent. `NaN`, infinities, hexadecimal notation, surrounding
  whitespace, and a missing integer/fraction digit are rejected. After leading
  and trailing zeroes are ignored it has at most 38 significant digits; zero is
  allowed, while a nonzero magnitude must be between `10^-128` and `10^126`,
  inclusive. SimQ preserves the supplied number string rather than normalizing
  it.

The message size is the number of UTF-8 bytes in `MessageBody` plus, for every
attribute, the byte lengths of its name, complete `DataType`, and decoded
value. The total must be at most 1,048,576 bytes. The body must still be
non-empty; every component also has its limits checked independently. HTTP
base64 expansion and JSON punctuation do not count toward the message size.

The domain value for binary data is decoded `[]byte`. Service, repository,
storage codec, and response projection boundaries deep-copy attribute maps and
binary slices. Message attributes become immutable when enqueue commits and
are unaffected by receive, visibility change, retention cleanup, or queue
setting changes.

### 17.2 Message attribute digest

SimQ computes `MD5OfMessageAttributes` using the canonical encoding documented
for Amazon SQS:

1. Sort attribute names by ascending UTF-8 byte order.
2. For each attribute append the name as a four-byte unsigned big-endian byte
   length followed by its UTF-8 bytes.
3. Append the complete `DataType` in the same length-plus-bytes form.
4. Append one transport byte: `1` for `String` and `Number`, `2` for `Binary`.
5. Append the value as a four-byte unsigned big-endian byte length and either
   its UTF-8 bytes (`String`/`Number`) or raw decoded bytes (`Binary`).
6. Return the lowercase hexadecimal MD5 of the concatenated bytes.

Map iteration order never affects the digest. A successful `SendMessage` with
one or more attributes returns the digest of all stored attributes in
`MD5OfMessageAttributes`; that field is omitted when there are none.

### 17.3 ReceiveMessage projection

`ReceiveMessage` accepts an optional `MessageAttributeNames` array:

```json
{
  "QueueUrl": "http://localhost:9324/queues/orders",
  "MessageAttributeNames": ["EventType", "Payload"]
}
```

- Omission and `[]` request no user message attributes.
- A single `"All"` requests every user message attribute.
- Otherwise every element must be a valid exact attribute name. An absent exact
  name simply contributes nothing to the response.
- Duplicate names, `"All"` combined with another name, an empty name, `".*"`,
  prefix wildcards such as `"Event.*"`, and all other wildcard forms are
  `InvalidRequest`.

Projection is response-only. It does not modify the stored attribute set or
affect claim, receipt, visibility, expiration, or deletion behavior. A received
message includes `MessageAttributes` and `MD5OfMessageAttributes` only when at
least one attribute was selected and exists. The digest is computed over the
attributes actually returned in that response. The existing `Attributes`
object remains receive-related system metadata and is distinct from
`MessageAttributes`.

### 17.4 GetQueueAttributes

Endpoint:

```text
POST /v1/sqs/GetQueueAttributes
```

Request and response:

```json
{
  "QueueUrl": "http://localhost:9324/queues/orders",
  "AttributeNames": ["VisibilityTimeout", "DelaySeconds", "MessageRetentionPeriod"]
}
```

```json
{
  "Attributes": {
    "VisibilityTimeout": "30",
    "DelaySeconds": "0",
    "MessageRetentionPeriod": "345600"
  },
  "RequestId": "01JEXAMPLE"
}
```

`QueueUrl` is required. Omitted `AttributeNames` and an empty array return a
successful empty `Attributes` object. A single `All` returns all three supported
values. Otherwise the array selects exact names from those three. Duplicate
names, `All` mixed with another name, unknown names, wildcards, nulls, and wrong
types are `InvalidRequest`. A missing queue is `QueueDoesNotExist` with HTTP
404. Values are current effective settings encoded as decimal strings.

### 17.5 SetQueueAttributes

Endpoint:

```text
POST /v1/sqs/SetQueueAttributes
```

Request and response:

```json
{
  "QueueUrl": "http://localhost:9324/queues/orders",
  "Attributes": {
    "VisibilityTimeout": "60",
    "DelaySeconds": "5"
  }
}
```

```json
{
  "RequestId": "01JEXAMPLE"
}
```

`QueueUrl` and a non-empty, non-null `Attributes` object are required. The
operation is a partial update: omitted values retain their previous values. It
accepts only `VisibilityTimeout` (0–43,200), `DelaySeconds` (0–900), and
`MessageRetentionPeriod` (60–1,209,600), using the same canonical unsigned
decimal-string validation as `CreateQueue`. Unknown attributes, invalid values,
and ambiguous JSON are rejected before repository mutation. A missing queue is
`QueueDoesNotExist` with HTTP 404.

All supplied values change in one repository transaction. Validation,
encoding, update, commit, or sync failure changes none of them and cannot return
success. After a successful Set, a repeated `CreateQueue` compares its
effective requested settings with the queue's current settings: a complete
match remains idempotent, while any difference remains `QueueAlreadyExists`.

Changing queue settings never rewrites an existing message's `AvailableAt`,
`ExpiresAt`, `SentAtMillis`, body, message attributes, digests, claim metadata,
or receipt history. A later Send atomically observes either the complete old or
complete new queue configuration and uses that snapshot for default delay and
retention. A later Receive without a visibility override atomically observes
either the old or new default visibility timeout in the same claim transaction.
The repository uses the explicit command timestamp supplied by the service and
does not read wall-clock time.

### 17.6 Errors, durability, and schema version 3

Both new actions use the existing strict JSON, response envelope, `RequestId`,
and error mapping: invalid input is HTTP 400 `InvalidRequest`, a missing queue
is HTTP 404 `QueueDoesNotExist`, repository failure is HTTP 503
`ServiceUnavailable`, and an unexpected failure is HTTP 500 `InternalError`.
No error or ordinary log includes a message body, message attribute name/value,
binary value, receipt handle, or storage path.

New durable stores use schema and record version 3. Version 3 message records
store attributes in deterministic name order and store the full attribute
digest. Opening schema version 2 first validates every version 2 record,
creates and synchronizes a mode-`0600` `.schema-v2.bak`, rewrites all records in
one bbolt write transaction with empty attributes for old messages, updates the
schema version last, commits, and validates all version 3 state before startup.
A pre-existing valid backup is reused and not overwritten; an invalid backup
fails closed. Version 1 follows the existing v1-to-v2 migration and preserves
both `.schema-v1.bak` and `.schema-v2.bak` before proceeding to version 3.

This paragraph records the M1-D boundary. M2 supersedes the store-level schema
with version 4 for queue identities and administration metadata; message and
attribute record formats remain at version 3. The schema-v3 source is validated
and preserved as `.schema-v3.bak` before the atomic version-4 migration.

Migration does not recompute established lifecycle deadlines, visibility,
receive metadata, receipt history, message IDs, bodies, body digests, or
timestamps. Corruption, unsupported versions, invalid backup state, or any
migration/validation failure prevents the listener from opening.

`GetQueueAttributes` and `SetQueueAttributes` are implemented in M1-D and are
removed from the HTTP 501 action set. M1-D does not implement long polling,
batch actions, approximate counts, policies, redrive, FIFO attributes, KMS,
tags, queue deletion/purge, authentication, tenancy, encryption, or clustering.

The semantic source material for names, types, size accounting, and the digest
format is the official AWS SQS documentation for
[message metadata](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-message-metadata.html),
[SendMessage](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SendMessage.html),
[ReceiveMessage](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html),
[GetQueueAttributes](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_GetQueueAttributes.html),
and [SetQueueAttributes](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SetQueueAttributes.html).
The SimQ-specific choices above are authoritative for SimQ's JSON API.

## 18. M1-E long polling contract

M1-E extends the existing `ReceiveMessage` JSON request with one optional
integer field:

```json
{
  "QueueUrl": "http://localhost:9324/queues/orders",
  "MaxNumberOfMessages": 10,
  "WaitTimeSeconds": 20
}
```

`WaitTimeSeconds` is measured in seconds and accepts the inclusive range 0
through 20. Omission and zero select the existing short-poll behavior and return
immediately after one authoritative claim attempt. A positive value enables
long polling. Wrong types, fractional numbers, null, values outside the range,
unknown or duplicate fields, malformed JSON, trailing JSON, and an oversized
request are rejected without claiming a message.

SimQ does not add the queue attribute `ReceiveMessageWaitTimeSeconds` in M1-E.
Therefore an omitted request field always means zero regardless of queue
configuration. `GetQueueAttributes`, `SetQueueAttributes`, and `CreateQueue`
continue to support exactly the three attributes in section 17. This keeps the
schema and record version at 3; adding a durable queue-level wait default is a
separate future contract and migration decision.

### 18.1 Waiting and return behavior

- If at least one message can be claimed immediately, the action returns as
  soon as that claim commits. It does not wait to fill `MaxNumberOfMessages`.
- Otherwise the request waits until at least one message can be claimed or the
  requested polling deadline is reached. An expired wait returns HTTP 200 with
  an empty `Messages` array.
- Messages made eligible by a committed send, `AvailableAt`, or a visibility
  deadline may wake the request. Retention remains authoritative: a message at
  `ExpiresAt` is removed and is never returned by a waiter.
- `MaxNumberOfMessages`, visibility override, message-attribute projection,
  receipt generation, timestamps, and every response field retain their
  existing meanings. A successful claim is the same single repository
  transition used by short polling.
- Several consumers may wait on one queue. A notification may wake all of them,
  but it only indicates that a message might be eligible. Each consumer retries
  the authoritative atomic claim, so one message generation cannot be returned
  to more than one consumer during a positive visibility window.
- Waiters are process-local and are not durable. Restart discards them while all
  durable messages and lifecycle deadlines remain unchanged and are evaluated
  normally by the restarted process.

### 18.2 Notification, time, cancellation, and shutdown

The service registers a queue-scoped transient waiter only after an empty
claim, then performs another authoritative claim before blocking. This
claim/register/recheck sequence prevents a send between the first claim and
registration from being lost. Notifications are broadcast hints and may be
spurious. A wake-up always loops through a new claim.

The repository returns the earliest future lifecycle transition observed by an
empty claim. It computes that hint from the explicit claim timestamp and stored
`AvailableAt`, visibility, and retention deadlines while holding its normal
memory lock or bbolt transaction. It never reads wall-clock time. The service
waits without holding a repository lock or transaction and uses a local timer
only to schedule another timestamped claim. Timer firing itself never changes
queue state.

Client cancellation removes the local waiter and stops waiting. If cancellation
wins before a claim begins, no receive mutation is made. If an atomic claim has
already committed, cancellation does not roll it back and does not prove the
message was not received. Server shutdown broadcasts a process-local shutdown
signal, releases all long-poll waiters, and allows HTTP graceful shutdown to
finish before repository close. A waiter interrupted by server shutdown may
receive HTTP 503 `ServiceUnavailable` if its connection is still writable.

Storage failures encountered by any claim return HTTP 503
`ServiceUnavailable`; unexpected failures return HTTP 500 `InternalError`.
Existing response-envelope, request-ID, strict-JSON, and sensitive-data
redaction rules apply. Message bodies, attributes, receipts, and storage paths
are never included in long-poll errors or ordinary logs.

The semantic reference is the official AWS SQS
[ReceiveMessage](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html)
and
[short and long polling](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-short-and-long-polling.html)
documentation. SimQ adopts the wait range and return-when-available meaning, not
AWS's distributed server sampling, Query protocol, SDK wire behavior, or
queue-level wait attribute in this milestone.

## 19. M1-F batch message action contract

M1-F implements `SendMessageBatch`, `DeleteMessageBatch`, and
`ChangeMessageVisibilityBatch` through SimQ's strict JSON action routes. Each
request contains one `QueueUrl` and an `Entries` array with 1 through 10
objects. Every entry has a required request-local `Id` containing 1 through 80
ASCII letters, digits, hyphens, or underscores. IDs are case-sensitive and
unique within the request.

Missing, null, empty, or more-than-ten `Entries`, duplicate IDs, and invalid ID
syntax are request-level HTTP 400 errors and apply no entry. SimQ uses the
stable codes `EmptyBatchRequest`, `TooManyEntriesInBatchRequest`,
`BatchEntryIdsNotDistinct`, and `InvalidBatchEntryId` for those cases. Invalid
or missing `QueueUrl`, malformed JSON, unknown or duplicate JSON fields at any
level, wrong JSON types, fractional integers, trailing JSON, and an oversized
HTTP body are request-level errors under the existing strict JSON contract. A
well-formed URL for a missing queue is request-level HTTP 404
`QueueDoesNotExist`; no entry is attempted.

A structurally valid batch returns HTTP 200 even when some or all entries fail.
It contains `Successful` and `Failed` arrays plus `RequestId`. Each request ID
appears exactly once in one of the two arrays. Within each array, results retain
the relative order of their entries in the request. A failed entry has `Id`, a
stable `Code`, a non-sensitive `Message`, and `SenderFault`. Caller validation
and receipt errors set `SenderFault=true`; storage and unexpected internal
errors set it to false.

Batching is not a transaction across entries. Each entry reuses the matching
single-message service validation and one existing authoritative repository
mutation. In durable mode, an entry appears in `Successful` only after its own
bbolt transaction commits and synchronizes. Failure of one entry neither rolls
back a prior successful entry nor prevents a later valid entry from being
attempted. SimQ does not claim exactly-once behavior when a client retries an
ambiguous batch or entry.

### 19.1 SendMessageBatch

Request entries contain required `Id` and `MessageBody`, with optional
`DelaySeconds` and `MessageAttributes` having exactly the meanings defined for
`SendMessage`.

Every success contains `Id`, `MessageId`, `MD5OfMessageBody`, and optional
`MD5OfMessageAttributes`. Body, delay, attribute validation, queue-default
snapshot selection, immutable storage, and waiter notification are identical
to `SendMessage`. In addition to the 1 MiB per-message limit, the sum of every
entry's body bytes plus message-attribute name, complete type, and decoded value
bytes must be at most 1,048,576 bytes. Exceeding that aggregate is request-level
HTTP 400 `BatchRequestTooLong` and enqueues nothing.

### 19.2 DeleteMessageBatch

Each entry contains required `Id` and `ReceiptHandle`. A current handle deletes
its message; issued stale and consumed handles are successful no-ops; malformed,
unissued, or cross-queue handles are entry failures with
`ReceiptHandleIsInvalid`. Each success contains only `Id`. Current/stale checks,
active-order removal, and durable commit use the existing single-delete
repository transition.

### 19.3 ChangeMessageVisibilityBatch

Each entry contains required `Id`, `ReceiptHandle`, and JSON integer
`VisibilityTimeout` from 0 through 43,200. The field is required in SimQ even
where some AWS models describe it as optional. Each entry obtains one service
clock timestamp when it is attempted and sets its deadline to that timestamp
plus its timeout, exactly like `ChangeMessageVisibility`. Current, stale,
consumed, malformed, unissued, and cross-queue receipt meanings are unchanged.
Each success contains only `Id`; a committed successful change broadcasts the
same harmless eligibility hint as the single action.

The semantic reference is the official AWS SQS documentation for
[SendMessageBatch](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SendMessageBatch.html),
[DeleteMessageBatch](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_DeleteMessageBatch.html),
[ChangeMessageVisibilityBatch](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ChangeMessageVisibilityBatch.html),
their request-entry types, and `BatchResultErrorEntry`. SimQ adopts the maximum
entry count, request-local IDs, per-entry result model, send aggregate size, and
single-action meanings, not AWS Query protocol, SDK wire behavior, SigV4,
FIFO-only fields, KMS errors, throttling, or AWS response ordering.

## 20. Open decisions

The following decisions are intentionally deferred and require an ADR before
implementation:

- Idempotency key format and retention period.
- Receipt handle encoding and key rotation.
- Queue URL behavior when `PublicBaseURL` changes.
- Dynamic shard-ID catalog changes and selective shard hosting.
- Standalone export and coordinated multi-shard restore semantics.
