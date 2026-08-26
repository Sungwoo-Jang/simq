# ADR 0004: Long-poll coordination outside durable storage

- Status: Accepted
- Date: 2026-08-23
- Scope: M1-E

## Context

M1-E adds `ReceiveMessage.WaitTimeSeconds`. A waiter must react to committed
enqueue, delayed availability, and visibility expiry without holding the memory
repository mutex or a bbolt transaction. A notification cannot grant ownership:
several consumers can observe the same event, while the existing repository
claim remains the only authoritative selection and mutation boundary.

The official Amazon SQS `ReceiveMessage` and long-polling documentation defines
a request wait of zero as short polling, a positive wait as long polling, a
maximum of 20 seconds, and return when messages become available. SimQ adopts
those meanings through its strict JSON API. AWS distributed server sampling,
Query protocol, SDK compatibility, and SigV4 are out of scope.

## Decision

### Request-only wait setting

M1-E supports only the request field `WaitTimeSeconds`, from 0 through 20.
Omission means zero. The durable queue attribute
`ReceiveMessageWaitTimeSeconds` is not added. The current queue record remains
unchanged and schema/record version 3 remains authoritative. Queue-level wait
defaults can be added later only with their own public contract, atomic-setting
tests, schema decision, and migration if needed.

### Transient broadcast registry

The queue service owns an in-process, queue-keyed broadcast registry. An empty
receive follows this sequence:

1. Run the ordinary authoritative claim with an explicit service-clock
   timestamp.
2. Register a transient waiter for that queue.
3. Run the same claim again.
4. If still empty, block on the queue notification, a lifecycle timer, request
   cancellation, polling deadline, or service-shutdown signal.
5. Remove the waiter and loop through the authoritative claim after any wake.

The second claim closes the lost-wakeup window between the first empty claim and
waiter registration. Notifications close a generation channel and wake every
waiter for that queue. They are hints and can be spurious. Enqueue notifies only
after its repository mutation succeeds. A successful visibility change also
notifies because it can shorten a deadline; stale or consumed no-op changes may
produce a harmless spurious wake.

### Lifecycle transition hints

`Repository.Claim` returns claimed messages plus the earliest future transition
observed during an empty scan. For a delayed record this is the earlier of
`AvailableAt` and `ExpiresAt`; for an in-flight record it is the earlier of its
visibility deadline and `ExpiresAt`. The memory implementation computes the
hint under its existing claim mutex. The bbolt adapter computes it in the same
`DB.Update` that removes expired records and attempts the claim.

The repository receives `Now` in the command and never reads the wall clock.
The service closes the transaction before waiting, schedules a local timer for
the earlier of the transition and polling deadline, obtains a new command
timestamp when the timer fires, and claims again. Timer callbacks never mutate
repository state. A controllable timer factory permits exact fake-clock tests
without correctness sleeps.

### Cancellation and shutdown

HTTP passes the request context into the service. Cancellation before a claim
prevents that claim; cancellation racing with an already committed claim does
not roll it back. Unsubscription is idempotent and affects only transient
waiter state.

The service has an idempotent long-poll shutdown signal. The HTTP server invokes
it when graceful shutdown starts, waking waiters before repository close. A
still-writable request receives the established service-unavailable response;
connection cancellation may instead prevent a response. Durable message state
is unchanged in either case. Process restart deliberately restores no waiters.

## Alternatives considered

- Polling the repository on a fixed interval is simple but adds empty write
  transactions, avoidable latency, and scheduler-dependent tests.
- Holding a read or write transaction while waiting blocks bbolt progress or
  mmap maintenance and violates the transient-waiter invariant.
- Giving repositories condition variables or channels mixes process-local
  orchestration with durable adapters and makes restart semantics ambiguous.
- A persistent waiter table adds recovery and client-identity questions with no
  durable queue-state benefit; waiters are connection-scoped and transient.
- A queue-level wait default would require widening the durable queue contract
  and potentially schema migration. It is unnecessary for request-level M1-E.

## Consequences

- Memory and bbolt retain the same atomic claim boundary and never stay locked
  while a client waits.
- Broadcasts can wake more consumers than available messages, so every wake can
  perform an empty claim. Correctness is preserved; future performance work may
  add bounded wake policies without granting ownership outside the repository.
- The existing ordered scan remains O(n) and now also computes an O(n) earliest
  transition hint. No new durable index or schema migration is introduced.
- A process crash loses active HTTP waits, but all acknowledged messages,
  claims, delays, visibility, retention, receipts, and attributes remain in
  schema v3 and are evaluated after restart.
- No dependency is added. The implementation uses the standard library for
  contexts, timers, synchronization, and broadcast channels, retaining only the
  bbolt dependency justified by ADR 0001.

## References

- [AWS SQS ReceiveMessage](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html)
- [AWS SQS short and long polling](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-short-and-long-polling.html)
