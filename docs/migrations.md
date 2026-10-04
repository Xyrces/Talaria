# Migration guide

## Versioned saga and inbox persistence

Saga transitions now use an optimistic versioned contract. A transition carries the expected state version and the triggering `MessageId`; stores report conflicts so Talaria can reload and retry the transition without applying stale state. Inbox receipts are scoped to application, endpoint identity, and message ID, and their retention is bounded by the configured persistence policy. Keep handlers idempotent because transports and outbox relays provide at-least-once delivery.

## Redis cutover

Treat the versioned saga and inbox keys as a new persistence format. Stop or drain old application instances and allow in-flight handlers and outbox relays to finish before deploying the new version. Then deploy all instances against the new schema/key layout and validate traffic before retiring old data. Do not assume the new version can read, migrate, or safely coexist with legacy unversioned Redis keys. If old saga state must be retained, export and transform it explicitly while both formats are quiescent; preserve message deduplication history only through a deliberate migration plan.

Use separate Redis key prefixes for environments and avoid sharing a prefix between old and new versions during cutover. Keep a recoverable backup until validation is complete.

Drain or explicitly export and transform every pending outbox entry and deferred delivery before retiring the old format. Stopping active handlers does not drain retries scheduled for a future time; the new deployment will not discover those legacy entries automatically.

## Endpoint identity changes

Minimal messaging endpoints derive destinations from the full CLR message contract name, normalized and suffixed with a stable hash. Commands share a command consumer identity for that destination. Event consumer identity includes the application name; `WithName` adds a subscriber name for independent event subscriptions. Renaming or moving a message type, changing `ApplicationName`, changing a destination, or changing an event subscriber name can create new broker entities and consumer offsets/receipts. Plan these changes as routing migrations: provision the new entity, deploy publishers and subscribers in a coordinated sequence, drain old traffic, and retire old entities only after verifying no required backlog remains.

## RabbitMQ subscription queues

RabbitMQ subscription queues now identify both the topic and consumer group, so reusing a group on different topics cannot mix their deliveries. The physical name is `talaria.subscription.` followed by the lowercase SHA-256 hex digest of the UTF-8 string `${topic.Length}:${topic}${group.Length}:${group}` (lengths are .NET string lengths). Command queue names stay the same.

Existing group-only subscription queues are not automatically renamed or drained. Pause publishers, drain the old queues with the old deployment, stop the old consumers, provision the new topology, and start the new consumers before resuming publishers. Retire old queues and bindings after verifying the cutover. Externally managed topology must use the same topic/group naming rule.

## SQL Server

SQL persistence uses the application's `DbContext`. Add `modelBuilder.AddTalaria()` to its model and apply the resulting EF migration. `WithTransaction()` commits application writes, receipt, and outbox rows atomically for endpoint handler execution. For application initiated sends outside that handler transaction, call `SaveChangesAsync` to persist the scoped outbox rows before expecting relay delivery.

Talaria's five infrastructure tables and `TalariaMessageLeaseSequence` explicitly use the `dbo` schema, even when the application configures another default schema. Business entities retain the application's schema. If an earlier migration created Talaria objects in another schema, generate and inspect the migration that moves those objects to `dbo`, preserving existing rows and the sequence value before restarting workers. Outbox and deferred-message acquisition support databases with `READ_COMMITTED_SNAPSHOT` enabled.

SQL receipt expiry controls duplicate suppression; it does not run a background table cleanup. Schedule maintenance to remove expired inbox and saga-history receipts, using their `ExpiresAt` values. Retained failures remain until explicitly deleted or successfully replayed. Monitor these tables and retain receipts for at least the period in which old broker deliveries may return.

## Saga and inbox store contracts

Saga subscriptions honor the message-type header even when they have only one local step. A tagged message registered for another saga on the same topic is acknowledged without invoking a local handler. An unrecognized type is dead-lettered as `unknown_message_type`. For legacy untagged messages, a single local step remains the fallback; subscriptions with multiple local steps require a type header. Publish typed messages when sharing topics between sagas.

Custom `IStateStore<TState>` implementations must now support `ReadSnapshotAsync(correlationId, messageId)` and conditional `TryTransitionAsync` with an expected version. The snapshot identifies whether the triggering message was already processed; transitions return `Committed`, `AlreadyProcessed`, or `Conflict`. Do not implement these as an unconditional read followed by a write: concurrent handlers must not overwrite one another. Inbox idempotency keys are scoped by application/consumer identity and message ID, and store fencing ownership must prevent an expired worker from completing another worker's claim. Use the built-in provider bundles when possible.
