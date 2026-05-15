# Partition Assignment / Rebalance Analysis

## Context
Investigating consumer loss during Kubernetes HPA scale-up/down events.
During HPA, pods start or get SIGTERM'd, triggering group rebalances.

## Call Chain Summary
```
User code / librdkafka callback
  └─ Consumer.RebalanceCallback()          (line 190)
       ├─ Local_AssignPartitions path       (line 217)
       │    ├─ no handler → Assign/IncrementalAssign           (219-229)
       │    └─ handler present → user handler → Assign/IncAssign (232-271)
       └─ Local_RevokePartitions path       (line 276)
            ├─ no handler → Unassign/IncrementalUnassign       (278-289)
            └─ handler present → user handler → Unassign/IncUn (292-327)
       catch(Exception) → emergency Unassign + store handlerException (333-354)

Consumer.Consume()
  └─ kafkaHandle.ConsumerPoll() → may trigger RebalanceCallback
  └─ checks handlerException → rethrows (823-831)

Consumer.Close()
  └─ kafkaHandle.ConsumerClose() → may trigger final RebalanceCallback
  └─ checks handlerException → rethrows (617-622)
  └─ Dispose(true)
```

---

## Analysis Table

| # | Place | Source | Explanation | Potential Problem (K8s HPA context) | Severity |
|---|-------|--------|-------------|--------------------------------------|----------|
| 1 | **`Dispose()` without `Close()`** | `src/Confluent.Kafka/Consumer.cs#L641-L644` | `Dispose()` just destroys the native handle. No offset commit, no group leave. Comment says rebalance happens after `session.timeout.ms`. | **If K8s sends SIGTERM and your shutdown code calls `Dispose()` without `Close()`, the consumer "disappears" silently.** The broker won't know for `session.timeout.ms` (default 45s). During that window, the dead consumer still "owns" partitions — no other consumer reads them. **This is the #1 suspect for "lost consumers".** | 🔴 **Critical** |
| 2 | **`Close()` exception path** | `src/Confluent.Kafka/Consumer.cs#L613-L626` | `Close()` calls `ConsumerClose()` (commits + leave group), then checks `handlerException`. If a handler threw during a *prior* `Consume()` rebalance, the exception is stored and rethrown here. | If your app catches the exception from `Close()` and doesn't retry/handle it, the consumer may not cleanly leave the group. Also, `Dispose(true)` is called *after* the rethrow on line 621, meaning **if `handlerException` is set, `Dispose` never runs** (thrown before line 624). Resource leak. | 🟠 **High** |
| 3 | **`handlerException` is not volatile** | `src/Confluent.Kafka/Consumer.cs#L88` | `private Exception handlerException = null;` — plain field. Written in rebalance callback (librdkafka thread), read in `Consume()` (user thread). | Without `volatile` or a memory barrier, the user thread may never see the exception set by the rebalance callback due to CPU cache coherence. On x86 this is *mostly* safe due to strong memory model, but it's formally a **data race**. Could cause a rebalance error to be silently ignored, leaving consumer in a bad state. | 🟡 **Medium** |
| 4 | **Exception in rebalance handler → emergency unassign** | `src/Confluent.Kafka/Consumer.cs#L333-L354` | If *any* exception occurs in the rebalance callback, the catch block does an emergency `IncrementalUnassign(Assignment)` (cooperative) or `Unassign()` (eager). Exception stored in `handlerException`. | The emergency unassign drops **all** partitions. If the original exception was transient (e.g., a flaky `PositionTopicPartitionOffset` call in the revoke path, line 297), the consumer loses everything. The exception is only surfaced on the *next* `Consume()` call, so there's a window where the consumer sits partition-less. In K8s, if this triggers during HPA-induced rebalance, the consumer is effectively dead until the app restarts. | 🟠 **High** |
| 5 | **`PositionTopicPartitionOffset` failure during revoke** | `src/Confluent.Kafka/Consumer.cs#L292-L302` | During revoke, the code fetches current position for each partition. If this throws, it falls back to `Offset.Unset`. | Silent offset loss on revoke. The revoked handler gets `Offset.Unset` for the failed partitions, making it impossible to commit accurate offsets before yielding. Could cause **duplicate processing** after rebalance — not consumer loss, but data correctness issue during HPA events. | 🟡 **Medium** |
| 6 | **Cooperative rebalance validation in assigned handler** | `src/Confluent.Kafka/Consumer.cs#L244-L262` | After calling the user's `partitionsAssignedHandler`, the code validates the returned set matches the input set (count + sorted comparison). If not → exception → emergency unassign (via catch on 333). | If user handler accidentally filters or reorders partitions, the cooperative validation throws, triggering full emergency unassign. **All partitions lost**, not just the ones being assigned. A subtle user-code bug that only manifests during rebalance. | 🟡 **Medium** |
| 7 | **`assignCallCount` reentrancy check** | `src/Confluent.Kafka/Consumer.cs#L232-L242` | Detects if user called `Assign()`/`Unassign()` inside the rebalance handler (which is forbidden). If detected → exception → emergency unassign. | Correct behavior, but the error is surfaced as `handlerException` on the *next* `Consume()` — not immediately. If user code doesn't handle `Consume()` exceptions properly, the consumer goes silent. | 🟢 **Low** |
| 8 | **`IsClosed` check at top of rebalance callback** | `src/Confluent.Kafka/Consumer.cs#L199-L205` | If `kafkaHandle.IsClosed` is true during a rebalance callback, it throws. This is described as "something is badly wrong". | During aggressive K8s shutdown (SIGKILL after grace period), the native handle could be disposed while a rebalance is in flight. This throw goes into the catch block on line 333, which tries `Unassign()` on an already-closed handle — likely **double fault**. In practice this is a shutdown race and the process is dying anyway, but it could hang the shutdown. | 🟡 **Medium** |
| 9 | **`ConsumerClose()` triggers rebalance callback** | `src/Confluent.Kafka/Impl/SafeKafkaHandle.cs#L767-L776` | `rd_kafka_consumer_close()` triggers a final revoke/lost rebalance before leaving the group. The callback runs synchronously inside `Close()`. | If the final rebalance callback takes too long (e.g., slow offset commit in the handler), K8s may SIGKILL the pod before it completes. This means the consumer **never cleanly leaves the group** — same effect as not calling `Close()` at all (problem #1). | 🟠 **High** |
| 10 | **No timeout on `Close()` itself** | `src/Confluent.Kafka/Consumer.cs#L613-L626` | `Close()` has no timeout parameter. It blocks until `rd_kafka_consumer_close()` completes, which internally waits for the final rebalance and offset commit. | In K8s, `terminationGracePeriodSeconds` is your only backstop (default 30s). If `Close()` takes longer (e.g., broker is slow to respond to LeaveGroup), the pod gets SIGKILL'd, and the consumer is "lost" for `session.timeout.ms`. **You have no way to set a deadline.** | 🟠 **High** |
| 11 | **`session.timeout.ms` / `max.poll.interval.ms` defaults** | `src/Confluent.Kafka/Config_gen.cs#L1791-L1859` | `session.timeout.ms` default=45s, `max.poll.interval.ms` default=300s. If a consumer is killed without `Close()`, other consumers must wait for these timeouts before rebalancing. | During HPA scale-down, if pods are killed non-gracefully, the **dead consumer's partitions are frozen** for up to 45 seconds. During scale-up, new consumers join immediately but existing consumers won't rebalance until the coordinator decides to. The 300s `max.poll.interval.ms` means a stuck consumer (GC pause, slow processing) can hold partitions for 5 minutes before the broker forces a rebalance. | 🔴 **Critical** |
| 12 | **`errorHandler` swallows exceptions** | `src/Confluent.Kafka/Consumer.cs#L92-L107` | The `ErrorCallback` catches and discards any exception from the user's error handler. | If the error handler is your only observability into broker disconnects/fatal errors, and it throws (e.g., logging framework crash), you lose visibility. Not a direct cause of consumer loss, but means you may not *know* about it. | 🟢 **Low** |
| 13 | **Eager rebalance: `Assign()` called with handler result** | `src/Confluent.Kafka/Consumer.cs#L268-L271` | In eager protocol, the assigned handler's return value is passed directly to `Assign()`. The user can return a *different* set than what the broker assigned. | If user handler returns fewer partitions, those partitions become orphaned until the next rebalance. Not specific to HPA, but during frequent rebalances this compounds. | 🟡 **Medium** |
| 14 | **`Assignment` property queries native handle** | `src/Confluent.Kafka/Consumer.cs#L405-L406` | `Assignment` calls `rd_kafka_assignment()` — a native call that reads current state. Used in the catch block (line 337) for cooperative emergency unassign. | During an error recovery in the rebalance callback, the `Assignment` read could return stale or empty data if the native state is already partially updated. The `IncrementalUnassign(Assignment)` on line 341 could then be a no-op (empty list) or miss partitions, leaving the consumer in an inconsistent state. | 🟡 **Medium** |

---

## Most Likely Root Cause for "Lost Consumers" During HPA

**Primary suspect: #1 + #11 (non-graceful shutdown + session timeout)**

When K8s HPA scales down:
1. Pod gets `SIGTERM`
2. App starts shutdown, but either:
   - Calls `Dispose()` instead of `Close()` → consumer vanishes without LeaveGroup
   - Calls `Close()` but it takes too long (#10) → SIGKILL after grace period → same result
3. Broker waits `session.timeout.ms` (45s default) before triggering rebalance
4. **Result: partitions are unread for up to 45 seconds**

**Secondary suspect: #4 + #9 (exception during rebalance → all partitions lost)**

During HPA-induced rebalance:
1. New pod joins → broker triggers rebalance on existing consumers
2. Existing consumer's rebalance handler throws (e.g., slow commit, transient error)
3. Emergency unassign drops ALL partitions
4. Exception stored, consumer sits idle until next `Consume()` surfaces it
5. If app doesn't handle `Consume()` exceptions → consumer is dead

## Recommendations
1. **Always call `Close()`, never just `Dispose()`** — wrap in `CancellationToken` timeout
2. **Set `session.timeout.ms` lower** (e.g., 10-15s) to reduce dead-consumer window
3. **Use `group.instance.id`** (static membership) to avoid rebalances during rolling restarts
4. **Consider cooperative-sticky assignor** to minimize partition movement during scale events
5. **Keep rebalance handlers fast and exception-free** — no I/O, no blocking
6. **Set K8s `terminationGracePeriodSeconds`** > expected `Close()` duration
