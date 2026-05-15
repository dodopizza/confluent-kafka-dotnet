# Diagnostic Logging Reference

All log points use the existing `LogAssignmentDiagnostic` infrastructure (facility: `ASSIGNDIAG`).
They are only emitted when a `SetLogHandler` is configured on the `ConsumerBuilder`.

## Log Points

| Stage tag | Where | What it captures |
|---|---|---|
| `rebalance-revoke-input` | Revoke path entry | `assignmentLost` flag, which handlers exist, partition list |
| `rebalance-revoke-no-handler` | No-handler fast path | Protocol used for unassign |
| `rebalance-revoke-position-failed` | Position fetch fallback | **Which partition** failed and why (was a silent `catch`) |
| `rebalance-revoke-calling-handler` | Before handler call | Which handler type (lost vs revoked), positions |
| `rebalance-revoke-before-incremental-unassign` | Cooperative unassign | Partition list being unassigned |
| `rebalance-revoke-before-unassign` | Eager unassign | Entry marker |
| `rebalance-exception-emergency-unassign` | Catch block | **Exception type + message** (was previously silent) |
| `rebalance-emergency-cooperative-unassign` | Cooperative emergency | Current assignment being dropped |
| `rebalance-emergency-unassign-failed` | Double-fault | Inner exception from unassign attempt |
| `rebalance-emergency-eager-unassign` | Eager emergency | Entry marker |
| `rebalance-callback-complete` | **Post-rebalance snapshot** | **Final assignment after every rebalance** + hasHandlerException |
| `consume-surfacing-handler-exception` | `Consume()` throws | Exception being surfaced to app |
| `consumer-close-start` / `consumer-close-completed` | `Close()` | Lifecycle markers |
| `consumer-close-surfacing-handler-exception` | `Close()` exception | Exception from prior rebalance |
| `consumer-incremental-unassign` | `IncrementalUnassign()` | Partition list (was unlogged) |
| `consumer-unassign` | `Unassign()` | Entry marker (was unlogged) |

## Key log point for debugging "lost partitions"

The most valuable log point is `rebalance-callback-complete` — it logs the **actual partition assignment
after every rebalance finishes**. If a partition silently disappears, you will see exactly when and
which rebalance caused the assignment to change.

### What to grep for

```bash
# See all rebalance completions and resulting assignments
grep "rebalance-callback-complete" logs.txt

# See any emergency unassign events (exceptions during rebalance)
grep "rebalance-exception-emergency-unassign" logs.txt

# See exceptions being surfaced to the app from Consume()
grep "consume-surfacing-handler-exception" logs.txt

# See position fetch failures during revoke (silent offset loss)
grep "rebalance-revoke-position-failed" logs.txt

# See Close() lifecycle
grep "consumer-close" logs.txt
```

## Source locations

All logging is in `src/Confluent.Kafka/Consumer.cs` within:
- `RebalanceCallback()` — rebalance lifecycle logging
- `Consume()` — exception surfacing
- `Close()` — shutdown lifecycle
- `IncrementalUnassign()` / `Unassign()` — assignment changes
