# OMP lane Run boundary: test map

This map covers each OMP test that was changed or removed when lane Runs moved to OMP 18.11.0's own `prompt_result` boundary (owner decision 2026-10-15; see the [OMP product facts](../products/omp.md#lane-run-boundary-on-omp-18110-2026-10-19)). Names before the change refer to the tree at `dc24697`. Files are under `wrappers/omp/`.

## Removed or renamed

| Test before | Observable behavior then | Replacement or disposition |
|---|---|---|
| `TestOMPManagedExtensionFailureAndPostTerminalWorkRetireRPC` | A managed extension failure outside or inside a Run retires the lane. An `agent_start` after the owned terminal retires future reuse but keeps the snapshotted result. | Renamed `TestOMPManagedExtensionFailureRetiresRPC` in `run_test.go`. Its managed-failure cases are unchanged. The `agent_start` after the Run is now OMP's own work by owner decision, so the case asserts the opposite: own work is ignored and a later managed failure still retires the transport. The new behavior is pinned by `TestOMPRunEndsAtItsOwnPromptResultAndIgnoresOwnWork`. |
| `TestOwnerRegistryWaitsForNextMatchingPreflightAndConsumesAccounting` | A waiter takes the next matching preflight and keeps only the consumed token's accounting. | `TestOwnerRegistryPreflightWindowTakesMatchingPreflight` in `owner_registry_test.go`: an open window takes its matching preflight, wakes its observer and keeps the same accounting. The window then closes. |
| `TestOwnerRegistryPreflightWaitDoesNotSearchPastForeignWitness` | A foreign preflight at the head is a registry protocol failure, and the registry does not search past it. | Changed by owner decision. `TestOwnerRegistryPreflightWindowRecordsForeignBeforeOurs`: the foreign preflight is recorded (the count is 1), not taken as ours, and no longer retires the registry. The Run fails instead: `TestOMPRunRejectsForeignLeadingPreflight` (unchanged assertion, now a turn failure) and `TestOMPRunFailsAtItsPromptResultWithoutItsOwnPreflight`. |
| `TestOwnerRegistryPreflightWaitWakesOnCancellationAndOwnerEnd` | A blocked waiter returns on context cancellation and on owner end. | Owner end: `TestOwnerRegistryPreflightWindowEndsWithOwnerAndRegistry/owner end`. Cancellation is now the Run's own select (`startNativeTurn`), covered by `TestOMPWorkerInterruptsSubmittedDeliveryBeforePreflight`. |
| `TestOwnerRegistryCanceledPreflightWaitPreservesArrivedEvidence` | A cancelled waiter leaves an arrived preflight queued for a later wait. | Retired by design: a queued preflight is what failed the next Run when OMP's own work had preflighted between Runs. Every preflight is now consumed on arrival. `TestOwnerRegistryPreflightOutsideWindowNeverReachesALaterRun` and `TestOMPOwnWorkPreflightBetweenRunsDoesNotReachTheNextRun` pin that, and that a reused token is still refused. |
| `TestOwnerRegistryFailedPreflightWaitPreservesArrivedEvidence` | A registry failure ends the wait with that failure, and the evidence stays queued. | The failure: `TestOwnerRegistryPreflightWindowEndsWithOwnerAndRegistry/registry failure`, also for opening a new window. The queued evidence: retired as above. |

## Changed in place

| Test | Change |
|---|---|
| `TestOMPRunJoinsLatePreflightAndCurrentTerminalAssistant`, `TestOMPRunCancelsOwnedRuntimeUIAndJoinsWrite`, `TestOMPAdmittedInterruptJoinsAbortAndPreservesNextRun` | Each Run now also gets our `prompt_result`, which ends it. Assertions are unchanged. |
| `TestOMPTerminalCancelsAndJoinsUnsettledUIBeforeResult` | The result now needs our `prompt_result`. The `foreign retirement` case uses a managed extension failure after the result, because OMP's own work no longer retires. The joined-dialog assertions are unchanged. |
| `TestOMPHeldCompletionWaitKeepsTerminalAcrossForeignRetirement` | It records our preflight and `prompt_result`. OMP's own `agent_start` after the result is accepted. The retirement comes from a managed failure; the held-wait assertions are unchanged. |
| `TestOMPImmediateNoAgentIsFiniteWithoutBorrowedAssistant` | It checks that no window or consumed preflight is left, instead of the removed queue. |
| `TestOwnerRegistryConsumedEvidenceDoesNotImposeLifetimeTurnLimit` | Preflights outside a window are consumed on arrival, in place of the removed `takePreflight`. The bound assertions are unchanged. |
| `TestOMPAssistantDecoderRejectsUnknownStopAndUsesTextParts` | `decodeOMPTerminal` became `decodeOMPYield`, with OMP's `yielded ?? isTerminal` rule and an invalid `yielded` flag. |

## New

`rpc_test.go`: `TestNativeRPCRefusedPromptAbsorbsItsTrailingSuccessAndNoAgentResult`.

`run_test.go`:
- `TestOMPRefusedPromptKeepsTheLaneForTheNextRun`;
- `TestOMPWorkerRefusedDeliveryIsNotSubmittedAndLaneStays`;
- `TestOMPOwnWorkOutsideARunKeepsTheLane`;
- `TestOMPRunEndsAtItsOwnPromptResultAndIgnoresOwnWork`;
- `TestOMPOwnWorkPreflightBetweenRunsDoesNotReachTheNextRun`;
- `TestOMPPreflightAfterOursInTheRunIsConsumed`;
- `TestOMPRunFailsAtItsPromptResultWithoutItsOwnPreflight`.

`interactive_arguments_test.go`: `TestOMPCommandsMatchNativeRegistry`.

## Weaker coverage

None found. The removed queue-preservation cases test behavior that the owner decision removes.
