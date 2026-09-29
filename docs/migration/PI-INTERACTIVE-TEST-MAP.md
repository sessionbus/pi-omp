# Pi interactive test migration

This map covers every Go test removed by the direct in-process Peer migration.
Test names refer to the pre-migration tree at `04224db6`. Replacement names are
the exact test names in the migrated tree.

## Interactive owner

| Removed Go test | Observable behavior | Replacement or disposition |
|---|---|---|
| `TestInteractiveOwnerPublishesRebindsAndRoutesExactToolIdentity` | Publish the native ID/name/groups/cwd, route a tool call with the exact session and call IDs, and bind a replacement session with fallback naming. | `wrappers/pi/peer.test.mjs`: `direct owner publishes, rehellos, replaces, and routes exact tool identity`; `direct owner uses the actual kit for hello, delivery, and Caller action` |
| `TestInteractiveOwnerReconnectsLatestIdentityWithoutReplayAndSessionEndStopsOldLifetime` | Reconnect with the latest identity, fail an in-flight call without replay, gate new calls until re-admission, and prevent an ended generation from acting on its successor. | `wrappers/pi/peer.test.mjs`: `actual kit reconnect gates Caller work and never replays retained delivery`; `session end while disconnected cancels the ended generation reconnect`; `same-ID native replacement rotates the Peer and rejects its old generation` |
| `TestInteractiveOwnerSupersededIsTerminal` | An admitted `session.superseded` makes the owner terminal and reports the superseded cause. | `wrappers/pi/peer.test.mjs`: `supersession and close join the direct owner lifetime` |
| `TestInteractiveOwnerSupersededOwnsTerminationBeforeHeldAck` | Supersession owns termination before its acknowledgement write completes; replacement and reconnect cannot cross that boundary. | `wrappers/pi/peer.test.mjs`: `supersession and close join the direct owner lifetime`; lower-level acknowledgement ordering is now owned by bundled `@sessionbus/kit` (see weaker assertions below). |
| `TestInteractiveOwnerCloseJoinsHeldSupersededAck` | Close joins work retained by a supersession acknowledgement. | `wrappers/pi/peer.test.mjs`: `supersession and close join the direct owner lifetime`; `close cancels and joins an admitted actual-kit Caller`; exact acknowledgement-write retention is delegated to `@sessionbus/kit`. |
| `TestInteractiveOwnerIgnoresSupersededBeforeHelloAdmission` | A stale, unadmitted connection cannot supersede the current native owner. | Retired: the approved design removes Pi's protocol implementation and adopts the canonical kit Peer admission contract. Pi no longer interprets pre-admission protocol requests independently; this is called out below as weaker Pi-local coverage. |
| `TestInteractiveOwnerSessionEndWhileDisconnectedStopsReconnect` | Ending a disconnected native generation cancels its scheduled reconnect. | `wrappers/pi/peer.test.mjs`: `session end while disconnected cancels the ended generation reconnect` |
| `TestInteractiveOwnerCloseJoinsAbsentDaemonReconnect` | Close cancels an absent-daemon retry and settles pending readiness without a later dial. | `wrappers/pi/peer.test.mjs`: `close cancels an absent-daemon retry and joins pending readiness` |
| `TestInteractiveOwnerPreservesFIFOAcrossBusyAndIdleDeliveries` | Busy deliveries retain FIFO order and duplicate message IDs, drain once, and idle delivery writes directly with the structured native envelope. | `wrappers/pi/peer.test.mjs`: `direct owner preserves FIFO busy wake, capacity, and tree gate`; `native delivery rendering preserves structured sender and closes envelope injection`; `wrappers/pi/extension.test.mjs`: `interactive drains before a prompt and after a settled turn through nested native appends` |
| `TestInteractiveOwnerRejectsUnobservableBranchSummaryBeforeRetention` | A delivery during an unobservable branch-summary window is rejected before retention, then ordinary delivery recovers. | `wrappers/pi/peer.test.mjs`: `direct owner preserves FIFO busy wake, capacity, and tree gate`; `wrappers/pi/extension.test.mjs`: `interactive wakes after manual compaction and brackets unobservable branch summaries` |
| `TestInteractiveOwnerCancelsTreeBeforeRetainedWorkAndGatesFastPath` | Retained work cancels tree navigation, the tree gate rejects a racing fast-path delivery, and the retained item drains before recovery. | `wrappers/pi/peer.test.mjs`: `direct owner preserves FIFO busy wake, capacity, and tree gate`; `wrappers/pi/extension.test.mjs`: `interactive wakes after manual compaction and brackets unobservable branch summaries` |
| `TestInteractiveOwnerCanceledDeliveryBeforeAdmissionDoesNotRetireOwner` | A canceled delivery makes no native submission and does not poison the owner. | `wrappers/pi/peer.test.mjs`: `canceled and poisoned delivery cannot corrupt retained FIFO` |
| `TestInteractiveOwnerOldGenerationCannotUseSameIDReplacement` | Reusing a durable native ID still rotates the owner generation; the prior generation cannot act through the replacement. | `wrappers/pi/peer.test.mjs`: `same-ID native replacement rotates the Peer and rejects its old generation`; `replacement rejects an in-flight busy result from the prior native generation` |
| `TestInteractiveOwnerCloseJoinsUnadmittedHelloWatcher` | Close settles an outstanding hello that has not been admitted. | `wrappers/pi/peer.test.mjs`: `close joins an unadmitted actual-kit hello` |
| `TestInteractiveOwnerRejectsPoisonedDeliveryBeforeExistingQueue` | Invalid delivery identity/body is rejected before it can alter an older retained FIFO item. | `wrappers/pi/peer.test.mjs`: `canceled and poisoned delivery cannot corrupt retained FIFO`; `wrappers/pi/native.test.mjs`: `delivery identity and body bounds reject before native submission` |

## Trace result

| Removed Go test | Observable behavior | Replacement or disposition |
|---|---|---|
| `TestInteractiveSpawnResultPreservesTrace` | A tool-originated spawn returns the daemon's exact child policy, including `trace`, without rewriting it. | `wrappers/pi/peer.test.mjs`: `direct owner uses the actual kit for hello, delivery, and Caller action` |

## Interactive launcher

| Removed Go test | Observable behavior | Replacement or disposition |
|---|---|---|
| `TestPiInteractiveNativeChild` | Subprocess helper for the old Go-owned native child, private `owner.sock`, signal forwarding, bridge loss, and exit-code fixtures. | Retired: interactive mode now validates an exec plan and calls `syscall.Exec`; there is no Go child, private socket, launch directory, or signal-forwarding owner to emulate. The surviving exec boundary is covered by `wrappers/pi/interactive_launch_test.go`: `TestPiInteractiveExecPlanIsExactAndLeavesNoLauncherArtifact` and `cmd/pi-peer/main_test.go`: `TestPiTTYRoutesManagedAndNativeInvocations`. |
| `TestRunPiInteractiveWaitsForInitiallyAbsentDaemonWithoutRestartingNative` | A missing daemon does not restart the native generation; later hello succeeds and the private launch directory is removed. | `wrappers/pi/peer.test.mjs`: `close cancels an absent-daemon retry and joins pending readiness`; `actual kit reconnect gates Caller work and never replays retained delivery`. Native PID and directory assertions are retired because the Peer now runs inside the exec'd native and no launch directory exists. |
| `TestRunPiInteractiveOwnsBridgeIdentityAndGracefulSignalCleanup` | Exact public identity and native argv/env, wrapper-to-child signal forwarding, and launch-directory cleanup. | Identity: `wrappers/pi/peer.test.mjs`: `direct owner uses the actual kit for hello, delivery, and Caller action`. Argv/env/no residue: `wrappers/pi/interactive_launch_test.go`: `TestPiInteractiveExecPlanIsExactAndLeavesNoLauncherArtifact`; `cmd/pi-peer/main_test.go`: `TestPiTTYRoutesManagedAndNativeInvocations`. Signal forwarding is retired because `syscall.Exec` leaves the native as the signal recipient. |
| `TestPiInteractiveBridgeResultOnlyNormalizesJoinedGracefulQuit` | Normalize expected bridge EOF only after graceful quit while preserving protocol and child failures. | Retired: the interactive private bridge and Go child join were removed. `wrappers/pi/extension.test.mjs`: `an idle owner connection ending retires the native session` now covers unexpected Peer loss; native exit status belongs directly to the exec'd process. |
| `TestRunPiInteractiveReconnectsPublicIdentityWithoutRestartingNative` | Reconnect the same public identity without replacing the native process, then remove the private launch directory. | `wrappers/pi/peer.test.mjs`: `actual kit reconnect gates Caller work and never replays retained delivery`; `wrappers/pi/interactive_launch_test.go`: `TestPiInteractiveExecPlanIsExactAndLeavesNoLauncherArtifact`. The old parent/PID equality assertion is structurally retired because there is no launcher process after exec. |
| `TestRunPiInteractivePropagatesNativeExitAndContainsBridgeLoss` | Return the native child's exact nonzero exit, terminate it after owner-bridge loss, and remove the private directory. | Bridge loss: `wrappers/pi/extension.test.mjs`: `an idle owner connection ending retires the native session`; `wrappers/pi/peer.test.mjs`: `supersession and close join the direct owner lifetime`. Exact child-exit propagation and directory cleanup are retired because exec has no child or private directory; the OS reports native exit directly. |
| `TestRunPiInteractiveClosesAcceptedConnectionWhenChildAlreadyExited` | If the child exits during socket accept handoff, close the accepted bridge and remove the private directory. | Retired: the approved exec/direct-Peer design has no accept handoff, Go child, `owner.sock`, or launch directory. `TestPiInteractiveExecPlanIsExactAndLeavesNoLauncherArtifact` proves planning itself creates no replacement artifact. |

## New generation-boundary regressions

| New test | Boundary locked |
|---|---|
| `wrappers/pi/peer.test.mjs`: `direct owner uses the actual kit for hello, delivery, and Caller action` | A daemon delivery sent immediately after the first hello acknowledgement observes the installed native generation and is written rather than falsely rejected. |
| `wrappers/pi/peer.test.mjs`: `replacement rejects an in-flight busy result from the prior native generation` | A delayed busy response cannot enqueue an old delivery into a different replacement session. |
| `wrappers/pi/peer.test.mjs`: `same-ID native replacement rotates the Peer and rejects its old generation` | The same durable session ID does not collapse two native owner generations or admit stale work. |

## Weaker or structurally removed assertions

- Supersession response-write timing and the stale pre-admission superseded frame are
  no longer implemented or inspected by Pi. The bundled canonical kit owns that
  protocol state machine. Pi retains integration coverage for terminal
  supersession, joined owner shutdown, admitted Caller cancellation, and
  unadmitted hello cancellation.
- The old launch tests inspected a Go parent PID, direct child PID, forwarded
  signals, and private socket-directory cleanup. `syscall.Exec` removes those
  layers. The replacement tests assert the exact executable, argv, scrubbed
  bootstrap environment, direct Peer identity, and absence of launcher artifacts;
  native PID and exit status are now the process's ordinary OS behavior.
- Reconnect no longer proves process continuity by comparing child PIDs. It uses
  a real kit connection and proves the same in-process owner retains FIFO state,
  gates Caller work until re-admission, and performs no replay.
