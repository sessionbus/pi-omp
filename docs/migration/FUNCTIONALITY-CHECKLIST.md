# Pi/OMP functionality preservation

Source baseline: combined peers `710e5d33369cba4fb9468cd24fea0fe844a0219d`. Frozen mapping: `PRESERVED-FILES.json`. Source preservation and fresh installed acceptance are separate.

| Contract | Source and test preservation | Installed evidence / remaining limit |
|---|---|---|
| Pi and OMP command dispatch, native argv, permissions and normal process exit | Both `cmd/*-peer` and `wrappers/{pi,omp}` with original Go tests | Existing product-specific acceptance remains historical until this archive is installed |
| Managed Run/Worker lifecycle, readiness, result, cancellation and cleanup | Product tests plus pinned `peer-common` host/socket/version tests | Pi normal Run and OMP core Worker were observed on prior builds; preserve actual native results and ownership on fresh tests |
| Interactive launch, reconnect, delivery and Forget | Product and `pifamily` tests | Pi interactive replacement/startup/resume and OMP zero-input interactive observed historically; staged OMP context and Pi interrupt recovery remain preview work |
| Native extension bridge and exact closed MCP declaration | Four original `.test.mjs` files, 48 static registrations and 50 passing runtime tests; byte-exact shared declaration test fixture copied locally | Pi uses its existing Node host and OMP its Bun host; no installed-behavior claim from source tests |
| Archive build, plugin members, role and checksum-safe bootstrap | `scripts/package-product`, `scripts/release/install-product`, two bootstrap scripts and boundary/download tests | Permanent real-home install and idempotent reinstall remain future acceptance |
| Module and source boundary | 105 frozen files, 346 Go tests; 32 common files at pinned `eb655f6`; root architecture/version tests | No product-path rewrite or alternate install prefix |
| Preview and release status | Local Pi/OMP design/acceptance/release-note links; held binary-release workflow | Both products stay preview. No publication before release gate review. |

The frozen inventory contains 71 product files, two shared packaging surfaces and 32 pinned common files. Product Go changes are import relocation only. The only exception in test assets is the local, byte-identical schema fixture and two path references described in `EXTRACTION-NOTES.md`. Original source tests and preserved installed evidence are not a substitute for fresh acceptance of this extraction.
