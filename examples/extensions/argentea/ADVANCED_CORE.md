# Argentea residual PageRank core

This crate now includes extension-owned residual PageRank primitives alongside
reference power PageRank. The separate version-2 Nutmeg adapter and Python
client are described in [DELTA.md](DELTA.md). Core and in-process DataFusion
tests are not evidence of Sail scheduling, network execution or Linux
compatibility. Its traversal is sequential; parallel source-block combining is
separate work.

`Adjacency` is the existing immutable admitted CSR extracted from reference
PageRank. Reference and residual partitions use the same construction. Residual
state adds scores, signed residuals, activity flags, admitted candidate snapshots
and separate producer-indexed statistics and contribution inboxes. All partitions
in one worker operation must receive the same `Resources` domain and lease.

## Protocol

1. Build one `DeltaPartition` per owner. Each graph vertex occurs once globally;
   all edge sources belong to the local owner. Validate global endpoints before
   building; contributions also reject an absent destination.
2. Call `statistics()` once, broadcast the report, and call `receive_statistics()`
   once per producer. Empty owners report count/mass/residual zero and minimum
   score positive infinity. The typed wire must preserve that empty-owner
   exception without allowing nonfinite scores or contributions.
3. After every complete statistics stream reaches EOF, call `seal()`. `Some`
   means a fresh global certificate passed and results are available through
   `rank_cursor()`. `None` means more work is required. A failed certificate at
   the push cap is an error, never an uncertified public result.
4. If continuing, call `start_emission()` under the owner lock, release the lock,
   and drain its owned cursor. Every owner deterministically selects the same
   mode from producer-ordered statistics. The initial mode performs a full
   normalized certificate; later modes push signed residuals or certify again.
5. Route `DeltaContribution` by target owner. Deliver every producer's completion
   to every owner, including empty streams. `finish_producer()` checks mode,
   sequence, dangling contribution and cardinality. Call `finish()` only after
   input EOF. The adapter must not publish at the first P completion markers:
   late or extra input rows still invalidate that stream.
6. Repeat with the next `Round.number`. A static plan can relay `Done` phases
   instead of sealing immediately. Those phases send only completion markers,
   preserve the certified scores and counters, and do not cancel the operation.

A push uses `min(R/(2N), tolerance*mass/(4N))`, retains inactive residual and
allows reactivation. It applies signed dangling redistribution. Only a fresh
normalized `||T(x)-x||1` certificate establishes convergence. A failed cheap-bound
certificate rebases both scores and maintained residual. `max_pushes` excludes
certificate passes. `native_phase_bound()` is the conservative `4K+4` layout;
future clients must additionally enforce the smaller qualified deployment limit.

Statistics collection, receiving and sealed states are explicit. Malformed,
replayed or incomplete barriers poison the partition. Candidate scores publish
only after full input, completed local emission, finite-state checks and admission
of the next barrier. Dropping unfinished emission cancels the shared execution
context. Retained statistics, contributions, completions and result cursors keep
their admission and host lease. Arrow adapters attach ownership to their own allocated buffers; the residual
adapter preserves the reference adapter's lease attachment.

## Validation

Run from the repository root, using an isolated target directory:

```sh
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/argentea-core-target \
  cargo test --manifest-path examples/extensions/argentea/Cargo.toml --locked --release
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/tmp/argentea-core-target \
  cargo clippy --manifest-path examples/extensions/argentea/Cargo.toml \
  --locked --all-targets -- -D warnings
```

Tests compare every signed transition with an independent dense matrix,
recompute final residuals, use Banda's reactivation and settling fixtures, and
cover P=1,2,3,11, empty owners, negative dangling updates, cap failure, fake
maintained-residual injection, certificate rebase, DONE relays, malformed
barriers, shared quota/work refusal, retained leases and bounded-channel
backpressure. The existing reference tests still run. Source-included scheduler
probes describe the host source in the checkout being tested; they are not a
qualification of the residual adapter inside a Sail worker process.
