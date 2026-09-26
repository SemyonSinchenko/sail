# Review of the extension design review, and of the parallel Grust proposal

A second-opinion review, written 2026-09-26 by a Claude Code session that had
been developing a competing design document in another repository and did not
know this proof of concept existed. That is the first finding: two overlapping
asks for the same maintainer discussion were being prepared independently, and
only one should be sent.

## What was reviewed, and what was not

Read in full: [`design-review.md`](design-review.md), which describes
implementation commit `de8e670989edb8ed5343764c52d0d002b6b6cd63` on
`querygraph/sail` branch `work/extensions-datafusion-graphs`, against upstream
baseline `a85d912d72ae03a6d97b6a3fd151f5752da636c6`. Read for orientation:
[`implementation-plan.md`](implementation-plan.md),
[`implementation-review.md`](implementation-review.md),
[`implementation-review-resolution.md`](implementation-review-resolution.md),
[`datafusion-graph-plan.md`](datafusion-graph-plan.md),
[`linux-environment.md`](linux-environment.md), and the evidence index at
`target/extensions-datafusion-final/evidence-index.json`, whose recorded status
is `all_gates_passed_and_published` at that commit.

Not done here: the gates were not re-run, the 30,210-line diff was not read, the
wheels were not audited, and the review's central architectural claim — that no
Nutmeg payload type and no Sedona dependency enters a Sail engine crate — was
taken as stated rather than verified against the source. That claim is the one a
maintainer will check first, so it is worth verifying before submission rather
than after.

The other document under discussion is `docs/proposals/sail-extension-api.md`
in `querygraph/grust`, branch `work/proposal-v5`, commit `7fc0514` — a fifth
revision written on 2026-09-25, restructured around the Spark Connect protocol
after two research notes established how the JVM mechanism works and where Sail
rejects extension messages. Its fourth revision carried an external architecture
review (`sail-extension-api-astra-review.md`, committed at `aa3903e`) whose
eleven findings shaped its execution-contract sections.

## The main finding: the proposal is behind the implementation

The Grust-side fifth revision presents as design decisions, and in two cases as
open problems, things this proof of concept has already built and gated.

| Fifth revision | State in the proof of concept |
| --- | --- |
| "Route on the type URL" | Built, and bounded: 8 MiB envelope, 1 MiB payload, 16 inputs, 512-byte type URLs, nesting depth 64, validated before children are planned |
| "A Sail envelope that makes nested plans first-class" | Built as ordinary Connect input plans; registered input-free handlers may opt into a bare `Any` |
| "A handler returns an `FFI_TableProvider`" | Built: a provider produced from resolved physical inputs |
| "The output-field-names contract" | Built: input field names and result naming follow Sail resolution |
| "Commands versus relations" | Decided: stage, algorithm, scan, status and drop are all relation extensions |
| **"Runtime fidelity — the sharpest open issue"** | **Solved**: the host-owned input adapter retains the real Sail task context and Tokio runtime instead of a reconstructed default `RuntimeEnv` |
| "Placement covering functions as well as nodes" | Built: `DriverExtensionExec`, owner/plan handles a worker cannot decode, and a native expression codec carrying ownership identity and metadata-bearing return fields |
| "Mutation under retry: at-most-once in v1" | Built: one attempt for a region containing driver-native execution, with unacknowledged mutation reported as indeterminate |
| "Cluster mode is where a working plugin breaks" | Superseded for scalars: worker loading, installed-package identity and expression serialization, exercised on separate-process workers |
| Native memory accounting | Absent from the proposal entirely; the proof of concept has a dependency-free lease ABI, a host-funded quota drawn from the actual DataFusion pool, and admission before expansion |

Two consequences. First, the execution contracts that the external review
demanded — physical input requirements, runtime fidelity, ownership identity,
placement covering functions, retry semantics, teardown — are not open questions
here; most are implemented, and the remainder are stated as bounded policies.
Second, sending both documents to the discussion would present the maintainers
with two framings of one request, which is the failure already flagged about an
earlier over-long description.

**Recommendation: one ask, built on this review's spine** — its six decisions to
request and its eight-slice upstream sequence. What is worth carrying over from
the proposal is the constraint analysis against the maintainers' own quoted
statements, and the citation-level map of where Sail rejects and resolves
extension messages.

## What this review does well, and should not be edited away

- **It refuses the whole branch as one pull request**, and says so in bold. The
  eight-slice sequence, each slice naming one invariant, its smallest meaningful
  regression and what depends on it, is the correct shape for this maintainer.
- **Its evidence section states what the evidence does not establish** — network
  acknowledgement-loss tolerance, abrupt process recovery, Kubernetes operation,
  optimized spatial joins, full RSS accounting, portable ABI across Sail
  releases, distributed native graph iteration, performance. A claims section is
  only as credible as its exclusions.
- **Failed candidates are retained as evidence.** `d7143e099` failed scalar-list
  geometry metadata and geometry/NULL coercion; the fix preserved the assertions
  and corrected the subject rather than the test.
- **Boundaries travel with every claim**: 128 registered Sedona functions is
  explicitly not semantic qualification of each; the two-host run qualifies
  cross-host execution and explicitly not mixed-architecture compatibility or
  performance; native results feeding distributed SQL is explicitly not a
  distributed CSR kernel.
- **The pool-sharing policy is flagged for maintainer agreement** rather than
  slipped in, and the relational graph path works with extensions disabled,
  which is a strong independent argument.

## Six concerns, in the order I would address them

1. **Exact wheel rebuilds collide head-on with the maintainer's stated reason
   for wanting an FFI.** From discussion #2001: "we won't need to recompile the
   extension on every Sail version or Rust version change. This allows Sail and
   the extension to release under different schedule." The initial policy here
   is the opposite. Calling it "initial policy, not a permanent promise" is
   honest but not enough; the ask should confront this in its opening, with
   either a supported-build matrix or an explicit experimental status and a
   named path to stability.
2. **Change the process-wide pool registry before upstreaming slice 6, rather
   than shipping it and discussing it.** Sharing pools by matching kind and
   configured limit treats equal configuration as equal tenancy. The review
   already says this needs agreement; a multi-tenant accounting defect would
   discredit the whole resource contract, so prefer an explicitly owned resource
   domain with pool injection.
3. **Lead with slices 1 and 2.** Lifecycle correctness and geometry field
   preservation are useful to Sail with no extension API at all, are not
   conditional on the flag, and landing them first earns standing while reducing
   what the contentious slices must carry.
4. **Package the evidence before review.** The receipts, logs and artifact
   hashes exist under `target/extensions-datafusion-final/` with an index, but
   they are gitignored and absent from a clone. A redacted, checksummed bundle
   attached to the review is the difference between a narrative and something a
   maintainer can audit.
5. **Isolate the conversion-depth guard and its `stacker` dependency.** The
   review already recommends treating it as its own design decision; it touches
   ordinary conversion and will draw review attention disproportionate to its
   size.
6. **Note the emulation in the two-host run.** Capitola ran x86-64 under
   Rosetta; Morrobay is native Intel. The review discloses this correctly, but a
   maintainer may discount it, and no second native x86-64 host is currently
   available.

## What remains genuinely open after this work

The review's own six questions stand, and are the right ones to ask. To them I
would add only that the *first* question — whether an opt-in, exact-build Python
bootstrap is acceptable for an initial experiment — is really the wheel-rebuild
question above, and deserves the strongest answer the evidence can support,
because a maintainer who declines it declines the packaging model that everything
else assumes.
