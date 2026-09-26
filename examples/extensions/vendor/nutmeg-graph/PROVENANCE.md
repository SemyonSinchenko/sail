# Vendored Nutmeg graph integration

Source: https://github.com/querygraph/nutmeg

Revision: `f267b03659dd536981f98420944f911b667632b7`.

Only `crates/nutmeg-graph/src` is included as implementation, with the upstream
MIT and Apache licenses and the upstream README as a catalog-test fixture. The standalone manifest reproduces its dependencies with the Sail
PoC's exact DataFusion 55.1.0 and Arrow 59.3.0 pins. This is a source dependency,
not a second implementation of Grust's algorithms.

Local changes add `SessionRegistry` with a separate graph store and memory pool,
atomic two-part replacement, stable graph snapshots for read providers, and
session-specific read accounting. Existing process-global APIs remain available
for the upstream test suite. The extension exclusively uses session APIs.

Schema probes now use a separate fixed 16 MiB setup store. `prepare_output_schemas`
warms the finite catalog before user planning; session read constructors use
cache-only schema lookup. The one upstream test that constructs `AlgorithmTable`
directly is bound to the setup store explicitly.
