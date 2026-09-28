# Argentea fault qualification across two hosts

This harness tests cancellation and worker loss after both native graph owners
have initialized. It uses the existing two-host launcher and Sail worker pool.
It does not add a graph scheduler or a production fault-control API.

## Prerequisites

Use a clean, identical source revision on the controller, driver, and workers.
The existing two-host configuration supplies `driver` and exactly two `workers`,
with distinct physical hostnames. Each target supplies `repo`, `python`, `sail`,
and `advertise`; remote targets also supply `ssh`. The driver supplies
`connect_port`, `gateway_port`, and a host-local `environment_file` containing the
shared S3 configuration. Keep credentials in that host-local file.

The launcher checks binary and extension package identities before starting.
Use the matching native artifacts on both hosts. The controller Python
environment needs the extension client and Spark Connect dependencies.

## Run

From the Sail checkout, substitute the exact source commits used to build the
Sail executable and native extension:

```sh
python examples/extensions/argentea/python/qualify_faults.py \
  --two-host-config /absolute/path/two-host.json \
  --runtime-source-sha "$SAIL_RUNTIME_COMMIT" \
  --native-source-sha "$ARGENTEA_NATIVE_COMMIT" \
  --algorithm wcc_star --case worker-loss --victim-owner 0 \
  --output /absolute/path/evidence/wcc-star-owner-0
```

Use a new output directory for every attempt. Repeat worker loss for owner 1.
For cancellation, use `--case cancel` and omit `--victim-owner`. The supported
algorithms are `pagerank_delta`, `wcc_reference`, `wcc_star`, `sssp_reference`,
and `sssp_delta_star`. Quota tests use the separate resource harness; this
command rejects two-host quota mode.

## What the evidence establishes

An opt-in private Unix socket lets each launcher supervisor signal only its own
child. Each control request has a five-second transport bound; query completion
prevents subsequent fault injection, and cleanup resumes stopped survivors.
Native receipts do not authorize arbitrary PID signaling. Host inventory,
launcher records, worker IDs, and PIDs bind the native owners to the two hosts;
identical PIDs on different hosts remain distinct workers.

The controller waits for both initialization receipts, stops both workers, and
checks that no result or terminal native event preceded injection. It then
cancels the query or kills the selected native owner, and resumes survivors.
The audit checks terminal tasks, surviving owner closure, view cleanup, process
cleanup, and absence of owned run objects in shared S3 storage after session
shutdown. The storage audit is read-only and refuses paths outside the configured
root. It does not delete leftover objects to manufacture a passing result.

Receipts retain source hashes, host inventories, signal records, logs, errors,
and cleanup results. An unsuccessful attempt remains a failure. These tests do
not establish performance, zero RSS after cleanup, or release of every retained
Arrow buffer.

## Qualification status

The remote transport and evidence checks have local automated coverage,
including real supervised processes. Physical two-host execution of this fault
matrix remains pending. Do not treat the harness implementation or its unit
tests as evidence that the physical-host matrix has passed.
