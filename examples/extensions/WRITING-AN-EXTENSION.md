# Writing a Sail extension

A Sail extension is a separately built native package that Sail loads at session
start. It can add **one thing today: a Connect relation** — a verb your client
sends as a protobuf message, which the server turns into a table your client
reads back as an ordinary DataFrame.

This page is the minimum needed to write one. The
[design review](../../docs/development/extensions/design-review.md) explains why
the contract is shaped this way; you do not need it to get started.

> Experimental. The host accepts one API version and exact DataFusion and Arrow
> versions, so an extension is built against a known Sail build. There is no
> stable ABI promise yet.

## What exists, and what does not

| | State |
| --- | --- |
| Connect **relation** extension | Implemented. This page. |
| Connect **expression** extension | Not implemented. `Expression.extension` is rejected, and `input_expressions` in the envelope is rejected. |
| Connect **command** extension | Not implemented. Model a mutating verb as a relation that returns a receipt row. |
| Scalar functions (`ST_Point`, …) | Supported, but by a different door: the package registers them when Sail loads it, and SQL resolves them by name. They are not sent as protobuf. |

A relation handler runs on the driver when its package declares
`placement: driver`, and its inputs are gathered there. Worker-resident scalar
functions are the distributed path.

## The protocol

Your message travels in Spark Connect's own extension field,
`Relation.extension` (an `Any`). Two forms are accepted.

**With inputs**, wrap it in the Sail envelope
([`extension.proto`](../../crates/sail-spark-connect/proto/sail/extension/v1/extension.proto)):

```proto
message SailExtensionRequest {
  string payload_type_url = 1;          // your type URL
  bytes  payload          = 2;          // your bytes; Sail never parses them
  repeated spark.connect.Plan inputs = 3;  // ordinary client DataFrames
  repeated spark.connect.Expression input_expressions = 4;  // rejected today
  uint32 envelope_version = 5;          // must be 1
}
```

packed with type URL
`type.googleapis.com/sail.extension.v1.SailExtensionRequest`.

**Without inputs**, and only if your handler registers `accepts_bare`, put your
own type URL and payload directly in `Relation.extension`.

Limits, all rejected before anything is planned: 8 MiB envelope, 1 MiB payload,
16 inputs, 512-byte type URLs, and a nesting-depth bound. Planning must be pure —
`AnalyzePlan` can call your handler, so it must not consume inputs or mutate
state. Do the work when the stream is read.

## The client

No protobuf toolchain is required. Subclass `LogicalPlan`, set the two fields,
and hand the result to `DataFrame`. Field numbers are small, so a six-line varint
encoder is enough; generated stubs work equally well.

```python
import json
from pyspark.sql.connect.dataframe import DataFrame
from pyspark.sql.connect.plan import LogicalPlan

TYPE_URL = "type.googleapis.com/example.v1.MyVerb"
ENVELOPE = "type.googleapis.com/sail.extension.v1.SailExtensionRequest"


def _varint(n):
    out = bytearray()
    while n > 127:
        out.append((n & 127) | 128)
        n >>= 7
    out.append(n)
    return bytes(out)


def _field(number, value):          # length-delimited field
    return _varint((number << 3) | 2) + _varint(len(value)) + value


class MyRelation(LogicalPlan):
    def __init__(self, request, inputs=()):
        super().__init__(None)
        self.request, self.inputs = request, tuple(inputs)

    def plan(self, session):
        relation = self._create_proto_relation()
        payload = json.dumps(self.request).encode()
        if self.inputs:
            envelope = _field(1, TYPE_URL.encode()) + _field(2, payload)
            for frame in self.inputs:
                envelope += _field(3, frame._plan.to_proto(session).SerializeToString())
            envelope += _varint(5 << 3) + _varint(1)          # envelope_version = 1
            relation.extension.type_url = ENVELOPE
            relation.extension.value = envelope
        else:
            relation.extension.type_url = TYPE_URL
            relation.extension.value = payload
        return relation


def my_verb(spark, name, df=None):
    return DataFrame(MyRelation({"verb": "count", "name": name},
                                (df,) if df is not None else ()), spark)
```

A JSON payload keeps the example short; any encoding works, since Sail passes
the bytes through untouched.

## The server

One trait, one method. `inputs` arrive already planned, with the client's column
names restored; return a provider and Sail scans it.

```rust
use std::sync::Arc;
use datafusion::catalog::TableProvider;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::Result;
use sail_common_datafusion::connect_extension::ConnectRelationHandler;

struct MyVerb;

impl ConnectRelationHandler for MyVerb {
    fn plan(
        &self,
        payload: &[u8],
        inputs: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn TableProvider>> {
        let request: serde_json::Value = serde_json::from_slice(payload)?;
        // Describe execution only. Read `inputs` when the stream is polled.
        build_provider(request, inputs)
    }
}
```

The package exposes its handlers over the DataFusion FFI through named capsules,
and declares itself in a manifest:

```json
{
  "name": "example",
  "version": "0.1.0",
  "api_version": 1,
  "datafusion_version": "55.1.0",
  "arrow_version": "59.3.0",
  "placement": "driver",
  "memory_bytes": 268435456,
  "relation_types": [
    {"type_url": "type.googleapis.com/example.v1.MyVerb",
     "accepts_bare": false, "min_inputs": 1, "max_inputs": 1}
  ]
}
```

`memory_bytes` is a prepaid native quota drawn from Sail's own memory pool and is
allowed only for `placement: driver`. Duplicate names or type URLs fail the load.

Sail finds the package through a Python entry point:

```toml
[project.entry-points."pysail.extensions"]
example = "sail_example:extension"
```

## Build and run

```bash
git clone https://github.com/querygraph/sail.git && cd sail
bash examples/extensions/scripts/build.sh      # builds the wheels and the server
export SAIL_EXPERIMENTAL_EXTENSIONS=1          # without this, nothing loads
target/extensions-poc/host/debug/sail spark server --port 50051
```

Check what was discovered:

```bash
.venv/bin/python - <<'PY'
from importlib.metadata import entry_points
for e in entry_points(group="pysail.extensions"):
    print(e.name, (e.load()() if callable(e.load()) else e.load()).manifest())
PY
```

Then connect and call your verb:

```python
from pyspark.sql.connect.session import SparkSession
spark = SparkSession.builder.remote("sc://127.0.0.1:50051").create()
my_verb(spark, "hello").show()
```

## A worked example

[`nutmeg`](nutmeg) is a complete one: a Rust package exposing graph verbs, a
Python client of about a hundred lines
([`client.py`](nutmeg/python/sail_nutmeg/client.py)), and a manifest declaring
one relation type. [`sedona`](sedona) is the scalar-function shape instead.
[`TUTORIAL.md`](TUTORIAL.md) walks through running both, including cluster modes.
