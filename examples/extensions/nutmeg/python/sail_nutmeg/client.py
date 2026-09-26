"""Version 1 JSON verbs carried in the proposal's Spark Connect Any envelope."""
import json
from pyspark.sql.connect.dataframe import DataFrame
from pyspark.sql.connect.plan import LogicalPlan
from . import TYPE_URL

ENVELOPE_TYPE_URL = "type.googleapis.com/sail.extension.v1.SailExtensionRequest"


def _varint(number):
    result = bytearray()
    while number > 127:
        result.append((number & 127) | 128)
        number >>= 7
    result.append(number)
    return bytes(result)


def _bytes_field(number, value):
    return _varint((number << 3) | 2) + _varint(len(value)) + value


class ExtensionRelation(LogicalPlan):
    def __init__(self, request, inputs=()):
        super().__init__(None)
        self.request = request
        self.inputs = tuple(inputs)

    def plan(self, session):
        relation = self._create_proto_relation()
        payload = json.dumps(self.request, separators=(",", ":"), allow_nan=False).encode()
        if self.inputs:
            envelope = _bytes_field(1, TYPE_URL.encode()) + _bytes_field(2, payload)
            for frame in self.inputs:
                envelope += _bytes_field(3, frame._plan.to_proto(session).SerializeToString())
            envelope += _varint(5 << 3) + _varint(1)
            relation.extension.type_url = ENVELOPE_TYPE_URL
            relation.extension.value = envelope
        else:
            relation.extension.type_url = TYPE_URL
            relation.extension.value = payload
        return relation


class Nutmeg:
    """Use with a Spark Connect session connected to the extension-enabled Sail."""
    def __init__(self, spark):
        self.spark = spark

    def _relation(self, verb, graph, *, inputs=(), **kwargs):
        return DataFrame(ExtensionRelation({"version": 1, "verb": verb, "graph": graph, **kwargs}, inputs), self.spark)

    def stage(self, graph, nodes, edges, *, node_mapping=None, edge_mapping=None):
        """Atomically overwrite one graph; eagerly collect its one-row receipt.

        Both DataFrames must belong to this session. Each receipt reports graph,
        nodeCount, edgeCount and revision. All input partitions are consumed.
        """
        if nodes.sparkSession is not self.spark or edges.sparkSession is not self.spark:
            raise ValueError("stage inputs must belong to this Nutmeg Spark session")
        return self._relation("stage", graph, inputs=(nodes, edges), nodeMapping=node_mapping or {}, edgeMapping=edge_mapping or {}).collect()[0]

    def run(self, graph, algorithm, *, column_names="grust", **options):
        """Return a lazy DataFrame, with a graph snapshot pinned during planning."""
        return self._relation("run", graph, algorithm=algorithm, options=options, columnNames=column_names)

    def drop(self, graph):
        """Drop this session's graph and return its removal receipt."""
        return self._relation("drop", graph).collect()[0]
