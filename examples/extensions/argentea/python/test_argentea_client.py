import json
from pathlib import Path
import sys

import pytest
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
from pyspark.sql.connect import proto
from sail_nutmeg.client import ExtensionRelation, ENVELOPE_TYPE_URL

sys.path.insert(0, str(Path(__file__).parent))
from argentea_client import Argentea, TYPE_URL, build_plan, options


def envelope_type():
    file = descriptor_pb2.FileDescriptorProto(name='argentea_test_envelope.proto', package='argentea.test', syntax='proto3')
    file.dependency.append(proto.Plan.DESCRIPTOR.file.name)
    message = file.message_type.add(name='Envelope')
    for name, number, kind in [('payload_type_url',1,9),('payload',2,12),('inputs',3,11),('envelope_version',5,13)]:
        field = message.field.add(name=name, number=number, type=kind, label=3 if name == 'inputs' else 1)
        if name == 'inputs':
            field.type_name = '.spark.connect.Plan'
    pool = descriptor_pool.Default()
    pool.Add(file)
    return message_factory.GetMessageClass(pool.FindMessageTypeByName('argentea.test.Envelope'))


class Input:
    def __init__(self, label):
        self._plan = ExtensionRelation(dict(version=1, verb='input-fixture', label=label))


@pytest.mark.parametrize('iterations', [1, 2, 8, 32])
def test_all_rounds_share_one_nested_connect_plan_and_operation(iterations):
    # A plain object cannot make Spark RPCs. Successful serialization therefore
    # proves the builder itself does no collect/write/iteration action.
    frame, request = build_plan(object(), Input('vertices'), Input('edges'), vertices_count=7,
                                iterations=iterations, partitions=3)
    envelope = envelope_type()
    node = frame._plan.plan(None)
    observed = []
    while node.extension.type_url == ENVELOPE_TYPE_URL:
        decoded = envelope.FromString(node.extension.value)
        assert decoded.envelope_version == 1
        assert decoded.payload_type_url == TYPE_URL
        payload = json.loads(decoded.payload)
        assert all(payload[key] == value for key, value in request.items())
        observed.append((payload['verb'], payload['round']))
        if payload['verb'] == 'init':
            assert len(decoded.inputs) == 2
            assert [json.loads(plan.root.extension.value)['label'] for plan in decoded.inputs] == ['vertices', 'edges']
            break
        assert len(decoded.inputs) == 1
        node = decoded.inputs[0].root
    assert observed == [('result', iterations-1)] + [('round', r) for r in range(iterations-1, 0, -1)] + [('init', 0)]
    assert request['damping'] == 0.85 and request['vertices'] == 7


@pytest.mark.parametrize('changed', [dict(iterations=0), dict(iterations=True), dict(iterations=33),
                                    dict(partitions=0), dict(partitions=65), dict(batch_rows=0),
                                    dict(reset_probability=float('nan')), dict(reset_probability=0),
                                    dict(reset_probability=True), dict(reset_probability=1e-100)])
def test_invalid_options_fail_before_spark_or_staging(changed):
    arguments = dict(iterations=2, partitions=2, reset_probability=.15, batch_rows=4096)
    arguments.update(changed)
    with pytest.raises(ValueError):
        Argentea(object()).pagerank(object(), object(), **arguments)


def test_empty_graph_and_noncanonical_operation_are_explicit():
    with pytest.raises(ValueError, match='at least one vertex'):
        build_plan(object(), Input('vertices'), Input('edges'), vertices_count=0)
    with pytest.raises(ValueError):
        build_plan(object(), Input('vertices'), Input('edges'), vertices_count=1, operation_id='shared-state')


def test_full_reset_is_supported_without_a_convergence_claim():
    _, request = build_plan(object(), Input('vertices'), Input('edges'), vertices_count=3, reset_probability=1)
    assert request['damping'] == 0
