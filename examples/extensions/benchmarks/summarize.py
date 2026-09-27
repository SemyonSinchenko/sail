#!/usr/bin/env python3
"""Export every matrix outcome and summarize only correctly completed trials."""
import argparse
from collections import Counter, defaultdict
import csv
from datetime import datetime, timezone
import json
from pathlib import Path
from statistics import median

from run_matrix import plan_cells


GROUP = ('suite', 'dataset', 'mode', 'algorithm', 'engine', 'variant')
METRICS = ('seconds', 'execution_rss_bytes', 'execution_pss_bytes',
           'cgroup_peak_through_result_bytes', 'cgroup_lifetime_peak_bytes',
           'native_pool_peak_bytes', 'native_read_peak_bytes', 'iterations',
           'true_residual', 'whole_vm_steal_fraction')


def number(value):
    return None if value is None else float(value)


def cell_row(cell, summary, receipt):
    row = {k: cell[k] for k in ('cell_id', 'sequence', 'repeat', *GROUP, 'expected_outcome')}
    row.update(outcome=summary['outcome'] if summary else 'not_run',
               expected_outcome_observed=bool(summary and summary['expected_outcome_observed']))
    row.update({key: None for key in METRICS})
    if not receipt:
        if row['outcome'] == 'passed':
            raise ValueError(f"successful cell has no receipt: {cell['cell_id']}")
        return row
    peaks = receipt.get('memory', {}).get('phase_peaks', {}).get('execute', {})
    correctness = receipt.get('correctness', {})
    status = receipt.get('native_status_after', receipt.get('native_status_on_error', {})) or {}
    reads = [r for r in status.get('reads', []) if r['algorithm'] == receipt.get('kernel')]
    native = reads[0] if len(reads) == 1 else {}
    diagnostics = native.get('diagnostics') or {}
    events = receipt.get('iteration_events', [])
    row.update(
        kernel=receipt.get('kernel'),
        seconds=receipt.get('end_to_end_seconds'),
        elapsed_until_error_seconds=receipt.get('elapsed_until_error_seconds'),
        execution_rss_bytes=peaks.get('rss_bytes'), execution_pss_bytes=peaks.get('pss_bytes'),
        execution_sampled=receipt.get('memory', {}).get('execution_sampled'),
        cgroup_peak_through_result_bytes=number(receipt.get('cgroup_execution_after', {}).get('memory.peak')),
        cgroup_lifetime_peak_bytes=number(receipt.get('cgroup_after', {}).get('memory.peak')),
        native_pool_peak_bytes=status.get('memory', {}).get('peak_bytes'),
        native_read_peak_bytes=native.get('peak_bytes'),
        iterations=receipt.get('algorithm_iterations', diagnostics.get('iterations', correctness.get('max_iterations'))),
        true_residual=correctness.get('true_fixed_point_residual'),
        whole_vm_steal_fraction=receipt.get('guest_steal_fraction'),
        frontier_messages=diagnostics.get('frontier_edges'),
        contraction_rounds=len(diagnostics.get('rounds', [])) if receipt.get('kernel') == 'wccRandomized' else None,
        native_source_sha=receipt.get('native_source_sha'),
        runtime_source_sha=receipt.get('runtime_source_sha'),
        harness_source_sha=receipt.get('harness_source_sha'),
        binary_sha256=receipt.get('binary_sha256'),
    )
    frontier = [e['active_edges'] for e in events if e['kind'] == 'iteration_end' and 'active_edges' in e]
    contraction = [e for e in events if e['kind'] == 'iteration_end' and 'edges_after' in e]
    if frontier:
        row['frontier_messages'] = sum(frontier)
    if contraction:
        row['contraction_rounds'] = len(contraction)
    return row


def aggregate(rows):
    groups = defaultdict(list)
    for row in rows:
        groups[tuple(row[k] for k in GROUP)].append(row)
    result = []
    for key, trials in sorted(groups.items()):
        passed = [r for r in trials if r['outcome'] == 'passed']
        metrics = {}
        for name in METRICS:
            values = [r[name] for r in passed if r[name] is not None]
            metrics[name] = (dict(samples=len(values), median=median(values), minimum=min(values), maximum=max(values))
                             if values else None)
        result.append(dict(zip(GROUP, key), planned=len(trials), passed=len(passed),
                           outcomes=dict(Counter(r['outcome'] for r in trials)), metrics=metrics))
    return result


def display(metric, divisor=1):
    if metric is None:
        return 'unavailable'
    middle, low, high = (metric[k] / divisor for k in ('median', 'minimum', 'maximum'))
    return f'{middle:.3f} [{low:.3f}, {high:.3f}]'


def tables(groups):
    lines = ['# All PageRank and WCC methods', '',
             'Cells show median [minimum, maximum] for passed trials only. Counts retain every outcome.', '',
             'RSS/PSS are sampled execution-phase process totals. Cgroup peak is the lifetime peak through result delivery, before verification. Memory is MiB.', '']
    previous = None
    names = {'pecan': 'Pecan', 'nutmeg-native': 'Banda', 'nutmeg-datafusion': 'Grenada'}
    for group in groups:
        section = tuple(group[k] for k in ('suite', 'dataset', 'mode', 'algorithm'))
        if section != previous:
            lines.extend(['', '## ' + ' / '.join(section), '',
                          '| Path | Variant | Outcomes | Seconds | PSS MiB | RSS MiB | Cgroup MiB |',
                          '| --- | --- | --- | ---: | ---: | ---: | ---: |'])
            previous = section
        metrics = group['metrics']
        outcomes = ', '.join(f'{name}: {count}' for name, count in sorted(group['outcomes'].items()))
        values = [names[group['engine']], group['variant'], outcomes, display(metrics['seconds']),
                  display(metrics['execution_pss_bytes'], 2**20), display(metrics['execution_rss_bytes'], 2**20),
                  display(metrics['cgroup_peak_through_result_bytes'], 2**20)]
        lines.append('| ' + ' | '.join(values) + ' |')
    return '\n'.join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--evidence', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    config = json.loads((args.evidence / 'configuration.json').read_text())
    rows = []
    for cell in plan_cells(config):
        directory = args.evidence / 'cells' / cell['cell_id']
        path = directory / 'summary.json'
        summary = json.loads(path.read_text()) if path.exists() else None
        receipt_path = directory / 'artifacts/receipt.json'
        try:
            receipt = json.loads(receipt_path.read_text()) if receipt_path.exists() else None
        except json.JSONDecodeError:
            receipt = None
        row = cell_row(cell, summary, receipt)
        if row['outcome'] == 'passed':
            for key in ('harness_source_sha', 'runtime_source_sha', 'native_source_sha'):
                assert row[key] == config[key], (cell['cell_id'], key)
            assert not receipt['source_dirty'], cell['cell_id']
            assert not receipt['arguments']['allow_dirty'] and not receipt['arguments']['allow_unisolated']
            assert receipt['memory']['error'] is None
            assert row['seconds'] is not None and row['seconds'] >= 0
        rows.append(row)
    groups = aggregate(rows)
    result = dict(generated_utc=datetime.now(timezone.utc).isoformat(),
                  planned_cells=len(rows), outcomes=dict(Counter(r['outcome'] for r in rows)),
                  expected_outcomes_observed=sum(r['expected_outcome_observed'] for r in rows),
                  sources={k: config[k] for k in ('harness_source_sha', 'runtime_source_sha', 'native_source_sha')},
                  groups=groups)
    args.output.mkdir(parents=True, exist_ok=False)
    (args.output / 'summary.json').write_text(json.dumps(result, indent=2) + '\n')
    columns = list(dict.fromkeys(key for row in rows for key in row))
    with (args.output / 'cells.csv').open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)
    (args.output / 'tables.md').write_text(tables(groups) + '\n')
    print(json.dumps({k: result[k] for k in ('planned_cells', 'outcomes', 'expected_outcomes_observed')}))


if __name__ == '__main__':
    main()
