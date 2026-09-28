"""Render large-graph time/memory tables and figures from an audited summary.

Usage: python render_large.py --summary summary.json --config configuration.json --host Morrobay --output figures
Requires matplotlib. This script does not run or discard benchmark trials.
"""
import argparse
import json
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.patches import Patch


PATHS = [('pecan', 'Pecan', '#2465a8'), ('nutmeg-native', 'Banda', '#bf5b17'),
         ('nutmeg-datafusion', 'Grenada', '#28834e')]
METHODS = {'pagerank': [('reference', 'Power'), ('optimized', 'Delta/frontier')],
           'wcc': [('reference', 'Reference'), ('optimized', 'Randomized'), ('fused', 'Fused randomized')]}


def formatted(metric, scale=1):
    if metric is None:
        return 'unavailable'
    return (f"{metric['median'] / scale:.3f} "
            f"[{metric['minimum'] / scale:.3f}, {metric['maximum'] / scale:.3f}] "
            f"(n={metric['samples']})")


def render(summary, config, host, output):
    if summary['integrity_errors']:
        raise ValueError('summary contains integrity errors')
    if any(config[key] != value for key, value in summary['sources'].items()):
        raise ValueError('summary and configuration source identities differ')
    output.mkdir(parents=True, exist_ok=True)
    groups = summary['groups']
    datasets = sorted({g['dataset'] for g in groups}, key=lambda name: (int(name.rsplit('-', 1)[1]), name))
    lookup = {(g['dataset'], g['algorithm'], g['variant'], g['engine']): g for g in groups}
    if len(lookup) != len(groups):
        raise ValueError('expected one execution mode/suite per dataset and method')
    lines = ['# Large graph results', '',
             'Median [minimum, maximum]; n is the number of successful samples. '
             'Missing measurements are unavailable. All outcomes remain in the CSV.', '']
    for dataset in datasets:
        lines += [f'## {dataset}', '', '| Algorithm | Method | Path | Outcomes | Seconds | Sampled peak PSS GiB |',
                  '| --- | --- | --- | --- | --- | --- |']
        fig, axes = plt.subplots(2, 2, figsize=(12, 8), constrained_layout=True)
        for row, (algorithm, methods) in enumerate(METHODS.items()):
            for index, (variant, method) in enumerate(methods):
                for offset, (engine, label, color) in enumerate(PATHS):
                    group = lookup[(dataset, algorithm, variant, engine)]
                    metrics = group['metrics']
                    outcomes = ', '.join(f'{count} {outcome}' for outcome, count in sorted(group['outcomes'].items()))
                    lines.append(f'| {algorithm} | {method} | {label} | {outcomes} | '
                                 f"{formatted(metrics['seconds'])} | {formatted(metrics['execution_pss_bytes'], 2**30)} |")
                    for col, (key, scale, ylabel) in enumerate([
                            ('seconds', 1, 'Complete-call seconds'),
                            ('execution_pss_bytes', 2**30, 'Sampled peak PSS (GiB)')]):
                        ax = axes[row, col]
                        metric = metrics[key]
                        x = index + (offset - 1) * .25
                        if metric is not None:
                            value = metric['median'] / scale
                            ax.bar(x, value, width=.23, color=color,
                                   label=label if index == 0 else None)
                            ax.errorbar(x, value, yerr=[[value - metric['minimum'] / scale],
                                                      [metric['maximum'] / scale - value]],
                                        fmt='none', ecolor='#333333', capsize=3)
                            planned = sum(group['outcomes'].values())
                            if metric['samples'] < planned:
                                missing = ', '.join(f'{n} {outcome}' for outcome, n in
                                                    sorted(group['outcomes'].items()) if outcome != 'passed')
                                label = f"n={metric['samples']}/{planned}"
                                if missing:
                                    label += '\n' + missing
                                ax.annotate(label, (x, metric['maximum'] / scale),
                                            xytext=(0, 5), textcoords='offset points',
                                            ha='center', va='bottom', fontsize=7)
                        else:
                            ax.text(x, .02, 'unavailable', rotation=90, ha='center', va='bottom',
                                    transform=ax.get_xaxis_transform(), fontsize=7)
                        ax.set_ylabel(ylabel)
            for ax in axes[row]:
                ax.set_xticks(range(len(methods)), [label for _, label in methods])
                ax.set_title('PageRank' if algorithm == 'pagerank' else 'WCC')
                ax.margins(y=.16)
                ax.set_ylim(bottom=0)
                ax.grid(axis='y', alpha=.2)
                ax.set_axisbelow(True)
        fig.legend(handles=[Patch(color=color, label=label) for _, label, color in PATHS],
                   loc='outside lower center', ncol=3, frameon=False)
        modes = {g['mode'] for g in groups if g['dataset'] == dataset}
        if len(modes) != 1:
            raise ValueError('expected one execution mode per figure')
        mode = 'Driver + two workers' if modes == {'process-cluster'} else 'Local Sail'
        limits = config['limits']
        fig.suptitle(f"{dataset} · {host} · {limits['cpus']} CPU quota / {limits['memory_gib']} GiB\n"
                     f'{mode}; Banda kernels stay driver-local\n'
                     'Reference WCC: Banda union-find; Pecan/Grenada min-label\n'
                     'Successful trials: median/min/max; incomplete groups labeled; all outcomes in tables', fontsize=10)
        for extension in ('png', 'svg'):
            fig.savefig(output / f'{dataset}.{extension}', dpi=160)
        plt.close(fig)
        lines += ['', f'![Time and memory for {dataset}]({dataset}.png)', '']
    (output / 'tables.md').write_text('\n'.join(lines) + '\n')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--summary', type=Path, required=True)
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--host', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    render(json.loads(args.summary.read_text()), json.loads(args.config.read_text()), args.host, args.output)


if __name__ == '__main__':
    main()
