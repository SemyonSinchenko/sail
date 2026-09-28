"""Linux measurements for one trial in a fresh, private PID/cgroup container.

RSS sums count shared mappings more than once; PSS apportions them. The kernel
cgroup peak includes page cache and the entire container lifetime. Neither is
interchangeable with the graph extension's allocator accounting.
"""
import json
from pathlib import Path
import threading
import time


CGROUP = Path('/sys/fs/cgroup')


def read_text(path):
    try:
        return Path(path).read_text().strip()
    except (OSError, UnicodeError):
        return None


def counters(path):
    text = read_text(path)
    if text is None:
        return {}
    result = {}
    for line in text.splitlines():
        fields = line.split()
        if len(fields) >= 2 and fields[1].isdigit():
            result[fields[0].rstrip(':')] = int(fields[1])
    return result


def cgroup_snapshot():
    return {name: read_text(CGROUP / name) for name in (
        'memory.current', 'memory.peak', 'memory.max', 'memory.swap.max',
        'memory.events', 'memory.stat', 'cpu.max', 'cpu.stat', 'cpuset.cpus.effective')}


def cpu_ticks():
    text = read_text('/proc/stat')
    return list(map(int, text.splitlines()[0].split()[1:])) if text else None


def steal_fraction(before, after):
    if before is None or after is None:
        return None
    # user/nice include guest/guest_nice: do not count the latter twice.
    delta = [b - a for a, b in zip(before[:8], after[:8])]
    return delta[7] / sum(delta) if sum(delta) else 0.0


def descriptor_count(path):
    """Open descriptors of one process, or None when /proc/<pid>/fd cannot be listed."""
    try:
        return sum(1 for _ in (Path(path) / 'fd').iterdir())
    except OSError:
        return None


def process_memory():
    rows = []
    for path in Path('/proc').glob('[0-9]*'):
        status = counters(path / 'status')
        if 'VmRSS' not in status:
            continue
        rollup = counters(path / 'smaps_rollup') if (path / 'smaps_rollup').exists() else {}
        rows.append({'pid': int(path.name), 'name': read_text(path / 'comm'),
                     'rss_bytes': status['VmRSS'] * 1024,
                     'pss_bytes': rollup['Pss'] * 1024 if 'Pss' in rollup else None,
                     'fd_count': descriptor_count(path)})
    return rows


def directory_usage(path):
    """Files and bytes under one directory now; None when it cannot be walked.

    A file removed between listing and stat is skipped, not counted as zero.
    """
    files = total = 0
    try:
        for item in Path(path).rglob('*'):
            try:
                if item.is_file() and not item.is_symlink():
                    files += 1
                    total += item.stat().st_size
            except OSError:
                continue
    except OSError:
        return None
    return {'files': files, 'bytes': total}


class Sampler:
    """Sample memory, descriptors and watched directories while a trial runs.

    A phase is the coarse interval the summaries read ('execute', 'verification',
    'cleanup'). A step subdivides a phase ('stage', 'projection', 'kernel',
    'output' for a native trial; 'rounds' for a relational one) so that staging
    and kernel work are measured separately without changing what 'execute'
    means. Watched directories are walked every `directory_every` scans, since a
    walk of a large staging tree is not free; their peaks are per phase and step.
    """
    PEAK_KEYS = ('rss_bytes', 'pss_bytes', 'cgroup_current_bytes', 'fd_total')

    def __init__(self, output, interval=0.05, watch=None, directory_every=20):
        self.output = Path(output)
        self.interval = interval
        self.phase = 'startup'
        self.step = None
        self.generation = 0
        self.stop = threading.Event()
        self.lock = threading.Lock()
        self.peaks = {}
        self.step_peaks = {}
        self.directory_peaks = {}
        self.first = {}
        self.count = 0
        self.phase_counts = {}
        self.watch = {str(name): Path(path) for name, path in (watch or {}).items()}
        self.directory_every = max(1, int(directory_every))
        self.error = None
        self.thread = threading.Thread(target=self._run, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def _run(self):
        try:
            with self.output.open('w') as stream:
                while not self.stop.is_set():
                    with self.lock:
                        phase, step, generation = self.phase, self.step, self.generation
                    started = time.monotonic()
                    processes = process_memory() if Path('/proc/stat').exists() else []
                    current = read_text(CGROUP / 'memory.current')
                    directories = None
                    if self.watch and self.count % self.directory_every == 0:
                        directories = {name: directory_usage(path) for name, path in self.watch.items()}
                    with self.lock:
                        if generation != self.generation:
                            phase, step = 'transition', None
                    fd_counts = [p.get('fd_count') for p in processes]
                    row = {'scan_started_seconds': started, 'scan_finished_seconds': time.monotonic(),
                           'phase': phase, 'step': step,
                           'rss_bytes': sum(p['rss_bytes'] for p in processes) if processes else None,
                           'pss_bytes': sum(p['pss_bytes'] for p in processes)
                               if processes and all(p['pss_bytes'] is not None for p in processes) else None,
                           'pss_processes_available': sum(p['pss_bytes'] is not None for p in processes),
                           'cgroup_current_bytes': int(current) if current else None,
                           'fd_total': sum(fd_counts) if processes and all(c is not None for c in fd_counts) else None,
                           'directories': directories,
                           'processes': processes}
                    stream.write(json.dumps(row) + '\n')
                    stream.flush()
                    with self.lock:
                        self.count += 1
                        self.phase_counts[phase] = self.phase_counts.get(phase, 0) + 1
                        if phase == 'transition':
                            continue
                        self.first.setdefault(phase, row)
                        targets = [self.peaks.setdefault(phase, {})]
                        if step is not None:
                            targets.append(self.step_peaks.setdefault(phase, {}).setdefault(step, {}))
                        for peak in targets:
                            for key in self.PEAK_KEYS:
                                value = row[key]
                                if value is not None:
                                    peak[key] = max(peak.get(key, 0), value)
                        if directories:
                            for name, usage in directories.items():
                                if usage is None:
                                    continue
                                for scope in (phase, f'{phase}/{step}' if step is not None else None):
                                    if scope is None:
                                        continue
                                    peak = self.directory_peaks.setdefault(name, {}).setdefault(scope, {})
                                    for key, value in usage.items():
                                        peak[key] = max(peak.get(key, 0), value)
                    self.stop.wait(self.interval)
        except BaseException as error:
            self.error = repr(error)

    def mark(self, phase, step=None):
        """Enter `phase`, optionally at `step`; the scan in flight is a transition."""
        with self.lock:
            self.phase = phase
            self.step = step
            self.generation += 1

    def mark_step(self, step):
        """Move to `step` within the current phase."""
        with self.lock:
            self.step = step
            self.generation += 1

    def __exit__(self, *_):
        self.stop.set()
        self.thread.join(timeout=5)
        if self.thread.is_alive():
            self.error = 'sampler thread failed to stop'

    def receipt(self):
        return {'interval_seconds': self.interval, 'samples': self.count,
                'phase_peaks': self.peaks, 'phase_first_samples': self.first,
                'phase_sample_counts': self.phase_counts,
                'step_peaks': self.step_peaks,
                'directory_peaks': self.directory_peaks,
                'watched_directories': {name: str(path) for name, path in self.watch.items()},
                'directory_scan_every': self.directory_every,
                'fd_boundary': 'sum of open descriptors over all visible processes; None when any /proc/<pid>/fd was unreadable',
                'directory_boundary': 'files and bytes under each watched directory at scan time; a walk sees a tree in motion',
                'execution_sampled': bool(self.phase_counts.get('execute')),
                'thread_alive': self.thread.is_alive(),
                'error': self.error, 'scope': 'all visible processes: requires a private PID namespace',
                'rss_boundary': 'sum of contemporaneous process RSS, shared pages counted per process',
                'pss_boundary': 'sum of contemporaneous Linux proportional set size',
                'cgroup_boundary': 'container current/peak includes page cache and allocator overhead'}
