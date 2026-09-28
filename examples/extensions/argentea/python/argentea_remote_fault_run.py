"""Physical-host launch adapter for the existing fault exercise."""
from argentea_evidence import parse_log
from argentea_fault_evidence import validate_fault
from argentea_remote_fault_control import bind_supervised_workers
from argentea_resource_evidence import read_complete_log
from qualify import two_hosts
from qualify_faults import exercise


def run_remote_fault(args, receipt):
    def run(endpoint, options):
        check = receipt['checks'] = {}
        def workers(spark):
            _, supervisors = parse_log(read_complete_log(options.output/'server-and-workers.log'))
            bound = bind_supervised_workers(supervisors, receipt['configuration']['workers'],
                                            receipt['inventories'][1:])
            # The controller session identifies the job scope. Process authority
            # still comes from the launcher and independent target inventory.
            session = spark.client._session_id
            for worker in bound:
                worker['session_id'] = session
                if not worker.get('fault_control'):
                    raise ValueError('worker supervisor did not enable fault controls')
            return bound
        exercise(endpoint, options, None, check, remote_workers=workers)
        receipt['post_session_storage'] = audit_remote_storage(
            receipt['configuration']['driver'], check.get('run_path'))
        if check['cleanup_errors']:
            raise AssertionError('remote fault exercise cleanup failed')
        return check

    def audit(receipt, log, *, minimum_workers, required_hosts):
        records, _ = parse_log(log)
        assert {w['host'] for w in receipt['checks']['supervised_workers']} == set(required_hosts)
        assert minimum_workers == 2
        receipt['native_execution'] = validate_fault(log, records, receipt['checks'])

    two_hosts(args, receipt, exercise_fn=run, audit_fn=audit, fault_control=True)


STORAGE_CHECK = r"""
import json, sys, time
from pathlib import Path
from urllib.parse import urlsplit
import pyarrow.fs as fs
config=json.loads(Path(sys.argv[1]).read_text())
root=urlsplit(config['SAIL_GRAPH_UTILS_ROOT'])
run=urlsplit(sys.argv[2])
if root.scheme!='s3' or run.scheme!='s3' or run.netloc!=root.netloc:
    raise ValueError('remote fault storage audit requires the configured S3 root')
base=root.path.rstrip('/')+'/'
if not run.path.startswith(base) or run.path==base:
    raise ValueError('run path is outside the configured owned storage root')
endpoint=urlsplit(config.get('AWS_ENDPOINT_URL') or config['AWS_ENDPOINT'])
store=fs.S3FileSystem(access_key=config['AWS_ACCESS_KEY_ID'],
    secret_key=config['AWS_SECRET_ACCESS_KEY'],endpoint_override=endpoint.netloc,
    scheme=endpoint.scheme,region=config.get('AWS_REGION',config.get('AWS_DEFAULT_REGION')))
prefix=run.netloc+run.path
started=time.monotonic()
while True:
    files=[f.path for f in store.get_file_info(fs.FileSelector(prefix,recursive=True,allow_not_found=True))
           if f.type==fs.FileType.File]
    if not files or time.monotonic()-started>=30:break
    time.sleep(.1)
print(json.dumps(dict(run_path=sys.argv[2],remaining_objects=files,
                     elapsed_seconds=time.monotonic()-started,passed=not files)))
"""


def audit_remote_storage(driver, run_path):
    from two_host import target_python
    if not run_path or not driver.get('environment_file'):
        raise ValueError('remote cleanup requires run path and host-local storage configuration')
    result = target_python(driver, STORAGE_CHECK, driver['environment_file'], run_path)
    if not result.get('passed') or result['remaining_objects']:
        raise AssertionError('owned run objects remain after remote session shutdown')
    return result
