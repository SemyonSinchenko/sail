"""Two-host adapter for same-session native memory-refusal and reuse gates."""
from argentea_evidence import parse_log
from argentea_remote_fault_control import bind_supervised_workers
from argentea_remote_fault_run import audit_remote_storage
from argentea_remote_resource_control import validate_remote_owners
from argentea_resource_evidence import read_complete_log
from qualify import two_hosts


def run_remote_resources(args, receipt):
    from qualify_graph_resources import exercise, audit_operation
    import qualify_sssp
    import qualify_wcc
    qualifier = qualify_sssp if args.algorithm.startswith('sssp_') else qualify_wcc

    def run(endpoint, options):
        check = receipt['checks'] = {}
        def workers(spark):
            _, supervisors = parse_log(read_complete_log(options.output/'server-and-workers.log'))
            bound = bind_supervised_workers(supervisors, receipt['configuration']['workers'],
                                            receipt['inventories'][1:])
            for worker in bound:
                worker['session_id'] = spark.client._session_id
                if not worker.get('fault_control'):
                    raise ValueError('resource worker lacks supervised state endpoint')
            return bound
        exercise(endpoint, options, check, remote_workers=workers)
        paths = [operation['run_path'] for operation in check['operations']]
        if len(paths) != 5 or len(set(paths)) != 5:
            raise ValueError('expected five independently owned resource runs')
        receipt['post_session_storage'] = [audit_remote_storage(
            receipt['configuration']['driver'], path) for path in paths]
        return check

    def audit(receipt, log, *, minimum_workers, required_hosts):
        check = receipt['checks']
        workers = check['supervised_workers']
        assert minimum_workers == 2 and {w['host'] for w in workers} == set(required_hosts)
        records, _ = parse_log(log)
        receipt['native_execution'] = []
        for operation in check['operations']:
            native = audit_operation(log, operation, qualifier)
            native['host_owners'] = validate_remote_owners(records, operation['request'], workers)
            receipt['native_execution'].append(native)
        assert not check['cleanup_errors']

    # The opt-in supervisor endpoint supplies process state; no signals are
    # injected by this resource gate.
    two_hosts(args, receipt, exercise_fn=run, audit_fn=audit, fault_control=True)
