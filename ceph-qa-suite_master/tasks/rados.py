"""
Rados modle-based integration tests
"""
import contextlib
import logging
import gevent
from teuthology import misc as teuthology

from teuthology.orchestra import run

log = logging.getLogger(__name__)

@contextlib.contextmanager
def task(ctx, config):
    """
    Run RadosModel-based integration tests.

    The config should be as follows::

        rados:
          clients: [client list]
          ops: <number of ops>
          objects: <number of objects to use>
          max_in_flight: <max number of operations in flight>
          object_size: <size of objects in bytes>
          min_stride_size: <minimum write stride size in bytes>
          max_stride_size: <maximum write stride size in bytes>
          op_weights: <dictionary mapping operation type to integer weight>
          runs: <number of times to run> - the pool is remade between runs
          ec_pool: use an ec pool
          erasure_code_profile: profile to use with the erasure coded pool
          pool_snaps: use pool snapshots instead of selfmanaged snapshots
	  write_fadvise_dontneed: write behavior like with LIBRADOS_OP_FLAG_FADVISE_DONTNEED.
	                          This mean data don't access in the near future.
				  Let osd backend don't keep data in cache.

    For example::

        tasks:
        - ceph:
        - rados:
            clients: [client.0]
            ops: 1000
            max_seconds: 0   # 0 for no limit
            objects: 25
            max_in_flight: 16
            object_size: 4000000
            min_stride_size: 1024
            max_stride_size: 4096
            op_weights:
              read: 20
              write: 10
              delete: 2
              snap_create: 3
              rollback: 2
              snap_remove: 0
            ec_pool: create an ec pool, defaults to False
            erasure_code_profile:
              name: teuthologyprofile
              k: 2
              m: 1
              ruleset-failure-domain: osd
            pool_snaps: true
	    write_fadvise_dontneed: true
            runs: 10
        - interactive:

    Optionally, you can provide the pool name to run against:

        tasks:
        - ceph:
        - exec:
            client.0:
              - ceph osd pool create foo
        - rados:
            clients: [client.0]
            pools: [foo]
            ...

    Alternatively, you can provide a pool prefix:

        tasks:
        - ceph:
        - exec:
            client.0:
              - ceph osd pool create foo.client.0
        - rados:
            clients: [client.0]
            pool_prefix: foo
            ...

    The tests are run asynchronously, they are not complete when the task
    returns. For instance:

        - rados:
            clients: [client.0]
            pools: [ecbase]
            ops: 4000
            objects: 500
            op_weights:
              read: 100
              write: 100
              delete: 50
              copy_from: 50
        - print: "**** done rados ec-cache-agent (part 2)"

     will run the print task immediately after the rados tasks begins but
     not after it completes. To make the rados task a blocking / sequential
     task, use:

        - sequential:
          - rados:
              clients: [client.0]
              pools: [ecbase]
              ops: 4000
              objects: 500
              op_weights:
                read: 100
                write: 100
                delete: 50
                copy_from: 50
        - print: "**** done rados ec-cache-agent (part 2)"

    """
    log.info('Beginning rados...')
    assert isinstance(config, dict), \
        "please list clients to run on"

    object_size = int(config.get('object_size', 4000000))
    op_weights = config.get('op_weights', {})
    testdir = teuthology.get_testdir(ctx)
    args = [
        'adjust-ulimits',
        'ceph-coverage',
        '{tdir}/archive/coverage'.format(tdir=testdir),
        'ceph_test_rados']
    if config.get('ec_pool', False):
        args.extend(['--ec-pool'])
    if config.get('write_fadvise_dontneed', False):
        args.extend(['--write-fadvise-dontneed'])
    if config.get('pool_snaps', False):
        args.extend(['--pool-snaps'])
    args.extend([
        '--op', 'read', str(op_weights.get('read', 100)),
        '--op', 'write', str(op_weights.get('write', 100)),
        '--op', 'delete', str(op_weights.get('delete', 10)),
        '--max-ops', str(config.get('ops', 10000)),
        '--objects', str(config.get('objects', 500)),
        '--max-in-flight', str(config.get('max_in_flight', 16)),
        '--size', str(object_size),
        '--min-stride-size', str(config.get('min_stride_size', object_size / 10)),
        '--max-stride-size', str(config.get('max_stride_size', object_size / 5)),
        '--max-seconds', str(config.get('max_seconds', 0))
        ])
    # Parallel of the op_types in test/osd/TestRados.cc
    for field in [
        # read handled above
        # write handled above
        # delete handled above
        "snap_create",
        "snap_remove",
        "rollback",
        "setattr",
        "rmattr",
        "watch",
        "copy_from",
        "hit_set_list",
        "is_dirty",
        "undirty",
        "cache_flush",
        "cache_try_flush",
        "cache_evict",
        "append",
        ]:
        if field in op_weights:
            args.extend([
                    '--op', field, str(op_weights[field]),
                    ])

    def thread():
        """Thread spawned by gevent"""
        clients = ['client.{id}'.format(id=id_) for id_ in teuthology.all_roles_of_type(ctx.cluster, 'client')]
        log.info('clients are %s' % clients)
        if config.get('ec_pool', False):
            profile = config.get('erasure_code_profile', {})
            profile_name = profile.get('name', 'teuthologyprofile')
            ctx.manager.create_erasure_code_profile(profile_name, profile)
        else:
            profile_name = None
        for i in range(int(config.get('runs', '1'))):
            log.info("starting run %s out of %s", str(i), config.get('runs', '1'))
            tests = {}
            existing_pools = config.get('pools', [])
            created_pools = []
            for role in config.get('clients', clients):
                assert isinstance(role, basestring)
                PREFIX = 'client.'
                assert role.startswith(PREFIX)
                id_ = role[len(PREFIX):]

                pool = config.get('pool', None)
                if not pool and existing_pools:
                    pool = existing_pools.pop()
                else:
                    pool = ctx.manager.create_pool_with_unique_name(erasure_code_profile_name=profile_name)
                    created_pools.append(pool)

                (remote,) = ctx.cluster.only(role).remotes.iterkeys()
                proc = remote.run(
                    args=["CEPH_CLIENT_ID={id_}".format(id_=id_)] + args +
                    ["--pool", pool],
                    logger=log.getChild("rados.{id}".format(id=id_)),
                    stdin=run.PIPE,
                    wait=False
                    )
                tests[id_] = proc
            run.wait(tests.itervalues())

            for pool in created_pools:
                ctx.manager.remove_pool(pool)

    running = gevent.spawn(thread)

    try:
        yield
    finally:
        log.info('joining rados')
        running.get()
