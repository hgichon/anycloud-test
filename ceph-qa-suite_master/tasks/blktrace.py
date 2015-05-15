"""
Run blktrace program through teuthology
"""
import contextlib
import logging

from teuthology import misc as teuthology
from teuthology import contextutil
from teuthology.orchestra import run 

log = logging.getLogger(__name__)
blktrace = '/usr/sbin/blktrace'
blkparse = '/usr/bin/blkparse'
daemon_signal = 'term'

@contextlib.contextmanager
def setup(ctx, config):
    """
    Setup all the remotes
    """
    osds = ctx.cluster.only(teuthology.is_type('osd'))
    log_dir = '/home/ubuntu/archive/performance/blktrace'#.format(tdir=teuthology.get_testdir(ctx))

    for remote, roles_for_host in osds.remotes.iteritems():
        log.info('Creating %s on %s' % (log_dir, remote.name))
        remote.run(
            args=['mkdir', '-p', '-m0755', '--', log_dir],
            wait=False,
            )
    yield

@contextlib.contextmanager
def execute(ctx, config):
    """
    Run the blktrace program on remote machines.
    """
    procs = []
    testdir = teuthology.get_testdir(ctx)
    log_dir = '/home/ubuntu/archive/performance/blktrace'#.format(tdir=testdir)

    osds = ctx.cluster.only(teuthology.is_type('osd'))
    for remote, roles_for_host in osds.remotes.iteritems():
        roles_to_devs = config['remote_to_roles_to_dev'][remote.name]
        for id_ in teuthology.roles_of_type(roles_for_host, 'osd'):
            if roles_to_devs.get(int(id_)):
                dev = roles_to_devs[int(id_)]
                log.info("running blktrace on %s: %s" % (remote.name, dev))

                proc = remote.run(
                    args=[
                        'daemon-helper',
                        daemon_signal,
                        'sudo', blktrace,
                        '-d', dev,
                        '-D', log_dir,
                        '-o', dev.rsplit("/", 1)[1],
                        ],
                    wait=False,   
                    stdin=run.PIPE,
                    )
                procs.append(proc)
                log.info(proc)

#        for id_ in teuthology.roles_of_type(roles_for_host, 'osd'):
#            if roles_to_devs.get(int(id_)):
#                dev = roles_to_devs[int(id_)]
#                remote.run(
#                    args=[
#                        'sudo',
#                        'chmod',
#                        '0664',
#                        '{0}/{1}.blktrace.*'.format(log_dir, dev.rsplit("/",1)[1]),
#                        ],
#                    wait=False,
#                    )
    try:
        yield
    finally:
        osds = ctx.cluster.only(teuthology.is_type('osd'))
        for remote, roles_for_host in osds.remotes.iteritems():
            roles_to_devs = config['remote_to_roles_to_dev'][remote.name]
            for id_ in teuthology.roles_of_type(roles_for_host, 'osd'):
                if roles_to_devs.get(int(id_)):
                    dev = roles_to_devs[int(id_)]
                    log.info("running blkparse on %s: %s" % (remote.name, dev))

                    remote.run(
                        args=[
                            'cd',
                            log_dir,
                            run.Raw(';'),
                            blkparse,
                            '-i', '{0}.blktrace.0'.format(dev.rsplit("/", 1)[1]),
                            '-o', '{0}.blkparse'.format(dev.rsplit("/", 1)[1]),
                            ],
                        wait=False,
                        )

        log.info('stopping blktrace processs')
        for proc in procs:
            proc.stdin.close()

@contextlib.contextmanager
def task(ctx, config):
    """
    Usage:
        blktrace:
      
    Runs blktrace on all clients.
    """
    if config is None:
        config = dict(('client.{id}'.format(id=id_), None)
                  for id_ in teuthology.all_roles_of_type(ctx.cluster, 'client'))
    elif isinstance(config, list):
        config = dict.fromkeys(config)

    with contextutil.nested(
        lambda: setup(ctx=ctx, config=config),
        lambda: execute(ctx=ctx, config=config),
        ):
        yield

