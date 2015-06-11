"""
Exececute custom commands
"""
import logging
import subprocess

from teuthology import misc as teuthology

log = logging.getLogger(__name__)

cmd_list = [
    #auth commands
    "ceph auth add client.test1 mon \'allow r\' osd \'allow rw\'",
    "ceph auth get-or-create client.test2",
    "ceph auth get-or-create-key client.test3",
    "ceph auth get client.test1",
    "ceph auth get-key client.test1 && echo",
    "ceph auth print-key client.test2 && echo",
    "ceph auth caps client.test3 mds \'allow *\'",
    "ceph auth export client.test1 -o exported_key",
    "ceph auth list",
    "ceph auth import -i ./exported_key",
    "ceph auth del client.test1 && ceph auth del client.test2 && ceph auth del client.test3",

    #config-key commands
    "ceph config-key put test AC_1.5",
    "ceph config-key exists test",
    "ceph config-key list",
    "ceph config-key get test",
    "ceph config-key del test",
    "ceph config-key list",

    #heap commands
    "ceph heap stats",
    "ceph heap start_profiler",
    "ceph heap dump",
    "ceph heap stop_profiler",
    "ceph heap release",
    "ceph heap stats",

    #mds commands
    "ceph mds dump",
    "ceph mds stat",
    "ceph mds compat show",

    #mon commands
    "ceph mon dump",
    "ceph mon stat",
    "ceph mon_status",
    "ceph quorum_status",

    #osd commands
    "ceph osd dump",
    "ceph osd stat",
    "ceph osd tree",
    "ceph osd df",
    "ceph osd find 0",
    "ceph osd crush dump",
    "ceph osd crush rule dump",
    "ceph osd crush rule list",
    "ceph osd crush show-tunables",
    "ceph osd erasure-code-profile ls",
    "ceph osd getmaxosd",
    "ceph osd ls",
    "ceph osd lspools",
    "ceph osd pool ls",
    "ceph osd pool stats",
    "ceph osd pool get {pool_name} pg_num",
    "ceph osd pool get-quota {pool_name}",
    "ceph osd map {pool_name} test",
    "ceph osd metadata 0",
    "ceph osd perf",
    "ceph osd blacklist ls",

    #pg commands
    "ceph pg dump",
    "ceph pg stat",
    "ceph pg ls",
    "ceph pg ls-by-osd 0",
    "ceph pg ls-by-pool {pool_name}",
    "ceph pg map {pool_id}.0",
    "ceph pg debug unfound_objects_exist",
    "ceph pg debug degraded_pgs_exist",
    "ceph pg dump_json",
    "ceph pg dump_stuck",

    #misc
    "ceph df",
    "ceph fs ls",
    "ceph fsid",
    "ceph health",
    "ceph status",
    "ceph report",
    "ceph version",

]

def task(ctx, config):

    log.info('Executing commands test...')
    assert isinstance(config, dict), "task exec got invalid config"

    test_result = {}
    testdir = teuthology.get_testdir(ctx)

    cmd = ['ceph', 'osd', 'pool', 'ls']
    fd_popen = subprocess.Popen(cmd, stdout=subprocess.PIPE).stdout
    data = fd_popen.read().strip()
    fd_popen.close()

    pool_name = data.split("\n")[0]

    cmd = ['ceph', 'osd', 'pool', 'stats', pool_name]
    fd_popen = subprocess.Popen(cmd, stdout=subprocess.PIPE).stdout
    data = fd_popen.read().strip()
    fd_popen.close()

    data=data.split("\n")[0].split(" ")
    pool_id = data[3]

    log.info("using {name}({id}) pool".format(name=pool_name,id=pool_id))

    for idx in range(len(cmd_list)):
        raw_cmd = cmd_list[idx]
        if raw_cmd.find("{pool_name}") is not -1:
            cmd_list[idx] = raw_cmd.format(pool_name=pool_name)

    for idx in range(len(cmd_list)):
        raw_cmd = cmd_list[idx]
        if raw_cmd.find("{pool_id}") is not -1:
            cmd_list[idx] = raw_cmd.format(pool_id=pool_id)

    if 'all' in config and len(config) == 1:
        a = config['all']
        roles = teuthology.all_roles(ctx.cluster)
        config = dict((id_, a) for id_ in roles)

    for role in config:
        (remote,) = ctx.cluster.only(role).remotes.iterkeys()
        log.info('Running commands on role %s host %s', role, remote.name)
        for c in cmd_list:
            log.info("custom commnad: {command}".format(command=c))
            command_result = subprocess.call(\
                "ssh {remote} sudo {command}".format(remote=remote,command=c), shell=True)
            if command_result is not 0:
                test_result[c] = (command_result, remote.name)

    if test_result:
        log.info("failed commands")
        fcmds = ""
        for command in test_result:
            cr, remote = test_result[command]
            fcmds = fcmds + "{c}, ".format(c=command)
            log.info("{remote}: \"{c}\" return {r}".format(remote=remote,c=command,r=cr))

    assert not test_result, "command fail - {fcmds}".format(fcmds=fcmds)
