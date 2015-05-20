
from StringIO import StringIO
import json
import logging
import time
import datetime
import re

from teuthology.exceptions import CommandFailedError
from teuthology.orchestra import run
from teuthology import misc
from teuthology.nuke import clear_firewall
from teuthology.parallel import parallel
from tasks.ceph_manager import write_conf
from tasks import ceph_manager


log = logging.getLogger(__name__)


DAEMON_WAIT_TIMEOUT = 120
ROOT_INO = 1


class ObjectNotFound(Exception):
    def __init__(self, object_name):
        self._object_name = object_name

    def __str__(self):
        return "Object not found: '{0}'".format(self._object_name)


class Filesystem(object):
    """
    This object is for driving a CephFS filesystem.

    Limitations:
     * Assume a single filesystem+cluster
     * Assume a single MDS
    """
    def __init__(self, ctx, admin_remote=None):
        self._ctx = ctx

        self.mds_ids = list(misc.all_roles_of_type(ctx.cluster, 'mds'))
        if len(self.mds_ids) == 0:
            raise RuntimeError("This task requires at least one MDS")

        first_mon = misc.get_first_mon(ctx, None)
        if admin_remote is None:
            (self.admin_remote,) = ctx.cluster.only(first_mon).remotes.iterkeys()
        else:
            self.admin_remote = admin_remote
        self.mon_manager = ceph_manager.CephManager(self.admin_remote, ctx=ctx, logger=log.getChild('ceph_manager'))
        if hasattr(self._ctx, "daemons"):
            # Presence of 'daemons' attribute implies ceph task rather than ceph_deploy task
            self.mds_daemons = dict([(mds_id, self._ctx.daemons.get_daemon('mds', mds_id)) for mds_id in self.mds_ids])

        client_list = list(misc.all_roles_of_type(self._ctx.cluster, 'client'))
        self.client_id = client_list[0]
        self.client_remote = list(misc.get_clients(ctx=ctx, roles=["client.{0}".format(self.client_id)]))[0][1]

    def create(self):
        pg_warn_min_per_osd = int(self.get_config('mon_pg_warn_min_per_osd'))
        osd_count = len(list(misc.all_roles_of_type(self._ctx.cluster, 'osd')))
        pgs_per_fs_pool = pg_warn_min_per_osd * osd_count

        self.admin_remote.run(args=['sudo', 'ceph', 'osd', 'pool', 'create', 'metadata', pgs_per_fs_pool.__str__()])
        self.admin_remote.run(args=['sudo', 'ceph', 'osd', 'pool', 'create', 'data', pgs_per_fs_pool.__str__()])
        self.admin_remote.run(args=['sudo', 'ceph', 'fs', 'new', 'default', 'metadata', 'data'])

    def delete(self):
        self.admin_remote.run(args=['sudo', 'ceph', 'fs', 'rm', 'default', '--yes-i-really-mean-it'])
        self.admin_remote.run(args=['sudo', 'ceph', 'osd', 'pool', 'delete',
                                  'metadata', 'metadata', '--yes-i-really-really-mean-it'])
        self.admin_remote.run(args=['sudo', 'ceph', 'osd', 'pool', 'delete',
                                  'data', 'data', '--yes-i-really-really-mean-it'])

    def legacy_configured(self):
        """
        Check if a legacy (i.e. pre "fs new") filesystem configuration is present.  If this is
        the case, the caller should avoid using Filesystem.create
        """
        try:
            proc = self.admin_remote.run(args=['sudo', 'ceph', '--format=json-pretty', 'osd', 'lspools'],
                                       stdout=StringIO())
            pools = json.loads(proc.stdout.getvalue())
            metadata_pool_exists = 'metadata' in [p['poolname'] for p in pools]
        except CommandFailedError as e:
            # For use in upgrade tests, Ceph cuttlefish and earlier don't support
            # structured output (--format) from the CLI.
            if e.exitstatus == 22:
                metadata_pool_exists = True
            else:
                raise

        return metadata_pool_exists

    def _df(self):
        return json.loads(self.mon_manager.raw_cluster_cmd("df", "--format=json-pretty"))

    def _fs_ls(self):
        fs_list = json.loads(self.mon_manager.raw_cluster_cmd("fs", "ls", "--format=json-pretty"))
        assert len(fs_list) == 1  # we don't handle multiple filesystems yet
        return fs_list[0]

    def get_data_pool_name(self):
        """
        Return the name of the data pool if there is only one, else raise exception -- call
        this in tests where there will only be one data pool.
        """
        names = self.get_data_pool_names()
        if len(names) > 1:
            raise RuntimeError("Multiple data pools found")
        else:
            return names[0]

    def get_data_pool_names(self):
        return self._fs_ls()['data_pools']

    def get_metadata_pool_name(self):
        return self._fs_ls()['metadata_pool']

    def get_pool_df(self, pool_name):
        """
        Return a dict like:
        {u'bytes_used': 0, u'max_avail': 83848701, u'objects': 0, u'kb_used': 0}
        """
        for pool_df in self._df()['pools']:
            if pool_df['name'] == pool_name:
                return pool_df['stats']

        raise RuntimeError("Pool name '{0}' not found".format(pool_name))

    def get_usage(self):
        return self._df()['stats']['total_used_bytes']

    def get_mds_hostnames(self):
        result = set()
        for mds_id in self.mds_ids:
            mds_remote = self.mon_manager.find_remote('mds', mds_id)
            result.add(mds_remote.hostname)

        return list(result)

    def get_config(self, key, service_type=None):
        """
        Get config from mon by default, or a specific service if caller asks for it
        """
        if service_type is None:
            service_type = 'mon'

        service_id = sorted(misc.all_roles_of_type(self._ctx.cluster, service_type))[0]
        return self.json_asok(['config', 'get', key], service_type, service_id)[key]

    def set_ceph_conf(self, subsys, key, value):
        if subsys not in self._ctx.ceph.conf:
            self._ctx.ceph.conf[subsys] = {}
        self._ctx.ceph.conf[subsys][key] = value
        write_conf(self._ctx)  # XXX because we don't have the ceph task's config object, if they
                         # used a different config path this won't work.

    def clear_ceph_conf(self, subsys, key):
        del self._ctx.ceph.conf[subsys][key]
        write_conf(self._ctx)

    def are_daemons_healthy(self):
        """
        Return true if all daemons are in one of active, standby, standby-replay, and
        at least max_mds daemons are in 'active'.

        :return:
        """

        active_count = 0
        status = self.mon_manager.get_mds_status_all()
        for mds_id, mds_status in status['info'].items():
            if mds_status['state'] not in ["up:active", "up:standby", "up:standby-replay"]:
                log.warning("Unhealthy mds state {0}:{1}".format(mds_id, mds_status['state']))
                return False
            elif mds_status['state'] == 'up:active':
                active_count += 1

        return active_count >= status['max_mds']

    def get_active_names(self):
        """
        Return MDS daemon names of those daemons holding ranks
        in state up:active

        :return: list of strings like ['a', 'b'], sorted by rank
        """
        status = self.mon_manager.get_mds_status_all()
        result = []
        for mds_status in sorted(status['info'].values(), lambda a, b: cmp(a['rank'], b['rank'])):
            if mds_status['state'] == 'up:active':
                result.append(mds_status['name'])

        return result

    def get_rank_names(self):
        """
        Return MDS daemon names of those daemons holding a rank,
        sorted by rank.  This includes e.g. up:replay/reconnect
        as well as active, but does not include standby or
        standby-replay.
        """
        status = self.mon_manager.get_mds_status_all()
        result = []
        for mds_status in sorted(status['info'].values(), lambda a, b: cmp(a['rank'], b['rank'])):
            if mds_status['rank'] != -1 and mds_status['state'] != 'up:standby-replay':
                result.append(mds_status['name'])

        return result


    def wait_for_daemons(self, timeout=None):
        """
        Wait until all daemons are healthy
        :return:
        """

        if timeout is None:
            timeout = DAEMON_WAIT_TIMEOUT

        elapsed = 0
        while True:
            if self.are_daemons_healthy():
                return
            else:
                time.sleep(1)
                elapsed += 1

            if elapsed > timeout:
                raise RuntimeError("Timed out waiting for MDS daemons to become healthy")

    def get_lone_mds_id(self):
        """
        Get a single MDS ID: the only one if there is only one
        configured, else the only one currently holding a rank,
        else raise an error.
        """
        if len(self.mds_ids) != 1:
            alive = self.get_rank_names()
            if len(alive) == 1:
                return alive[0]
            else:
                raise ValueError("Explicit MDS argument required when multiple MDSs in use")
        else:
            return self.mds_ids[0]

    def _one_or_all(self, mds_id, cb, in_parallel=True):
        """
        Call a callback for a single named MDS, or for all.

        Note that the parallelism here isn't for performance, it's to avoid being overly kind
        to the cluster by waiting a graceful ssh-latency of time between doing things, and to
        avoid being overly kind by executing them in a particular order.  However, some actions
        don't cope with being done in parallel, so it's optional (`in_parallel`)

        :param mds_id: MDS daemon name, or None
        :param cb: Callback taking single argument of MDS daemon name
        :param in_parallel: whether to invoke callbacks concurrently (else one after the other)
        """
        if mds_id is None:
            if in_parallel:
                with parallel() as p:
                    for mds_id in self.mds_ids:
                        p.spawn(cb, mds_id)
            else:
                for mds_id in self.mds_ids:
                    cb(mds_id)
        else:
            cb(mds_id)

    def mds_stop(self, mds_id=None):
        """
        Stop the MDS daemon process(se).  If it held a rank, that rank
        will eventually go laggy.
        """
        self._one_or_all(mds_id, lambda id_: self.mds_daemons[id_].stop())

    def mds_fail(self, mds_id=None):
        """
        Inform MDSMonitor of the death of the daemon process(es).  If it held
        a rank, that rank will be relinquished.
        """
        self._one_or_all(mds_id, lambda id_: self.mon_manager.raw_cluster_cmd("mds", "fail", id_))

    def mds_restart(self, mds_id=None):
        self._one_or_all(mds_id, lambda id_: self.mds_daemons[id_].restart())

    def mds_fail_restart(self, mds_id=None):
        """
        Variation on restart that includes marking MDSs as failed, so that doing this
        operation followed by waiting for healthy daemon states guarantees that they
        have gone down and come up, rather than potentially seeing the healthy states
        that existed before the restart.
        """
        def _fail_restart(id_):
            self.mds_daemons[id_].stop()
            self.mon_manager.raw_cluster_cmd("mds", "fail", id_)
            self.mds_daemons[id_].restart()

        self._one_or_all(mds_id, _fail_restart)

    def reset(self):
        log.info("Creating new filesystem")

        self.mon_manager.raw_cluster_cmd_result('mds', 'set', "max_mds", "0")
        for mds_id in self.mds_ids:
            assert not self._ctx.daemons.get_daemon('mds', mds_id).running()
            self.mon_manager.raw_cluster_cmd_result('mds', 'fail', mds_id)
        self.mon_manager.raw_cluster_cmd_result('fs', 'rm', "default", "--yes-i-really-mean-it")
        self.mon_manager.raw_cluster_cmd_result('fs', 'new', "default", "metadata", "data")

    def get_metadata_object(self, object_type, object_id):
        """
        Retrieve an object from the metadata pool, pass it through
        ceph-dencoder to dump it to JSON, and return the decoded object.
        """
        temp_bin_path = '/tmp/out.bin'

        # FIXME get the metadata pool name from mdsmap instead of hardcoding
        self.client_remote.run(args=[
            'sudo', 'rados', '-p', 'metadata', 'get', object_id, temp_bin_path
        ])

        stdout = StringIO()
        self.client_remote.run(args=[
            'sudo', 'ceph-dencoder', 'type', object_type, 'import', temp_bin_path, 'decode', 'dump_json'
        ], stdout=stdout)
        dump_json = stdout.getvalue().strip()
        try:
            dump = json.loads(dump_json)
        except (TypeError, ValueError):
            log.error("Failed to decode JSON: '{0}'".format(dump_json))
            raise

        return dump

    def get_journal_version(self):
        """
        Read the JournalPointer and Journal::Header objects to learn the version of
        encoding in use.
        """
        journal_pointer_object = '400.00000000'
        journal_pointer_dump = self.get_metadata_object("JournalPointer", journal_pointer_object)
        journal_ino = journal_pointer_dump['journal_pointer']['front']

        journal_header_object = "{0:x}.00000000".format(journal_ino)
        journal_header_dump = self.get_metadata_object('Journaler::Header', journal_header_object)

        version = journal_header_dump['journal_header']['stream_format']
        log.info("Read journal version {0}".format(version))

        return version

    def json_asok(self, command, service_type, service_id):
        proc = self.mon_manager.admin_socket(service_type, service_id, command)
        response_data = proc.stdout.getvalue()
        log.info("_json_asok output: {0}".format(response_data))
        if response_data.strip():
            return json.loads(response_data)
        else:
            return None

    def mds_asok(self, command, mds_id=None):
        if mds_id is None:
            mds_id = self.get_lone_mds_id()

        return self.json_asok(command, 'mds', mds_id)

    def get_mds_map(self):
        """
        Return the MDS map, as a JSON-esque dict from 'mds dump'
        """
        return json.loads(self.mon_manager.raw_cluster_cmd('mds', 'dump', '--format=json-pretty'))

    def get_mds_addr(self, mds_id):
        """
        Return the instance addr as a string, like "10.214.133.138:6807\/10825"
        """
        mds_map = self.get_mds_map()
        for gid_string, mds_info in mds_map['info'].items():
            # For some reason
            if mds_info['name'] == mds_id:
                return mds_info['addr']

        log.warn(json.dumps(mds_map, indent=2))  # dump map for debugging
        raise RuntimeError("MDS id '{0}' not found in MDS map".format(mds_id))

    def set_clients_block(self, blocked, mds_id=None):
        """
        Block (using iptables) client communications to this MDS.  Be careful: if
        other services are running on this MDS, or other MDSs try to talk to this
        MDS, their communications may also be blocked as collatoral damage.

        :param mds_id: Optional ID of MDS to block, default to all
        :return:
        """
        da_flag = "-A" if blocked else "-D"

        def set_block(_mds_id):
            remote = self.mon_manager.find_remote('mds', _mds_id)

            addr = self.get_mds_addr(_mds_id)
            ip_str, port_str, inst_str = re.match("(.+):(.+)/(.+)", addr).groups()

            remote.run(
                args=["sudo", "iptables", da_flag, "OUTPUT", "-p", "tcp", "--sport", port_str, "-j", "REJECT", "-m",
                      "comment", "--comment", "teuthology"])
            remote.run(
                args=["sudo", "iptables", da_flag, "INPUT", "-p", "tcp", "--dport", port_str, "-j", "REJECT", "-m",
                      "comment", "--comment", "teuthology"])

        self._one_or_all(mds_id, set_block, in_parallel=False)

    def clear_firewall(self):
        clear_firewall(self._ctx)

    def is_full(self):
        flags = json.loads(self.mon_manager.raw_cluster_cmd("osd", "dump", "--format=json-pretty"))['flags']
        return 'full' in flags

    def is_pool_full(self, pool_name):
        pools = json.loads(self.mon_manager.raw_cluster_cmd("osd", "dump", "--format=json-pretty"))['pools']
        for pool in pools:
            if pool['pool_name'] == pool_name:
                return 'full' in pool['flags_names'].split(",")

        raise RuntimeError("Pool not found '{0}'".format(pool_name))

    def wait_for_state(self, goal_state, reject=None, timeout=None, mds_id=None):
        """
        Block until the MDS reaches a particular state, or a failure condition
        is met.

        When there are multiple MDSs, succeed when exaclty one MDS is in the
        goal state, or fail when any MDS is in the reject state.

        :param goal_state: Return once the MDS is in this state
        :param reject: Fail if the MDS enters this state before the goal state
        :param timeout: Fail if this many seconds pass before reaching goal
        :return: number of seconds waited, rounded down to integer
        """

        elapsed = 0
        while True:

            if mds_id is not None:
                # mds_info is None if no daemon with this ID exists in the map
                mds_info = self.mon_manager.get_mds_status(mds_id)
                current_state = mds_info['state'] if mds_info else None
                log.info("Looked up MDS state for {0}: {1}".format(mds_id, current_state))
            else:
                # In general, look for a single MDS
                mds_status = self.mon_manager.get_mds_status_all()
                states = [m['state'] for m in mds_status['info'].values()]
                if [s for s in states if s == goal_state] == [goal_state]:
                    current_state = goal_state
                elif reject in states:
                    current_state = reject
                else:
                    current_state = None
                log.info("mapped states {0} to {1}".format(states, current_state))

            if current_state == goal_state:
                log.info("reached state '{0}' in {1}s".format(current_state, elapsed))
                return elapsed
            elif reject is not None and current_state == reject:
                raise RuntimeError("MDS in reject state {0}".format(current_state))
            elif timeout is not None and elapsed > timeout:
                log.error("MDS status at timeout: {0}".format(self.mon_manager.get_mds_status_all()))
                raise RuntimeError(
                    "Reached timeout after {0} seconds waiting for state {1}, while in state {2}".format(
                        elapsed, goal_state, current_state
                    ))
            else:
                time.sleep(1)
                elapsed += 1

    def read_backtrace(self, ino_no):
        """
        Read the backtrace from the data pool, return a dict in the format
        given by inode_backtrace_t::dump, which is something like:

        ::

            rados -p cephfs_data getxattr 10000000002.00000000 parent > out.bin
            ceph-dencoder type inode_backtrace_t import out.bin decode dump_json

            { "ino": 1099511627778,
              "ancestors": [
                    { "dirino": 1,
                      "dname": "blah",
                      "version": 11}],
              "pool": 1,
              "old_pools": []}

        """
        mds_id = self.mds_ids[0]
        remote = self.mds_daemons[mds_id].remote

        obj_name = "{0:x}.00000000".format(ino_no)

        temp_file = "/tmp/{0}_{1}".format(obj_name, datetime.datetime.now().isoformat())

        args = [
            "rados", "-p", self.get_data_pool_name(), "getxattr", obj_name, "parent",
            run.Raw(">"), temp_file
        ]
        try:
            remote.run(
                args=args,
                stdout=StringIO())
        except CommandFailedError as e:
            log.error(e.__str__())
            raise ObjectNotFound(obj_name)

        p = remote.run(
            args=["ceph-dencoder", "type", "inode_backtrace_t", "import", temp_file, "decode", "dump_json"],
            stdout=StringIO()
        )

        return json.loads(p.stdout.getvalue().strip())

    def _enumerate_data_objects(self, ino, size):
        """
        Get the list of expected data objects for a range, and the list of objects
        that really exist.

        :return a tuple of two lists of strings (expected, actual)
        """
        stripe_size = 1024 * 1024 * 4

        size = max(stripe_size, size)

        want_objects = [
            "{0:x}.{1:08x}".format(ino, n)
            for n in range(0, ((size - 1) / stripe_size) + 1)
        ]

        exist_objects = self.rados(["ls"], pool=self.get_data_pool_name()).split("\n")

        return want_objects, exist_objects

    def data_objects_present(self, ino, size):
        """
        Check that *all* the expected data objects for an inode are present in the data pool
        """

        want_objects, exist_objects = self._enumerate_data_objects(ino, size)
        missing = set(want_objects) - set(exist_objects)

        if missing:
            log.info("Objects missing (ino {0}, size {1}): {2}".format(
                ino, size, missing
            ))
            return False
        else:
            log.info("All objects for ino {0} size {1} found".format(ino, size))
            return True

    def data_objects_absent(self, ino, size):
        want_objects, exist_objects = self._enumerate_data_objects(ino, size)
        present = set(want_objects) & set(exist_objects)

        if present:
            log.info("Objects not absent (ino {0}, size {1}): {2}".format(
                ino, size, present
            ))
            return False
        else:
            log.info("All objects for ino {0} size {1} are absent".format(ino, size))
            return True

    def rados(self, args, pool=None):
        """
        Call into the `rados` CLI from an MDS
        """

        if pool is None:
            pool = self.get_metadata_pool_name()

        # Doesn't matter which MDS we use to run rados commands, they all
        # have access to the pools
        mds_id = self.mds_ids[0]
        remote = self.mds_daemons[mds_id].remote

        # NB we could alternatively use librados pybindings for this, but it's a one-liner
        # using the `rados` CLI
        args = ["rados", "-p", pool] + args
        p = remote.run(
            args=args,
            stdout=StringIO())
        return p.stdout.getvalue().strip()

    def list_dirfrag(self, dir_ino):
        """
        Read the named object and return the list of omap keys

        :return a list of 0 or more strings
        """

        dirfrag_obj_name = "{0:x}.00000000".format(dir_ino)

        try:
            key_list_str = self.rados(["listomapkeys", dirfrag_obj_name])
        except CommandFailedError as e:
            log.error(e.__str__())
            raise ObjectNotFound(dirfrag_obj_name)

        return key_list_str.split("\n") if key_list_str else []

    def erase_metadata_objects(self, prefix):
        """
        For all objects in the metadata pool matching the prefix,
        erase them.

        This O(N) with the number of objects in the pool, so only suitable
        for use on toy test filesystems.
        """
        all_objects = self.rados(["ls"]).split("\n")
        matching_objects = [o for o in all_objects if o.startswith(prefix)]
        for o in matching_objects:
            self.rados(["rm", o])

    def erase_mds_objects(self, rank):
        """
        Erase all the per-MDS objects for a particular rank.  This includes
        inotable, sessiontable, journal
        """

        def obj_prefix(multiplier):
            """
            MDS object naming conventions like rank 1's
            journal is at 201.***
            """
            return "%x." % (multiplier * 0x100 + rank)

        # MDS_INO_LOG_OFFSET
        self.erase_metadata_objects(obj_prefix(2))
        # MDS_INO_LOG_BACKUP_OFFSET
        self.erase_metadata_objects(obj_prefix(3))
        # MDS_INO_LOG_POINTER_OFFSET
        self.erase_metadata_objects(obj_prefix(4))
        # MDSTables & SessionMap
        self.erase_metadata_objects("mds{rank:d}_".format(rank=rank))

    def _run_tool(self, tool, args, rank=None, quiet=False):
        mds_id = self.mds_ids[0]
        remote = self.mds_daemons[mds_id].remote

        # Tests frequently have [client] configuration that jacks up
        # the objecter log level (unlikely to be interesting here)
        # and does not set the mds log level (very interesting here)
        if quiet:
            base_args = [tool, '--debug-mds=1', '--debug-objecter=1']
        else:
            base_args = [tool, '--debug-mds=4', '--debug-objecter=1']

        if rank is not None:
            base_args.extend(["--rank", "%d" % rank])

        t1 = datetime.datetime.now()
        r = remote.run(
            args=base_args + args,
            stdout=StringIO()).stdout.getvalue().strip()
        duration = datetime.datetime.now() - t1
        log.info("Ran {0} in time {1}, result:\n{2}".format(
            base_args + args, duration, r
        ))
        return r

    def journal_tool(self, args, rank=None, quiet=False):
        """
        Invoke cephfs-journal-tool with the passed arguments, and return its stdout
        """
        return self._run_tool("cephfs-journal-tool", args, rank, quiet)

    def table_tool(self, args, quiet=False):
        """
        Invoke cephfs-table-tool with the passed arguments, and return its stdout
        """
        return self._run_tool("cephfs-table-tool", args, None, quiet)
