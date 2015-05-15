"""
Kernel installation task
"""
from cStringIO import StringIO

import logging
import os
import re
import shlex
import urllib2
import urlparse

from teuthology import misc as teuthology
from ..orchestra import run
from ..config import config as teuth_config
from ..exceptions import (UnsupportedPackageTypeError,
                          ConfigError,
                          VersionNotFoundError)
from ..packaging import (
    install_package,
    get_koji_build_info,
    get_kojiroot_base_url,
    get_koji_package_name,
)

log = logging.getLogger(__name__)


def normalize_config(ctx, config):
    """
    Returns a config whose keys are all real roles.
    Generic roles (client, mon, osd, etc.) are replaced with
    the actual roles (client.0, client.1, etc.). If the config
    specifies a different version for a specific role, this is
    unchanged.

    For example, with 4 OSDs this::

         osd:
           tag: v3.0
           kdb: true
         osd.1:
           branch: new_btrfs
           kdb: false
         osd.3:
           deb: /path/to/linux-whatever.deb

    is transformed into::

         osd.0:
           tag: v3.0
           kdb: true
         osd.1:
           branch: new_btrfs
           kdb: false
         osd.2:
           tag: v3.0
           kdb: true
         osd.3:
           deb: /path/to/linux-whatever.deb

    If config is None or just specifies a version to use,
    it is applied to all nodes.

    :param ctx: Context
    :param config: Configuration
    """
    if config is None or \
            len(filter(lambda x: x in ['tag', 'branch', 'sha1', 'kdb',
                                       'deb', 'rpm', 'koji'],
                       config.keys())) == len(config.keys()):
        new_config = {}
        if config is None:
            config = {'branch': 'master'}
        for _, roles_for_host in ctx.cluster.remotes.iteritems():
            new_config[roles_for_host[0]] = config
        return new_config

    new_config = {}
    for role, role_config in config.iteritems():
        if role_config is None:
            role_config = {'branch': 'master'}
        if '.' in role:
            new_config[role] = role_config
        else:
            for id_ in teuthology.all_roles_of_type(ctx.cluster, role):
                name = '{type}.{id}'.format(type=role, id=id_)
                # specific overrides generic
                if name not in config:
                    new_config[name] = role_config
    return new_config

def validate_config(ctx, config):
    """
    Make sure that all kernels in the list of remove kernels
    refer to the same kernel.

    :param ctx: Context
    :param config: Configuration
    """
    for _, roles_for_host in ctx.cluster.remotes.iteritems():
        kernel = None
        for role in roles_for_host:
            role_kernel = config.get(role, kernel)
            if kernel is None:
                kernel = role_kernel
            elif role_kernel is not None:
                assert kernel == role_kernel, \
                    "everything on the same host must use the same kernel"
                if role in config:
                    del config[role]

def _vsplitter(version):
    """Kernels from Calxeda are named ...ceph-<sha1>...highbank.
    Kernels that we generate are named ...-g<sha1>.
    This routine finds the text in front of the sha1 that is used by
    need_to_install() to extract information from the kernel name.

    :param version: Name of the kernel
    """
    if version.endswith('highbank'):
        return 'ceph-'
    return '-g'

def need_to_install(ctx, role, version):
    """
    Check to see if we need to install a kernel.  Get the version of the
    currently running kernel, and compare it against the value passed in.

    :param ctx: Context
    :param role: Role
    :param version: value to compare against (used in checking), can be either
                    a utsrelease string (e.g. '3.13.0-rc3-ceph-00049-ge2817b3')
                    or a sha1.
    """
    ret = True
    log.info('Checking kernel version of {role}, want {ver}...'.format(
             role=role, ver=version))
    uname_fp = StringIO()
    ctx.cluster.only(role).run(
        args=[
            'uname',
            '-r',
            ],
        stdout=uname_fp,
        )
    cur_version = uname_fp.getvalue().rstrip('\n')
    log.debug('current kernel version is {ver}'.format(ver=cur_version))

    if '.' in version:
        # version is utsrelease, yay
        if cur_version == version:
            log.debug('utsrelease strings match, do not need to install')
            ret = False
    else:
        # version is sha1, need to try to extract sha1 from cur_version
        splt = _vsplitter(cur_version)
        if splt in cur_version:
            _, cur_sha1 = cur_version.rsplit(splt, 1)
            dloc = cur_sha1.find('-')
            if dloc > 0:
                cur_sha1 = cur_sha1[0:dloc]
            log.debug('extracting sha1, {ver} -> {sha1}'.format(
                      ver=cur_version, sha1=cur_sha1))
            # FIXME: The above will match things like ...-generic on Ubuntu
            # distro kernels resulting in 'eneric' cur_sha1.
            m = min(len(cur_sha1), len(version))
            assert m >= 6, "cur_sha1 and/or version is too short, m = %d" % m
            if cur_sha1[0:m] == version[0:m]:
                log.debug('extracted sha1 matches, do not need to install')
                ret = False
        else:
            log.debug('failed to parse current kernel version')
    uname_fp.close()
    return ret

def install_firmware(ctx, config):
    """
    Go to the github to get the latest firmware.

    :param ctx: Context
    :param config: Configuration
    """
    linux_firmware_git_upstream = 'git://git.kernel.org/pub/scm/linux/kernel/git/firmware/linux-firmware.git'
    uri = teuth_config.linux_firmware_git_url or linux_firmware_git_upstream
    fw_dir = '/lib/firmware/updates'

    for role in config.iterkeys():
        if isinstance(config[role], str) and config[role].find('distro') >= 0:
            log.info('Skipping firmware on distro kernel');
            return
        (role_remote,) = ctx.cluster.only(role).remotes.keys()
        package_type = teuthology.get_system_type(role_remote)
        if package_type == 'rpm':
            return
        log.info('Installing linux-firmware on {role}...'.format(role=role))
        role_remote.run(
            args=[
                # kludge around mysterious 0-byte .git/HEAD files
                'cd', fw_dir,
                run.Raw('&&'),
                'test', '-d', '.git',
                run.Raw('&&'),
                'test', '!', '-s', '.git/HEAD',
                run.Raw('&&'),
                'sudo', 'rm', '-rf', '.git',
                run.Raw(';'),
                # init
                'sudo', 'install', '-d', '-m0755', fw_dir,
                run.Raw('&&'),
                'cd', fw_dir,
                run.Raw('&&'),
                'sudo', 'git', 'init',
                ],
            )
        role_remote.run(
            args=[
                'sudo', 'git', '--git-dir=%s/.git' % fw_dir, 'config',
                '--get', 'remote.origin.url', run.Raw('>/dev/null'),
                run.Raw('||'),
                'sudo', 'git', '--git-dir=%s/.git' % fw_dir,
                'remote', 'add', 'origin', uri,
                ],
            )
        role_remote.run(
            args=[
                'cd', fw_dir,
                run.Raw('&&'),
                'sudo', 'git', 'fetch', 'origin',
                run.Raw('&&'),
                'sudo', 'git', 'reset', '--hard', 'origin/master'
                ],
            )

def gitbuilder_pkg_name(remote):
    if remote.os.package_type == 'rpm':
        pkg_name = 'kernel.x86_64.rpm'
    elif remote.os.package_type == 'deb':
        pkg_name = 'linux-image.deb'
    else:
        raise UnsupportedPackageTypeError(remote)
    return pkg_name

def remote_pkg_path(remote):
    """
    This is where kernel packages are copied over (in case of local
    packages) or downloaded to (in case of gitbuilder packages) and
    then installed from.
    """
    return os.path.join('/tmp', gitbuilder_pkg_name(remote))

def download_kernel(ctx, config):
    """
    Supply each remote with a kernel package:
      - local kernels are copied over
      - gitbuilder kernels are downloaded
      - nothing is done for distro kernels

    :param ctx: Context
    :param config: Configuration
    """
    procs = {}
    for role, src in config.iteritems():
        needs_download = False

        if src == 'distro':
            # don't need to download distro kernels
            log.debug("src is distro, skipping download");
            continue

        (role_remote,) = ctx.cluster.only(role).remotes.keys()
        if isinstance(src, dict):
            # we're downloading a kernel from koji, the src dict here
            # is the build_info retrieved from koji using get_koji_build_info
            build_id = src["id"]
            log.info("Downloading kernel with build_id {build_id} on {role}...".format(
                build_id=build_id,
                role=role
            ))
            needs_download = True
            baseurl = get_kojiroot_base_url(src)
            pkg_name = get_koji_package_name("kernel", src)
        elif src.find('/') >= 0:
            # local package - src is path
            log.info('Copying kernel package {path} to {role}...'.format(
                path=src, role=role))
            f = open(src, 'r')
            proc = role_remote.run(
                args=[
                    'python', '-c',
                    'import shutil, sys; shutil.copyfileobj(sys.stdin, file(sys.argv[1], "wb"))',
                    remote_pkg_path(role_remote),
                    ],
                wait=False,
                stdin=f
                )
            procs[role_remote.name] = proc
        else:
            # gitbuilder package - src is sha1
            log.info('Downloading kernel {sha1} on {role}...'.format(sha1=src,
                                                                     role=role))
            needs_download = True
            package_type = role_remote.os.package_type
            if package_type == 'rpm':
                system_type, system_ver = teuthology.get_system_type(
                    role_remote, distro=True, version=True)
                if '.' in system_ver:
                   system_ver = system_ver.split('.')[0]
                ldist = '{system_type}{system_ver}'.format(
                    system_type=system_type, system_ver=system_ver)
                larch = 'x86_64'
            elif package_type == 'deb':
                ldist, larch = role_remote.os.codename, role_remote.arch
            else:
                raise UnsupportedPackageTypeError(role_remote)

            _, baseurl = teuthology.get_ceph_binary_url(
                package='kernel',
                sha1=src,
                format=package_type,
                flavor='basic',
                arch=larch,
                dist=ldist,
                )

            pkg_name = gitbuilder_pkg_name(role_remote)

            log.info("fetching, gitbuilder baseurl is %s", baseurl)

        if needs_download:
            proc = role_remote.run(
                args=[
                    'rm', '-f', remote_pkg_path(role_remote),
                    run.Raw('&&'),
                    'echo',
                    pkg_name,
                    run.Raw('|'),
                    'wget',
                    '-nv',
                    '-O',
                    remote_pkg_path(role_remote),
                    '--base={url}'.format(url=baseurl),
                    '--input-file=-',
                    ],
                wait=False)
            procs[role_remote.name] = proc

    for name, proc in procs.iteritems():
        log.debug('Waiting for download/copy to %s to complete...', name)
        proc.wait()


def _no_grub_link(in_file, remote, kernel_ver):
    """
    Copy and link kernel related files if grub cannot be used
    (as is the case in Arm kernels)

    :param infile: kernel file or image file to be copied.
    :param remote: remote machine
    :param kernel_ver: kernel version
    """
    boot1 = '/boot/%s' % in_file
    boot2 = '%s.old' % boot1
    remote.run(
        args=[
            'if', 'test', '-e', boot1, run.Raw(';'), 'then',
            'sudo', 'mv', boot1, boot2, run.Raw(';'), 'fi',],
    )
    remote.run(
        args=['sudo', 'ln', '-s', '%s-%s' % (in_file, kernel_ver) , boot1, ],
    )

def install_and_reboot(ctx, config):
    """
    Install and reboot the kernel.  This mostly performs remote
    installation operations.   The code does check for Arm images
    and skips grub operations if the kernel is Arm.  Otherwise, it
    extracts kernel titles from submenu entries and makes the appropriate
    grub calls.   The assumptions here are somewhat simplified in that
    it expects kernel entries to be present under submenu entries.

    :param ctx: Context
    :param config: Configuration
    """
    procs = {}
    kernel_title = ''
    for role, src in config.iteritems():
        (role_remote,) = ctx.cluster.only(role).remotes.keys()
        if isinstance(src, str) and src.find('distro') >= 0:
            log.info('Installing distro kernel on {role}...'.format(role=role))
            install_kernel(role_remote)
            continue

        log.info('Installing kernel {src} on {role}...'.format(src=src,
                                                               role=role))
        system_type = teuthology.get_system_type(role_remote)
        if system_type == 'rpm':
            proc = role_remote.run(
                args=[
                    'sudo',
                    'rpm',
                    '-ivh',
                    '--oldpackage',
                    '--replacefiles',
                    '--replacepkgs',
                    remote_pkg_path(role_remote),
                ])
            install_kernel(role_remote, remote_pkg_path(role_remote))
            continue

        # TODO: Refactor this into install_kernel() so that it handles all
        # cases for both rpm and deb packages.
        proc = role_remote.run(
            args=[
                # install the kernel deb
                'sudo',
                'dpkg',
                '-i',
                remote_pkg_path(role_remote),
                ],
            )

        # collect kernel image name from the .deb
        kernel_title = get_image_version(role_remote,
                                         remote_pkg_path(role_remote))
        log.info('searching for kernel {}'.format(kernel_title))

        if kernel_title.endswith("-highbank"):
            _no_grub_link('vmlinuz', role_remote, kernel_title)
            _no_grub_link('initrd.img', role_remote, kernel_title)
            proc = role_remote.run(
                args=[
                    'sudo',
                    'shutdown',
                    '-r',
                    'now',
                    ],
                wait=False,
            )
            procs[role_remote.name] = proc
            continue

        # look for menuentry for our kernel, and collect any
        # submenu entries for their titles.  Assume that if our
        # kernel entry appears later in the file than a submenu entry,
        # it's actually nested under that submenu.  If it gets more
        # complex this will totally break.

        cmdout = StringIO()
        proc = role_remote.run(
            args=[
                'egrep',
                '(submenu|menuentry.*' + kernel_title + ').*{',
                '/boot/grub/grub.cfg'
               ],
            stdout = cmdout,
            )
        submenu_title = ''
        default_title = ''
        for l in cmdout.getvalue().split('\n'):
            fields = shlex.split(l)
            if len(fields) >= 2:
                command, title = fields[:2]
                if command == 'submenu':
                    submenu_title = title + '>'
                if command == 'menuentry':
                    if title.endswith(kernel_title):
                        default_title = title
                        break
        cmdout.close()
        log.info('submenu_title:{}'.format(submenu_title))
        log.info('default_title:{}'.format(default_title))

        proc = role_remote.run(
            args=[
                # use the title(s) to construct the content of
                # the grub menu entry, so we can default to it.
                '/bin/echo',
                '-e',
                r'cat <<EOF\nset default="' + submenu_title + \
                    default_title + r'"\nEOF\n',
                # make it look like an emacs backup file so
                # unfortunately timed update-grub runs don't pick it
                # up yet; use sudo tee so we are able to write to /etc
                run.Raw('|'),
                'sudo',
                'tee',
                '--',
                '/etc/grub.d/01_ceph_kernel.tmp~',
                run.Raw('>/dev/null'),
                run.Raw('&&'),
                'sudo',
                'chmod',
                'a+x',
                '--',
                '/etc/grub.d/01_ceph_kernel.tmp~',
                run.Raw('&&'),
                'sudo',
                'mv',
                '--',
                '/etc/grub.d/01_ceph_kernel.tmp~',
                '/etc/grub.d/01_ceph_kernel',
                # update grub again so it accepts our default
                run.Raw('&&'),
                'sudo',
                'update-grub',
                run.Raw('&&'),
                'rm',
                remote_pkg_path(role_remote),
                run.Raw('&&'),
                'sudo',
                'shutdown',
                '-r',
                'now',
                ],
            wait=False,
            )
        procs[role_remote.name] = proc

    for name, proc in procs.iteritems():
        log.debug('Waiting for install on %s to complete...', name)
        proc.wait()

def enable_disable_kdb(ctx, config):
    """
    Enable kdb on remote machines in use.  Disable on those that are
    not in use.

    :param ctx: Context
    :param config: Configuration
    """
    for role, enable in config.iteritems():
        (role_remote,) = ctx.cluster.only(role).remotes.keys()
        if "mira" in role_remote.name:
            serialdev = "ttyS2"
        else:
            serialdev = "ttyS1"
        if enable:
            log.info('Enabling kdb on {role}...'.format(role=role))
            try:
                role_remote.run(
                    args=[
                        'echo', serialdev,
                        run.Raw('|'),
                        'sudo', 'tee', '/sys/module/kgdboc/parameters/kgdboc'
                        ])
            except run.CommandFailedError:
                log.warn('Kernel does not support kdb')
        else:
            log.info('Disabling kdb on {role}...'.format(role=role))
            # Add true pipe so command doesn't fail on kernel without kdb support.
            try:
                role_remote.run(
                    args=[
                        'echo', '',
                        run.Raw('|'),
                        'sudo', 'tee', '/sys/module/kgdboc/parameters/kgdboc',
                        run.Raw('|'),
                        'true',
                        ])
            except run.CommandFailedError:
                log.warn('Kernel does not support kdb')

def wait_for_reboot(ctx, need_install, timeout, distro=False):
    """
    Loop reconnecting and checking kernel versions until
    they're all correct or the timeout is exceeded.

    :param ctx: Context
    :param need_install: list of packages that we need to reinstall.
    :param timeout: number of second before we timeout.
    """
    import time
    starttime = time.time()
    while need_install:
        teuthology.reconnect(ctx, timeout)
        for client in need_install.keys():
            if 'distro' in need_install[client]:
                 distro = True
            log.info('Checking client {client} for new kernel version...'.format(client=client))
            try:
                if distro:
                    assert not need_to_install_distro(ctx, client), \
                            'failed to install new distro kernel version within timeout'

                else:
                    assert not need_to_install(ctx, client, need_install[client]), \
                            'failed to install new kernel version within timeout'
                del need_install[client]
            except Exception:
                log.exception("Saw exception")
                # ignore connection resets and asserts while time is left
                if time.time() - starttime > timeout:
                    raise
        time.sleep(1)


def need_to_install_distro(ctx, role):
    """
    Installing kernels on rpm won't setup grub/boot into them.
    This installs the newest kernel package and checks its version
    and compares against current (uname -r) and returns true if newest != current.
    Similar check for deb.
    """
    (role_remote,) = ctx.cluster.only(role).remotes.keys()
    system_type = teuthology.get_system_type(role_remote)
    output, err_mess = StringIO(), StringIO()
    role_remote.run(args=['uname', '-r' ], stdout=output, stderr=err_mess )
    current = output.getvalue().strip()
    if system_type == 'rpm':
        role_remote.run(args=['sudo', 'yum', 'install', '-y', 'kernel'], stdout=output, stderr=err_mess )
        if 'Nothing to do' in output.getvalue():
            output.truncate(0), err_mess.truncate(0)
            role_remote.run(args=['echo', 'no', run.Raw('|'), 'sudo', 'yum', 'reinstall', 'kernel', run.Raw('||'), 'true'], stdout=output, stderr=err_mess )
            if 'Skipping the running kernel' in err_mess.getvalue():
                # Current running kernel is already newest and updated
                log.info('Newest distro kernel already installed/running')
                return False
            else:
                output.truncate(0), err_mess.truncate(0)
                role_remote.run(args=['sudo', 'yum', 'reinstall', '-y', 'kernel', run.Raw('||'), 'true'], stdout=output, stderr=err_mess )
        #reset stringIO output.
        output.truncate(0), err_mess.truncate(0)
        role_remote.run(args=['rpm', '-q', 'kernel', '--last' ], stdout=output, stderr=err_mess )
        for kernel in output.getvalue().split():
            if kernel.startswith('kernel'):
                if 'ceph' not in kernel:
                    newest = kernel.split('kernel-')[1]
                    break

    if system_type == 'deb':
        distribution = teuthology.get_system_type(role_remote, distro=True)
        newest = get_latest_image_version_deb(role_remote, distribution)

    output.close()
    err_mess.close()
    if current in newest:
        return False
    log.info('Not newest distro kernel. Curent: {cur} Expected: {new}'.format(cur=current, new=newest))
    return True

def maybe_generate_initrd_rpm(remote, path, version):
    """
    Generate initrd with mkinitrd if the hooks that should make it
    happen on its own aren't there.

    :param path: rpm package path
    :param version: kernel version to generate initrd for
                    e.g. 3.18.0-rc6-ceph-00562-g79a9fa5
    """
    proc = remote.run(
        args=[
            'rpm',
            '--scripts',
            '-qp',
            path,
        ],
        stdout=StringIO())
    out = proc.stdout.getvalue()
    if 'bin/installkernel' in out or 'bin/kernel-install' in out:
        return

    log.info("No installkernel or kernel-install hook in %s, "
             "will generate initrd for %s", path, version)
    remote.run(
        args=[
            'sudo',
            'mkinitrd',
            '--allow-missing',
            '-f', # overwrite existing initrd
            '/boot/initramfs-' + version + '.img',
            version,
        ])

def install_kernel(remote, path=None):
    """
    A bit of misnomer perhaps - the actual kernel package is installed
    elsewhere, this function deals with initrd and grub.  Currently the
    following cases are handled:
      - local, gitbuilder, distro for rpm packages
      - distro for deb packages - see TODO in install_and_reboot()

    TODO: reboots should be issued from install_and_reboot()

    :param path: package path (for local and gitbuilder cases)
    """
    system_type = teuthology.get_system_type(remote)
    if system_type == 'rpm':
        if path:
            version = get_image_version(remote, path)
            # This is either a gitbuilder or a local package and both of these
            # could have been built with upstream rpm targets with specs that
            # don't have a %post section at all, which means no initrd.
            maybe_generate_initrd_rpm(remote, path, version)
        else:
            version = get_latest_image_version_rpm(remote)
        update_grub_rpm(remote, version)
        remote.run( args=['sudo', 'shutdown', '-r', 'now'], wait=False )
        return

    if system_type == 'deb':
        distribution = teuthology.get_system_type(remote, distro=True)
        newversion = get_latest_image_version_deb(remote, distribution)
        if 'ubuntu' in distribution:
            grub2conf = teuthology.get_file(remote, '/boot/grub/grub.cfg', True)
            submenu = ''
            menuentry = ''
            for line in grub2conf.split('\n'):
                if 'submenu' in line:
                    submenu = line.split('submenu ')[1]
                    # Ubuntu likes to be sneaky and change formatting of
                    # grub.cfg between quotes/doublequotes between versions
                    if submenu.startswith("'"):
                        submenu = submenu.split("'")[1]
                    if submenu.startswith('"'):
                        submenu = submenu.split('"')[1]
                if 'menuentry' in line:
                    if newversion in line and 'recovery' not in line:
                        menuentry = line.split('\'')[1]
                        break
            if submenu:
                grubvalue = submenu + '>' + menuentry
            else:
                grubvalue = menuentry
            grubfile = 'cat <<EOF\nset default="' + grubvalue + '"\nEOF'
            teuthology.delete_file(remote, '/etc/grub.d/01_ceph_kernel', sudo=True, force=True)
            teuthology.sudo_write_file(remote, '/etc/grub.d/01_ceph_kernel', StringIO(grubfile), '755')
            log.info('Distro Kernel Version: {version}'.format(version=newversion))
            remote.run(args=['sudo', 'update-grub'])
            remote.run(args=['sudo', 'shutdown', '-r', 'now'], wait=False )
            return

        if 'debian' in distribution:
            grub2_kernel_select_generic(remote, newversion, 'deb')
            log.info('Distro Kernel Version: {version}'.format(version=newversion))
            remote.run( args=['sudo', 'shutdown', '-r', 'now'], wait=False )
            return

def update_grub_rpm(remote, newversion):
    """
    Updates grub file to boot new kernel version on both legacy grub/grub2.
    """
    grub='grub2'
    # Check if grub2 is isntalled
    try:
        remote.run(args=['sudo', 'rpm', '-qi', 'grub2'])
    except Exception:
        grub = 'legacy'
    log.info('Updating Grub Version: {grub}'.format(grub=grub))
    if grub == 'legacy':
        data = ''
        #Write new legacy grub entry.
        newgrub = generate_legacy_grub_entry(remote, newversion)
        for line in newgrub:
            data += line + '\n'
        temp_file_path = remote.mktemp()
        teuthology.sudo_write_file(remote, temp_file_path, StringIO(data), '755')
        teuthology.move_file(remote, temp_file_path, '/boot/grub/grub.conf', True)
    else:
        #Update grub menu entry to new version.
        grub2_kernel_select_generic(remote, newversion, 'rpm')

def grub2_kernel_select_generic(remote, newversion, ostype):
    """
    Can be used on DEB and RPM. Sets which entry should be boted by entrynum.
    """
    if ostype == 'rpm':
        grubset = 'grub2-set-default'
        mkconfig = 'grub2-mkconfig'
        grubconfig = '/boot/grub2/grub.cfg'
    if ostype == 'deb':
        grubset = 'grub-set-default'
        grubconfig = '/boot/grub/grub.cfg'
        mkconfig = 'grub-mkconfig'
    remote.run(args=['sudo', mkconfig, '-o', grubconfig, ])
    grub2conf = teuthology.get_file(remote, grubconfig, True)
    entry_num = 0
    for line in grub2conf.split('\n'):
        if line.startswith('menuentry'):
            if newversion in line:
                break
            entry_num += 1
    remote.run(args=['sudo', grubset, str(entry_num), ])

def generate_legacy_grub_entry(remote, newversion):
    """
    This will likely need to be used for ceph kernels as well
    as legacy grub rpm distros don't have an easy way of selecting
    a kernel just via a command. This generates an entry in legacy
    grub for a new kernel version using the existing entry as a base.
    """
    grubconf = teuthology.get_file(remote, '/boot/grub/grub.conf', True)
    titleline = ''
    rootline = ''
    kernelline = ''
    initline = ''
    kernelversion = ''
    linenum = 0
    titlelinenum = 0

    #Grab first kernel entry (title/root/kernel/init lines)
    for line in grubconf.split('\n'):
        if re.match('^title', line):
            titleline = line
            titlelinenum = linenum
        if re.match('(^\s+)root', line):
            rootline = line
        if re.match('(^\s+)kernel', line):
            kernelline = line
            for word in line.split(' '):
                if 'vmlinuz' in word:
                    kernelversion = word.split('vmlinuz-')[-1]
        if re.match('(^\s+)initrd', line):
            initline = line
        if (kernelline != '') and (initline != ''):
            break
        else:
            linenum += 1

    #insert new entry into grubconfnew list:
    linenum = 0
    newgrubconf = []
    for line in grubconf.split('\n'):
        line = line.rstrip('\n')
        if linenum == titlelinenum:
            newtitle = re.sub(kernelversion, newversion, titleline)
            newroot = re.sub(kernelversion, newversion, rootline)
            newkernel = re.sub(kernelversion, newversion, kernelline)
            newinit = re.sub(kernelversion, newversion, initline)
            newgrubconf.append(newtitle)
            newgrubconf.append(newroot)
            newgrubconf.append(newkernel)
            newgrubconf.append(newinit)
            newgrubconf.append('')
            newgrubconf.append(line)
        else:
            newgrubconf.append(line)
        linenum += 1
    return newgrubconf

def get_image_version(remote, path):
    """
    Get kernel image version from (rpm or deb) package.

    :param path: (rpm or deb) package path
    """
    if remote.os.package_type == 'rpm':
        proc = remote.run(
            args=[
                'rpm',
                '-qlp',
                path
            ],
            stdout=StringIO())
    elif remote.os.package_type == 'deb':
        proc = remote.run(
            args=[
                'dpkg-deb',
                '-c',
                path
            ],
            stdout=StringIO())
    else:
        raise UnsupportedPackageTypeError(remote)

    files = proc.stdout.getvalue()
    for file in files.split('\n'):
        if '/boot/vmlinuz-' in file:
            version = file.split('/boot/vmlinuz-')[1]
            break

    log.debug("get_image_version: %s", version)
    return version

def get_latest_image_version_rpm(remote):
    """
    Get kernel image version of the newest kernel rpm package.
    Used for distro case.
    """
    proc = remote.run(
        args=[
            'rpm',
            '-q',
            'kernel',
            '--last', # order by install time
        ], stdout=StringIO())
    out = proc.stdout.getvalue()
    version = out.split()[0].split('kernel-')[1]
    log.debug("get_latest_image_version_rpm: %s", version)
    return version


def get_latest_image_version_deb(remote, ostype):
    """
    Get kernel image version of the newest kernel deb package.
    Used for distro case.

    Round-about way to get the newest kernel uname -r compliant version string
    from the virtual package which is the newest kenel for debian/ubuntu.
    """
    output = StringIO()
    newest = ''
    # Depend of virtual package has uname -r output in package name. Grab that.
    if 'debian' in ostype:
        remote.run(args=['sudo', 'apt-get', '-y', 'install',
                         'linux-image-amd64'], stdout=output)
        remote.run(args=['dpkg', '-s', 'linux-image-amd64'], stdout=output)
        for line in output.getvalue().split('\n'):
            if 'Depends:' in line:
                newest = line.split('linux-image-')[1]
                output.close()
                return newest
    # Ubuntu is a depend in a depend.
    if 'ubuntu' in ostype:
        try:
            remote.run(args=['sudo', 'apt-get', '-y', 'install',
                             'linux-image-current-generic'], stdout=output)
            remote.run(args=['dpkg', '-s', 'linux-image-current-generic'],
                       stdout=output)
            for line in output.getvalue().split('\n'):
                if 'Depends:' in line:
                    depends = line.split('Depends: ')[1]
            remote.run(args=['dpkg', '-s', depends], stdout=output)
        except run.CommandFailedError:
            # Non precise ubuntu machines (like trusty) don't have
            # linux-image-current-generic so use linux-image-generic instead.
            remote.run(args=['sudo', 'apt-get', '-y', 'install',
                             'linux-image-generic'], stdout=output)
            remote.run(args=['dpkg', '-s', 'linux-image-generic'],
                       stdout=output)
        for line in output.getvalue().split('\n'):
            if 'Depends:' in line:
                newest = line.split('linux-image-')[1]
                if ',' in newest:
                    newest = newest.split(',')[0]
    output.close()
    return newest


def get_sha1_from_pkg_name(path):
    """
    Get commit hash (min 7 max 40 chars) from (rpm or deb) package name.
    Sample basenames of "make deb-pkg" and "make rpm-pkg" packages
        linux-image-3.10.0-ceph-rhdeb-00050-g687d1a5f0083_3.10.0-ceph-rhdeb-00050-g687d1a5f0083-6_amd64.deb
        kernel-3.10.0_ceph_rhrpm_00050_g687d1a5f0083-8.x86_64.rpm
    Make sure kernel was built with CONFIG_LOCALVERSION_AUTO=y.

    :param path: (rpm or deb) package path (only basename is used)
    """
    basename = os.path.basename(path)
    match = re.search('\d+[-_]g([0-9a-f]{7,40})', basename)
    sha1 = match.group(1) if match else None
    log.debug("get_sha1_from_pkg_name: %s -> %s -> %s", path, basename, sha1)
    return sha1

def task(ctx, config):
    """
    Make sure the specified kernel is installed.
    This can be a branch, tag, or sha1 of ceph-client.git or a local
    kernel package.

    To install ceph-client.git branch (default: master)::

        kernel:
          branch: testing

    To install ceph-client.git tag::

        kernel:
          tag: v3.18

    To install ceph-client.git sha1::

        kernel:
          sha1: 275dd19ea4e84c34f985ba097f9cddb539f54a50

    To install from a koji build_id::

        kernel:
          koji: 416058

    When installing from koji you also need to set the urls for koji hub
    and the koji root in your teuthology.yaml config file. These are shown
    below with their default values::

        kojihub_url: http://koji.fedoraproject.org/kojihub
        kojiroot_url: http://kojipkgs.fedoraproject.org/packages

    To install local rpm (target should be an rpm system)::

        kernel:
          rpm: /path/to/appropriately-named.rpm

    To install local deb (target should be a deb system)::

        kernel:
          deb: /path/to/appropriately-named.deb

    For rpm: or deb: to work it should be able to figure out sha1 from
    local kernel package basename, see get_sha1_from_pkg_name().  This
    means that you can't for example install a local tag - package built
    with upstream {rpm,deb}-pkg targets won't have a sha1 in its name.

    If you want to schedule a run and use a local kernel package, you
    have to copy the package over to a box teuthology workers are
    running on and specify a path to the package on that box.

    All of the above will install a specified kernel on all targets.
    You can specify different kernels for each role or for all roles of
    a certain type (more specific roles override less specific, see
    normalize_config() for details)::

        kernel:
          client:
            tag: v3.0
          osd:
            branch: btrfs_fixes
          client.1:
            branch: more_specific
          osd.3:
            branch: master

    To wait 3 minutes for hosts to reboot (default: 300)::

        kernel:
          timeout: 180

    To enable kdb::

        kernel:
          kdb: true

    :param ctx: Context
    :param config: Configuration
    """
    assert config is None or isinstance(config, dict), \
        "task kernel only supports a dictionary for configuration"

    timeout = 300
    if config is not None and 'timeout' in config:
        timeout = config.pop('timeout')

    config = normalize_config(ctx, config)
    validate_config(ctx, config)
    log.info('config %s' % config)

    need_install = {}  # sha1 to dl, or path to rpm or deb
    need_version = {}  # utsrelease or sha1
    kdb = {}
    for role, role_config in config.iteritems():
        # gather information about this remote
        (role_remote,) = ctx.cluster.only(role).remotes.keys()
        system_type, system_ver = role_remote.os.name, role_remote.os.version
        if role_config.get('rpm') or role_config.get('deb'):
            # We only care about path - deb: vs rpm: is meaningless,
            # rpm: just happens to be parsed first.  Nothing is stopping
            # 'deb: /path/to/foo.rpm' and it will work provided remote's
            # os.package_type is 'rpm' and vice versa.
            path = role_config.get('rpm')
            if not path:
                path = role_config.get('deb')
            sha1 = get_sha1_from_pkg_name(path)
            assert sha1, "failed to extract commit hash from path %s" % path
            if need_to_install(ctx, role, sha1):
                need_install[role] = path
                need_version[role] = sha1
        elif role_config.get('sha1') == 'distro':
            if need_to_install_distro(ctx, role):
                need_install[role] = 'distro'
                need_version[role] = 'distro'
        elif role_config.get("koji", None):
            # installing a kernel from koji
            build_id = role_config.get("koji")
            if role_remote.os.package_type != "rpm":
                msg = (
                    "Installing a kernel from koji is only supported "
                    "on rpm based systems. System type is {system_type}."
                )
                msg = msg.format(system_type=system_type)
                log.error(msg)
                ctx.summary["failure_reason"] = msg
                ctx.summary["status"] = "dead"
                raise ConfigError(msg)

            # FIXME: this install should probably happen somewhere else
            # but I'm not sure where, so we'll leave it here for now.
            install_package('koji', role_remote)

            # get information about this build from koji
            build_info = get_koji_build_info(build_id, role_remote, ctx)
            version = "{ver}-{rel}.x86_64".format(
                ver=build_info["version"],
                rel=build_info["release"]
            )

            if need_to_install(ctx, role, version):
                need_install[role] = build_info
                need_version[role] = version
        else:
            package_type = role_remote.os.package_type
            larch = role_remote.arch
            if package_type == 'rpm':
                if '.' in system_ver:
                    system_ver = system_ver.split('.')[0]
                ldist = '{system_type}{system_ver}'.format(system_type=system_type, system_ver=system_ver)
            if package_type == 'deb':
                system_ver = role_remote.os.codename
                ldist = '{system_ver}'.format(system_ver=system_ver)
            sha1, base_url = teuthology.get_ceph_binary_url(
                package='kernel',
                branch=role_config.get('branch'),
                tag=role_config.get('tag'),
                sha1=role_config.get('sha1'),
                flavor='basic',
                format=package_type,
                dist=ldist,
                arch=larch,
                )
            log.debug('sha1 for {role} is {sha1}'.format(role=role, sha1=sha1))
            ctx.summary['{role}-kernel-sha1'.format(role=role)] = sha1

            if need_to_install(ctx, role, sha1):
                version = sha1
                version_url = urlparse.urljoin(base_url, 'version')
                try:
                    version_fp = urllib2.urlopen(version_url)
                    version = version_fp.read().rstrip('\n')
                    version_fp.close()
                except urllib2.HTTPError:
                    log.debug('failed to get utsrelease string using url {url}'.format(
                              url=version_url))

                if not version:
                    raise VersionNotFoundError("{url} is empty!".format(
                        url=version_url))

                need_install[role] = sha1
                need_version[role] = version

        # enable or disable kdb if specified, otherwise do not touch
        if role_config.get('kdb') is not None:
            kdb[role] = role_config.get('kdb')

    if need_install:
        install_firmware(ctx, need_install)
        download_kernel(ctx, need_install)
        install_and_reboot(ctx, need_install)
        wait_for_reboot(ctx, need_version, timeout)

    enable_disable_kdb(ctx, kdb)
