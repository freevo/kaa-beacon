# -*- coding: iso-8859-1 -*-
# -----------------------------------------------------------------------------
# utils.py - Some utils for the server
# -----------------------------------------------------------------------------
# $Id$
#
# -----------------------------------------------------------------------------
# kaa.beacon.server - A virtual filesystem with metadata
# Copyright (C) 2007 Dirk Meyer
#
# First Edition: Dirk Meyer <https://github.com/Dischi>
# Maintainer:    Dirk Meyer <https://github.com/Dischi>
#
# Please see the file AUTHORS for a complete list of authors.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of MER-
# CHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
# Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
#
# -----------------------------------------------------------------------------

__all__ = [ 'BurstHandler', 'do_thumbnail' ]

#python imports
from collections import namedtuple
import ctypes, ctypes.util
import os
import struct

# kaa imports
import kaa

class BurstHandler(object):
    """
    Monitor growing files.
    """

    _all_instances = []

    def __init__(self, interval, callback):
        self._ts = {}
        self._thumb = {}
        self._timer = kaa.WeakTimer(self._poll)
        self._timer.start(interval)
        self._callback = callback
        self._all_instances.append(self)


    def remove(self, name):
        """
        Remove a file from the list of growing files.
        """
        if name in self._ts:
            del self._ts[name]
        if name in self._thumb:
            del self._thumb[name]


    def is_growing(self, name):
        """
        Return True if the file is growing. Detection is based on the
        frequency this function is called.
        """
        if not name in self._ts:
            self._ts[name] = False
            return False
        self._ts[name] = True
        return True


    def _do_thumbnail(self, name):
        """
        Check if a thumbnail should be created.
        """
        if not name in self._ts:
            # not in the list of growing files
            return True
        if not name in self._thumb:
            self._thumb[name] = 0
            # first time is always ok
            return True
        self._thumb[name] += 1
        return (self._thumb[name] % 10) == 0


    def _poll(self):
        """
        Run callback on all growing files.
        """
        ts = self._ts
        self._ts = {}
        for name in [ name for name, needed in ts.items() if needed ]:
            self._callback(name)


def do_thumbnail(name):
    """
    Global function to check if a thumbnail should be created.
    """
    for i in BurstHandler._all_instances:
        if i._do_thumbnail(name):
            return True
    return False


statfs_result = namedtuple('statfs_result', 'f_type, f_bsize, f_blocks, f_bfree, f_bavail,'
                                            'f_files, f_ffree, f_fsid, f_namelen, f_frsize')
def statfs(path):
    """
    A python implementation of statfs(2).

    :param path: mount point of the filesystem to statfs
    :returns: 10-tuple: filesystem type, optimal transfer block size, total
              data blocks, blocks free, blocks available to non-root, used file
              nodes, free file nodes, file system id, max filename length.

    filesystem type is resolved into a string if it is known (e.g. 'nfs' or
    'ext3'), otherwise it is the integer value as returned by statfs(2).
    """
    if not hasattr(statfs, 'libc'):
        # Store libc, so we only do this once.
        statfs.libc = ctypes.CDLL(ctypes.util.find_library('c'), use_errno=True)

    # long fs_type, f_bsize; int64_t f_blocks, f_bfree, f_bavail, f_files, f_ffree,
    # f_fsid; long f_namelen, f_frsize, f_space[5]
    # TODO: is this structure the same on other platforms or Linux only?
    fmt = '@2L6Q7L'
    buf = ctypes.create_string_buffer(struct.calcsize(fmt))
    ret = statfs.libc.statfs64(path, buf)
    if ret != 0:
        errno = ctypes.get_errno()
        raise OSError(errno, "%s: '%s'" % (os.strerror(errno), path))

    result = struct.unpack(fmt, buf)[:10]
    fstypes = { 
        0xadf5: 'adfs',
        0xadff: 'affs',
        0x5346414F: 'afs',
        0x0187: 'autofs',
        0x73757245: 'coda',
        0x28cd3d45: 'cramfs',
        0x453dcd28: 'cramfs',
        0x64626720: 'debugfs',
        0x62656572: 'sysfs',
        0x73636673: 'securityfs',
        0x858458f6: 'ramfs',
        0x01021994: 'tmpfs',
        0x958458f6: 'hugetblfs',
        0x73717368: 'squashfs',
        0x414A53: 'efs',
        0xEF53: 'ext2/ext3',
        0xabba1974: 'xenfs',
        0x9123683E: 'btrfs',
        0xf995e849: 'hpfs',
        0x9660: 'isofs',
        0x4004: 'isofs',
        0x4000: 'isofs',
        0x07C0: 'jffs',
        0x72b6: 'jffs2',
        0x4d44: 'msdos',
        0x58465342: 'xfs',
        0x6969: 'nfs',
        0x6E667364: 'nfsd',
        0x15013346: 'udf',
        0x00011954: 'ufs',
        0x54190100: 'ufs',
        0x9FA2: 'usbdevfs',
        0x9fa0: 'procfs',
        0x002f: 'qnx4',
        0x52654973: 'reiserfs',
        0x517B: 'smbfs',
        0x9fa2: 'usbfs',
        0xBAD1DEA: 'futexfs',
        0x2BAD1DEA: 'inotifyfs',
        0x1cd1: 'devpts',
        0x534F434B: 'sockfs',
        0xabababab: 'vmblock',
        0x65735543: 'fusectl',
        0x42494e4d: 'binfmt_misc',
    }
    return statfs_result(fstypes.get(result[0], result[0]), *result[1:])
