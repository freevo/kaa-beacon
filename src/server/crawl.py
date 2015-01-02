# -*- coding: iso-8859-1 -*-
# -----------------------------------------------------------------------------
# crawl.py - Crawl filesystem and monitor it
# -----------------------------------------------------------------------------
# $Id$
#
# -----------------------------------------------------------------------------
# kaa.beacon.server - A virtual filesystem with metadata
# Copyright (C) 2006-2009 Dirk Meyer
#
# First Edition: Dirk Meyer <dischi@freevo.org>
# Maintainer:    Dirk Meyer <dischi@freevo.org>
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

# python imports
import os
import time
import logging

# kaa imports
import kaa
from kaa.inotify import INotify

# kaa.beacon imports
from parser import parse
from config import config
import scheduler
import utils
import time

# get logging object
log = logging.getLogger('beacon.crawler')

try:
    WATCH_MASK = INotify.MODIFY | INotify.CLOSE_WRITE | INotify.DELETE | \
                 INotify.CREATE | INotify.DELETE_SELF | INotify.UNMOUNT | \
                 INotify.MOVE
except:
    WATCH_MASK = None


class MonitorList(dict):

    def __init__(self, inotify):
        dict.__init__(self)
        self._inotify = inotify
        # A list of directories on NFS or CIFS.  This list only contains the
        # top-most NFS/CIFS directory so it should not become very large.  For
        # example, if /mnt/filer/ is an NFS mount we're monitoring, and it
        # contains subdirs foo/ and bar/, only /mnt/filer/ would be in this
        # list.
        self.nfs_items = []

    def add(self, dirname, item, use_inotify=True):
        if self._inotify and use_inotify:
            log.debug('Adding INotify watch for %s' % dirname)
            try:
                self._inotify.watch(dirname, WATCH_MASK)
                self[dirname] = True
            except IOError, e:
                log.error(e)

            # Is this dir on a network filesystem?
            if not any(1 for i in self.nfs_items if dirname.startswith(i.filename)):
                # Parent isn't already in rescan list, so check to see if this dir is NFS/CIFS.
                if utils.statfs(dirname).f_type in ('nfs', 'smbfs'):
                    self.nfs_items.append(item)
        else:
            self[dirname] = False


    def remove(self, dirname):
        """
        Removes the given directory name and all directories under it from
        monitoring.

        This is O(1) with respect to the size of the current monitor list.
        """
        for d in self.keys():
            if d.startswith(dirname) and self.pop(d):
                log.debug('Removing INotify watch for %s', dirname)
                self._inotify.ignore(dirname)

        # Remove any NFS/CIFS items at or under this path.
        self.nfs_items = [i for i in self.nfs_items if not i.filename.startswith(dirname)]



class Crawler(object):
    """
    Class to crawl through a filesystem and check for changes. If inotify
    support is enabled in the kernel, this class will use it to avoid
    polling the filesystem.
    """
    # Number of active Crawler instances, one per monitored filesystem.  A
    # Crawler is active when it is currently traversing the filesystem tree.
    # If INotify is available, a Crawler is not considered active after its
    # initial traversal.  If INotify is not available, the Crawler is more
    # or less always active (except for the 10 second moratorium after
    # crawling is finished).
    active = 0
    # The id of the last Crawler instance.
    lastid = 0

    def __init__(self, db, use_inotify=True, monitor=True):
        """
        Init the Crawler.
        Parameter db is a beacon.db.Database object.
        """
        self._db = db
        self.num = Crawler.lastid = Crawler.lastid + 1

        # set up inotify
        self._inotify = None
        if use_inotify and monitor:
            try:
                self._inotify = INotify()
                self._inotify.signals['event'].connect(self._inotify_event)
            except SystemError, e:
                log.warning('%s', e)

        # Set up the burst handler to simulate a MODIFY INotify event.
        cb = kaa.WeakCallable(self._inotify_event, INotify.MODIFY)
        cb.init_args_first = True
        self._bursthandler = utils.BurstHandler(config.scheduler.growscan, cb)

        # List of directories we are interested in monitoring (either with
        # INotify or by polling).
        self.monitors = MonitorList(self._inotify)

        # root items that are 'appended'
        self._root_items = []

        kaa.main.signals["shutdown"].connect_weak(self.stop)

        # create internal scan variables
        self._scan_list = []
        self._scan_dict = {}
        # CoroutineInProgress for self._scanner
        self._coroutine = None
        if monitor:
            self._scan_restart_timer = kaa.WeakOneShotTimer(self._scan_restart)
        else:
            self._scan_restart_timer = None
        self._crawl_start_time = None


    def append(self, item):
        """
        Append a directory to be crawled and monitored.
        """
        throttle = not self._scan_restart_timer
        log.info('crawler %d: added %s to list (throttle=%s)', self.num, item, throttle)
        self._root_items.append(item)
        self._scan_add(item, recursive=True, force_thumbnail_check=True, throttle=throttle)


    def stop(self):
        """
        Stop the crawler and remove the inotify watching.
        """
        kaa.main.signals["shutdown"].disconnect(self.stop)
        # stop running scan process
        del self._scan_list[:]
        self._scan_dict.clear()
        if self._coroutine:
            self._coroutine.abort()
            self._coroutine = None
        # stop inotify
        self._inotify = None
        # stop restart timer
        if self._scan_restart_timer:
            self._scan_restart_timer.stop()


    def __repr__(self):
        return '<kaa.beacon.Crawler id=%d>' % self.num


    # -------------------------------------------------------------------------
    # Internal functions - INotify
    # -------------------------------------------------------------------------

    def _inotify_event(self, mask, name, target=None):
        """
        Callback for inotify.
        """
        if mask & INotify.MODIFY and self._bursthandler.is_growing(name):
            # A file was modified. Do this check as fast as we can because the
            # events may come in bursts when a file is just copied. In this case
            # a timer is already active and we can return. It still uses too
            # much CPU time in the burst, but there is nothing we can do about
            # it.
            return True

        if self._db.read_lock.locked:
            # The database is locked now and we may want to change entries.
            # When the db becomes unlocked, INotify events will be replayed in
            # the same order because kaa.Signal will emit callbacks in the
            # order they were connected.
            kaa.inprogress(self._db.read_lock).connect_once(self._inotify_event, mask, name, target)
            return True

        # some debugging to find a bug in beacon
        log.info('crawler %d: inotify event %s for "%s" (target=%s)', self.num,
                 INotify.mask_to_string(mask), name, target)

        item = self._db.query_filename(name)
        if item._beacon_parent.filename not in self.monitors:
            # that is a different monitor, ignore it
            # FIXME: this is a bug (missing feature) in inotify
            return True

        if item._beacon_name.startswith('.'):
            # hidden file, ignore except in move operations
            if mask & INotify.MOVE and target:
                # we moved from a hidden file to a good one. So handle
                # this as a create for the new one.
                log.info('crawler %d: inotify: handle move as create for %s', self.num, target)
                self._inotify_event(INotify.CREATE, target)
            return True

        # ---------------------------------------------------------------------
        # MOVE_FROM -> MOVE_TO
        # ---------------------------------------------------------------------

        if mask & INotify.MOVE and target and item._beacon_id:
            # Move information with source and destination
            move = self._db.query_filename(target)
            if move._beacon_name.startswith('.'):
                # move to hidden file, delete
                log.info('crawler %d: inotify: move to hidden file, delete', self.num)
                self._inotify_event(INotify.DELETE, name)
                return True

            if move._beacon_id:
                # New item already in the db, delete it first
                log.info('crawler %d: inotify: delete %s', self.num, item)
                self._db.delete_object(move)
            changes = {}
            if item._beacon_parent._beacon_id != move._beacon_parent._beacon_id:
                # Different directory, set new parent
                changes['parent'] = move._beacon_parent._beacon_id
            if item._beacon_data['name'] != move._beacon_data['name']:
                # New name, set name to item
                move._beacon_data = dict(move._beacon_data)
                if move._beacon_data.get('image') == move._beacon_data['name']:
                    # update image to new filename
                    changes['image'] = move._beacon_data['name']
                changes['name'] = move._beacon_data['name']
            if changes:
                log.info('crawler %d: inotify: move: %s', self.num, changes)
                self._db.update_object(item._beacon_id, **changes)

            # Now both directories need to be checked again
            # FIXME: instead of creating new thumbnails here, we should rename the
            # existing thumbnails from the files in the directory and adjust the
            # metadata in it.
            self._scan_add(item._beacon_parent, recursive=False)
            self._scan_add(move._beacon_parent, recursive=True)

            if not mask & INotify.ISDIR:
                # commit changes so that the client may get notified
                self._db.commit()
                return True

            # The directory is a dir. We now remove all the monitors to that
            # directory and crawl it again. This keeps track for softlinks that
            # may be different or broken now.
            self.monitors.remove(name + '/')
            # now make sure the directory is parsed recursive again
            self._scan_add(move, recursive=True)
            # commit changes so that the client may get notified
            self._db.commit()
            return True

        # ---------------------------------------------------------------------
        # MOVE_TO, CREATE, MODIFY or CLOSE_WRITE
        # ---------------------------------------------------------------------

        if mask & INotify.MOVE and target:
            # We have a move with to and from, but the from item is not in
            # the db. So we handle it as a simple MOVE_TO
            name = target

        if os.path.exists(name):
            # The file or directory exists.  So it is either created or modified.
            if item._beacon_isdir:
                # It is a directory. Just do a full directory rescan.
                recursive = not (mask & INotify.MODIFY)
                self._scan_add(item, recursive)
                if name.lower().endswith('/video_ts'):
                    # it could be a dvd on hd
                    self._scan_add(item._beacon_parent)
                return True

            # Modified item is a file.
            # handle bursts of inotify events when a file is growing very
            # fast (e.g. cp)
            if mask & INotify.CLOSE_WRITE:
                self._bursthandler.remove(name)

            # parent directory changed, too. Even for a simple modify of an
            # item another item may be affected (xml metadata, images)
            # so scan the file by rechecking the parent dir
            self._scan_add(item._beacon_parent)
            return True

        # ---------------------------------------------------------------------
        # DELETE
        # ---------------------------------------------------------------------

        # before we delete, maybe the filesystem was just umounted
        if mask & INotify.UNMOUNT:
            # Oops, our filesystem was umounted. This should never happen
            # since all removable drives which could be umounted are on
            # a different media in beacon. It happens sometimes on system
            # shutdown, so we just ignore this event for now.
            if name + '/' in self.monitors:
                self.monitors.remove(name + '/')
            return True

        # The file does not exist, we need to delete it in the database
        log.info('crawler %d: inotify: delete %s', self.num, item)
        self._db.delete_object(item)

        # remove directory and all subdirs from the inotify. The directory
        # is gone, so all subdirs are invalid, too.
        if name + '/' in self.monitors:
            # FIXME: This is not correct when you deal with softlinks.
            # If you move a directory with relative softlinks, the new
            # directory to monitor is different.
            self.monitors.remove(name + '/')
        # rescan parent directory
        self._scan_add(item._beacon_parent)
        # commit changes so that the client may get notified
        self._db.commit()
        return True


    # -------------------------------------------------------------------------
    # Internal functions - Scanner
    # -------------------------------------------------------------------------

    def _scan_add(self, directory, recursive=False, throttle=False, force_thumbnail_check=False, force_scan=False):
        """
        Add a directory to the list of directories to scan, and start
        the scanner coroutine if it's not already running.

        If throttle is True, _scanner() will increase the sleep time before
        processing the given directory, over and above what the scheduler
        advises.  This is used for periodic background rescanning for NFS/CIFS
        directories or when INotify is not available.

        If force_scan is True, we scan the given directory even if it's already
        in the active monitor list.  This is used for NFS/CIFS directories.
        """
        if directory.filename in self._scan_dict:
            # ok then, already in list and close to the beginning
            # if we are called bu inotify (because it has to be scanned
            # once) or somewhere else in normal mode. In both cases we
            # don't do anything.
            return False

        if not recursive:
            # called from inotify. this means the file can not be in
            # the list as recursive only again from inotify. Add to the
            # beginning of the list, it is important and fast.
            self._scan_list.insert(0, (directory, False, throttle, force_thumbnail_check, force_scan))
        else:
            # called from inside the crawler recursive or by massive changes
            # from inotify. In both cases, add to the end of the list because
            # this takes much time.
            if not force_scan and directory.filename in self.monitors:
                # already scanned and being monitored
                # TODO: softlink dirs are not handled correctly, they may be
                # scanned twiece.
                return False
            self._scan_list.append((directory, True, throttle, force_thumbnail_check, force_scan))
        self._scan_dict[directory.filename] = directory

        # start scanning
        if not self._coroutine or self._coroutine.finished:
            self._coroutine = self._scanner()
            self._coroutine.signals['abort'].connect_weak(self._scan_completed)


    @kaa.coroutine()
    def _scanner(self):
        Crawler.active += 1
        log.info('crawler %d: starting directory scan', self.num)
        # remember start time for debugging output
        self._crawl_start_time = time.time()

        while self._scan_list:
            interval = scheduler.next(config.scheduler.policy) * config.scheduler.multiplier
            # get next item to scan and start the scanning
            directory, recursive, throttle, force_thumbnail_check, force_scan = self._scan_list.pop(0)
            if throttle:
                # Directory rescanning when INotify is not available.  This is
                # an idle task, so slow it down.
                interval *= 5
            del self._scan_dict[directory.filename]

            ip = self._scan(directory, force_thumbnail_check)
            if ip.finished:
                # Already done.
                yield kaa.delay(interval) if interval else kaa.NotFinished

            subdirs = yield ip
            if recursive:
                # add results to the list of files to scan
                for d in subdirs:
                    self._scan_add(d, True, throttle, force_thumbnail_check, force_scan)

        self._scan_completed(aborted=False)

        if (not self._inotify or (self.monitors.nfs_items and config.scheduler.nfsrescan)) and self._scan_restart_timer:
            # We need to schedule a rescan either because INotify is not in use or because we
            # have NFS directories that need to be polled.  Start crawling again in 10 seconds.
            # During a rescan, the scanner will slow down even beyond what the scheduler
            # dictates.
            log.debug('crawler %d: scheduling a rescan', self.num)
            self._scan_restart_timer.start(10)


    def _scan_completed(self, aborted=True):
        """
        Called when the scanner is either completed successfully or aborted
        (such as when stop() is called).
        """
        # crawler finished
        duration = time.time() - self._crawl_start_time
        log.info('crawler %d: %s; took %0.1f seconds.', self.num, 'aborted' if aborted else 'finished', duration)
        self._crawl_start_time = None
        Crawler.active -= 1
        # mark current time as complete scan time for all items
        for item in self._root_items:
            item['last_crawl'] = int(time.time())
        # commit changes so that the client may get notified
        self._db.commit()


    def _scan_restart(self):
        """
        Restart the crawler when inotify is not enabled.
        """
        if not self._inotify:
            # reset self.monitors and add all directories once passed to
            # this object with 'append' again.
            self.monitors = MonitorList(self._inotify)
            for item in self._root_items:
                self._scan_add(item, recursive=True, throttle=True)
        elif self.monitors.nfs_items:
            # Force rescan NFS/CIFS directories.  These are already being
            # monitored with INotify so we don't clear the MonitorList as with
            # the non-INotify case above.
            for item in self.monitors.nfs_items[:]:
                self._scan_add(item, recursive=True, throttle=True, force_scan=True)


    @kaa.coroutine()
    def _scan(self, directory, force_thumbnail_check):
        """
        Scan a directory and all files in it, return list of subdirs.
        """
        log.info('crawler %d: scan directory %s (force thumbnails: %s)', self.num,
                 directory.filename, force_thumbnail_check)

        if not os.path.exists(directory.filename):
            log.warning('crawler %d: %s does not exist; skipping scan.', self.num, directory.filename)
            yield []

        if directory._beacon_parent and not directory._beacon_parent._beacon_isdir:
            log.warning('crawler %d: parent of %s is not a directory', self.num, directory)
            if hasattr(directory, 'filename') and directory.filename + '/' in self.monitors:
                self.monitors.remove(directory.filename + '/')
            yield []

        # parse directory
        async = parse(self._db, directory, force_thumbnail_check=force_thumbnail_check)
        if isinstance(async, kaa.InProgress):
            yield async

        # Having parsed the directory, check to see if the item is still flagged
        # as a directory.  This happens, for example, with a DVD directory tree.
        if not directory._beacon_isdir:
            if directory.get('scheme') != 'dvd':
                # Only warn if this isn't a DVD tree.
                log.warning('crawler %d: %s turned out not to be a directory after parsing', self.num, directory)
            if hasattr(directory, 'filename') and directory.filename + '/' in self.monitors:
                self.monitors.remove(directory.filename + '/')
            yield []

        if directory._beacon_islink:
            # it is a softlink. Add directory with inotify to the monitor
            # list and with inotify using the realpath (later)
            self.monitors.add(directory.filename, directory, use_inotify=False)
            dirname = os.path.realpath(directory.filename)
            directory = self._db.query_filename(dirname)
            async = parse(self._db, directory, force_thumbnail_check=force_thumbnail_check)
            if isinstance(async, kaa.InProgress):
                yield async

        # add to monitor list using inotify
        if directory.filename not in self.monitors:
            self.monitors.add(directory.filename, directory)

        # iterate through the files
        subdirs = []
        garbage = []
        counter = 0

        # check if we should crawl deeper
        recursive = not os.path.exists(os.path.join(directory.filename, '.beacon-no-crawl'))
        for child in (yield self._db.query(parent=directory, garbage=garbage)):
            if child._beacon_isdir:
                if child.scanned and not recursive:
                    # FIXME: it would be nice to activate inotify
                    log.info('crawler %d: skip crawling/monitoring %s', self.num, child.filename)
                else:
                    # add directory to list of files to return
                    subdirs.append(child)
                continue

            # check file
            async = parse(self._db, child, force_thumbnail_check=force_thumbnail_check)
            if isinstance(async, kaa.InProgress):
                async = yield async

            delay = scheduler.next(config.scheduler.policy) * config.scheduler.multiplier
            if delay:
                yield kaa.delay(delay)

        # If any dir objects were implicitly removed during query() above, then
        # we should remove any existing INotify watch.  This can happen when
        # nfsrescan=True and directory removal was not observed by INotify.
        for child in garbage:
            if child._beacon_isdir:
                self.monitors.remove(child.filename)

        if not subdirs:
            # No subdirectories that need to be checked. Add some extra
            # attributes based on the found items (recursive back to parents)
            yield self._add_directory_attributes(directory)
        yield subdirs


    @kaa.coroutine()
    def _add_directory_attributes(self, directory):
        """
        Add some extra attributes for a directory recursive. This function
        checkes album, artist, image and length. When there are changes,
        go up to the parent and check it, too.
        """
        data = { 'length': 0, 'artist': u'', 'album': u'', 'image': '', 'series': '', 'season': '' }
        check_attr = data.keys()[:]
        check_attr.remove('length')

        items = { 'video': [], 'audio': [], 'image': [], 'dir': [], 'other': [] }
        for item in (yield self._db.query(parent=directory)):
            t = item._beacon_data.get('type')
            if t in items:
                items[t].append(item)
            else:
                items['other'].append(item)
        relevant = []
        if (not items['video'] and not items['other'] and not items['dir']) and \
           ((len(items['audio']) > 2, len(items['image']) <= 1) or \
            (len(items['audio']) > 8, len(items['image']) <= 3)):
            # Could be music files. Only music files plus maybe
            # folder/cover images
            relevant = items['audio']
        if (not items['audio'] and not items['other'] and not items['dir']) and \
           (len(items['video']) > 2, len(items['image']) <= 1):
            # Could be video files. Only video files plus maybe one
            # folder/cover image
            relevant = items['video']
        if not items['audio'] and not items['video'] and not items['other'] and \
           items['dir'] and len(items['image']) <= 1:
            # only directories with maybe one folder/cover image
            relevant = items['dir']
        for item in relevant:
            data['length'] += item._beacon_data.get('length', 0) or 0
            for attr in check_attr:
                value = item._beacon_data.get(attr, data[attr])
                if data[attr] == '':
                    data[attr] = value
                if data[attr] != value:
                    data[attr] = None
                    check_attr.remove(attr)

        if data['image'] and not (data['artist'] or data['album']):
            # We have neither artist nor album. So this seems to be a video
            # or an image directory and we don't want to set the image from
            # maybe one item in that directory as our directory image.
            data['image'] = None
        data = dict([ x for x in data.items() if x[1] ])
        for attr in data.keys():
            if data[attr] != directory._beacon_data[attr]:
                break
        else:
            # no changes.
            yield True

        yield kaa.inprogress(self._db.read_lock)

        # update directory in database
        self._db.update_object(directory._beacon_id, **data)
        directory._beacon_data.update(data)

        # check parent
        if directory._beacon_parent.filename in self.monitors:
            yield self._add_directory_attributes(directory._beacon_parent)
