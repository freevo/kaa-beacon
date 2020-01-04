# -*- coding: iso-8859-1 -*-
# -----------------------------------------------------------------------------
# udisks.py - Disk montor
# -----------------------------------------------------------------------------
# kaa.beacon.server - A virtual filesystem with metadata
# Copyright (C) 2012 Dirk Meyer
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

# python imports
import logging
import os
import stat
import dbus

# kaa imports
import kaa
import kaa.metadata

# kaa.beacon imports
from ..utils import get_title

# get logging object
log = logging.getLogger('beacon.udisk')

# Set dbus to use gtk and adjust kaa to it.
from dbus.mainloop.glib import DBusGMainLoop
DBusGMainLoop(set_as_default=True)
kaa.gobject_set_threaded()

OBJ_DEVICE = 'org.freedesktop.UDisks.Device'
OBJ_UDISKS = 'org.freedesktop.UDisks'

class UDisks(object):
    """
    Hardware monitor based on UDisks (dbus)
    """
    def __init__(self, handler, db, rootfs):
        log.info('start hardware monitor')
        self._db = db
        # handler == beacon.server.Controller
        self.handler = handler
        self.partition_tables = []
        self.detected_devices = {}
        self._gobject_init()
        self._device_update_media(rootfs)

    @kaa.threaded(kaa.GOBJECT)
    def _gobject_init(self):
        """
        Connect to dbus and start to connect to UDisks.
        """
        self.bus = dbus.SystemBus()
        self.proxy = self.bus.get_object("org.freedesktop.UDisks", "/org/freedesktop/UDisks")
        self.iface = dbus.Interface(self.proxy, "org.freedesktop.UDisks")
        # #####################################################################
        # The code is broken
        # #####################################################################
        # self.iface.connect_to_signal('DeviceAdded', self._device_udisks_update)
        # self.iface.connect_to_signal('DeviceRemoved', self._device_remove)
        # self.iface.connect_to_signal('DeviceChanged', self._device_udisks_update)
        # for dev in self.iface.EnumerateDevices():
        #     self._device_udisks_update(dev)

    @kaa.threaded(kaa.GOBJECT)
    def mount(self, devdict):
        """
        Mount the device.
        """
        def success(*args):
            pass
        def error(error):
            log.error('unable to mount %s: %s', devdict['block.device'], error)
        path = self.iface.FindDeviceByDeviceFile(devdict['block.device'])
        dbus = self.bus.get_object(OBJ_UDISKS, path)
        dbus.FilesystemMount('', ['auth_no_user_interaction'],
                 reply_handler=success, error_handler=error)

    @kaa.threaded(kaa.GOBJECT)
    def eject(self, devdict):
        """
        Eject the device. This includes umounting and removing from
        the list. Devices that can't be ejected (USB sticks) are only
        umounted and removed from the list.
        """
        def success(*args):
            pass
        def error(error):
            log.error('unable to umount %s: %s', devdict['block.device'], error)
        path = self.iface.FindDeviceByDeviceFile(devdict['block.device'])
        dbus = self.bus.get_object(OBJ_UDISKS, path)
        if path in self.detected_devices:
            del self.detected_devices[path]
        if devdict.get('volume.is_disc'):
            dbus.DriveEject(['unmount'], reply_handler=success, error_handler=error)
        if devdict.get('volume.mount_point'):
            dbus.FilesystemUnmount(['force'], reply_handler=success, error_handler=error)

    @kaa.threaded(kaa.MAINTHREAD)
    def _device_remove(self, path):
        """
        Remove a device from the list. This function is called by
        UDisks when a device is removed (e.g. USB sicks) or by
        _device_udisks_update when an optical drive lost its media.
        """
        devdict = self.detected_devices.get(path)
        if not devdict:
            for devdict in self.detected_devices.values()[:]:
                if devdict.get('block.parent') == path:
                    self._device_remove(devdict.get('block.path'))
            return
        beacon_id = devdict.get('beacon.id')
        media = self._db.medialist.get_by_media_id(beacon_id)
        if media is not None:
            log.info('remove device %s' % beacon_id)
            self.handler.media_removed(media)
            self._db.medialist.remove(beacon_id)

    def _device_udisks_update(self, path):
        """
        Gather information about the device described by the UDisks
        path. Note: path is NOT the device path in the filesystem; it
        is the UDisks device path.
        """
        try:
            device_obj = self.bus.get_object(OBJ_UDISKS, path)
            device_props = dbus.Interface(device_obj, dbus.PROPERTIES_IFACE)
            DeviceIsMediaAvailable = device_props.Get('org.freedesktop.UDisks.Device', "DeviceIsMediaAvailable")
            DeviceIsRemovable = device_props.Get('org.freedesktop.UDisks.Device', "DeviceIsRemovable")
            DeviceFile = device_props.Get('org.freedesktop.UDisks.Device', "DeviceFile")
            IdUuid = device_props.Get('org.freedesktop.UDisks.Device', "IdUuid")
            IdLabel = device_props.Get('org.freedesktop.UDisks.Device', "IdLabel")
        except Exception, e:
            log.error('unable to get dbus information: %s', e)
            return False
        # Try to figure out if the device is a removable drive beacon
        # can mount or a media player can play. Right now it can
        # detect optical media and USB sticks. A disc connected via
        # USB may not get detected. For historical reasons the
        # information is stored in a dict with names used by HAL. This
        # should be changed in the future. Other media such as SD
        # cards are not supported yet. Feel free to send patches.
        if not DeviceIsMediaAvailable and path in self.detected_devices:
            # The media was available before and is not anymore. This
            # happens for optical drives where the drive itself is not
            # removed, only the media.
            self._device_remove(path)
            return True
        if device_props.Get('org.freedesktop.UDisks.Device', "DeviceIsOpticalDisc"):
            # Disc in an optical drive
            devdict = {
                'beacon.id': str(IdUuid) or kaa.metadata.cdrom.status(DeviceFile)[1],
                'volume.mount_point': '',
                'volume.is_disc': True,
                'volume.label': str(IdLabel),
                'volume.read_only': True,
                'block.device': str(DeviceFile)
            }
            self.detected_devices[str(path)] = devdict
            return kaa.MainThreadCallable(self._device_update_media)(devdict)
        if DeviceIsRemovable and device_props.Get('org.freedesktop.UDisks.Device', 'DeviceIsPartitionTable'):
            # UDisks reports USB sticks with a filesystem as two
            # different things. The actual device (/dev/sdX) is
            # removable, but of course we cannot mount it. But we have
            # to remember that information because the partition
            # (/dev/sdX1) is not reported as removable.
            self.partition_tables.append(path)
            return True
        is_filesystem = False
        if device_props.Get('org.freedesktop.UDisks.Device', 'DeviceIsPartition'):
            # Device is a partition. Check if it is on a removable
            # device. If not, do not cover it.
            for parent in self.partition_tables:
                if path.startswith(parent):
                    break
            else:
                return True
            is_filesystem = True
        if DeviceIsRemovable and DeviceIsMediaAvailable:
            # USB stick without partition table. We should be able to
            # mount /dev/sdX directly. Sadly UDisks does not give away
            # the information what kind of filesystem there is to be
            # sure it is a mountable partition.
            is_filesystem = True
            parent = None
        if not is_filesystem:
            return True
        # Either partition or whole device with a filesystem on it we
        # should be able to mount.
        devdict = {
            'beacon.id': str(IdUuid),
            'volume.mount_point': '',
            'volume.is_disc': False,
            'volume.label': str(IdLabel),
            'volume.read_only': bool(device_props.Get('org.freedesktop.UDisks.Device', 'DeviceIsReadOnly')),
            'block.device': str(DeviceFile),
            'block.parent': str(parent),
            'block.path': str(path)
        }
        DeviceMountPaths = device_props.Get('org.freedesktop.UDisks.Device', 'DeviceMountPaths')
        if DeviceMountPaths:
            # mounted at some location
            devdict['volume.mount_point'] = str(DeviceMountPaths[0])
        self.detected_devices[str(path)] = devdict
        kaa.MainThreadCallable(self._device_update_media)(devdict)
        return True

    @kaa.coroutine()
    def _device_update_media(self, devdict):
        """
        Device update handling
        """
        id = devdict.get('beacon.id')
        if self._db.medialist.get_by_media_id(id):
            # The device is already in the MediaList
            media = self._db.query_media(id)
            if media['content'] == 'file' and not devdict.get('volume.mount_point'):
                # it was mounted before and is now umounted. We remove it from the media list
                # FIXME: it will never be updated again, even if it is mounted again later
                self._device_remove(devdict.get('block.path'))
                self.eject(devdict)
                yield True
            # update the medialist Media
            media = self._db.medialist.get_by_media_id(id)
            media._beacon_update(devdict)
            self.handler.media_changed(media)
            yield True
        # get media information from the db
        media = self._db.query_media(id)
        if not media:
            # it is a new/unknown device
            if not devdict.get('volume.is_disc') == True:
                # It is a directory, no additional metadata
                metadata = None
            else:
                # It is a DVD, AudioCD, etc. Scan with kaa.metadata
                parse = kaa.ThreadCallable(kaa.metadata.parse)
                metadata = yield parse(devdict.get('block.device'))
            # Add the new device to the database
            if not (yield self._device_add_to_database(metadata, devdict)):
                log.error('%s not added to database', devdict.get('block.device'))
                yield False
            media = self._db.query_media(id)
        # now we have a valid media object from the db
        if media['content'] == 'file' and not devdict.get('volume.mount_point'):
            # It is a disc or partition with directories and
            # files. Mount it to become valid and wait for an update.
            # FIXME: mount only on request
            log.info('mount %s', devdict.get('block.device'))
            self.mount(devdict)
            yield True
        # add the device to the MediaList, returns Media object
        m = yield self._db.medialist.add(id, devdict)
        for d in ('large', 'normal', 'fail/beacon'):
            dirname = os.path.join(m.thumbnails, d)
            if not os.path.isdir(dirname):
                os.makedirs(dirname, 0700)
        # signal change
        self.handler.media_changed(m)

    @kaa.coroutine(policy=kaa.POLICY_SYNCHRONIZED)
    def _device_add_to_database(self, metadata, devdict):
        """
        Add the device to the database
        """
        yield kaa.inprogress(self._db.read_lock)
        id = devdict.get('beacon.id')
        if devdict.get('volume.is_disc') == True and metadata and \
               metadata.get('mime') in ('video/vcd', 'video/dvd'):
            # pass rom drive
            type = metadata['mime'][6:]
            log.info('detect %s as %s' % (devdict.get('beacon.id'), type))
            mid = self._db.add_object("media", name=devdict.get('beacon.id'), content=type)['id']
            vid = self._db.add_object("video", name="", parent=('media', mid),
                title=unicode(get_title(metadata['label'])), media = mid)['id']
            for track in metadata.tracks:
                self._db.add_object('track_%s' % type, name='%02d' % track.trackno,
                    parent=('video', vid), media=mid, chapters=track.chapters,
                    length=track.length, audio=[ x.convert() for x in track.audio ],
                    subtitles=[ x.convert() for x in track.subtitles ])
            yield True
        if devdict.get('volume.disc.has_audio') and metadata:
            # Audio CD
            log.info('detect %s as audio cd' % devdict.get('beacon.id'))
            mid = self._db.add_object("media", name=devdict.get('beacon.id'), content='cdda')['id']
            aid = self._db.add_object("audio", name='', title = metadata.get('title'),
                artist = metadata.get('artist'),
                parent=('media', mid), media = mid)['id']
            for track in metadata.tracks:
                self._db.add_object('track_cdda', name=str(track.trackno),
                    title=track.get('title'), artist=track.get('artist'),
                    parent=('audio', aid), media=mid)
            yield True
        # filesystem
        log.info('detect %s as filesystem' % devdict.get('beacon.id'))
        mid = self._db.add_object("media", name=devdict.get('beacon.id'), content='file')['id']
        mtime = 0
        if devdict.get('block.device'):
            mtime = os.stat(devdict.get('block.device'))[stat.ST_MTIME]
        self._db.add_object("dir", name="", parent=('media', mid), media=mid, mtime=mtime)
        yield True
