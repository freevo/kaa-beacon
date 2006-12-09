# -*- coding: iso-8859-1 -*-
# -----------------------------------------------------------------------------
# media.py - Medialist handling
# -----------------------------------------------------------------------------
# $Id$
#
# -----------------------------------------------------------------------------
# kaa.beacon - A virtual filesystem with metadata
# Copyright (C) 2006 Dirk Meyer
#
# First Edition: Dirk Meyer <dischi@freevo.org>
# Maintainer:    Dirk Meyer <dischi@freevo.org>
#
# Please see the file AUTHORS for a complete list of authors.
#
# This library is free software; you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License version
# 2.1 as published by the Free Software Foundation.
#
# This library is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
# 02110-1301 USA
#
# -----------------------------------------------------------------------------

__all__ = [ 'medialist' ]

# python imports
import os
import logging

# kaa imports
from kaa.weakref import weakref

# kaa.beacon imports
import utils

# get logging object
log = logging.getLogger('beacon')


class Media(object):

    # check what mounpoint needs right now

    def __init__(self, id, db, controller, prop):
        self._db = db
        self._controller = controller
        self.id = id
        self.update(prop)

        # needed by server.
        self.crawler = None

        log.info('new media %s', self.id)

        # when we are here the media is in the database and also
        # the item for it

    def _beacon_controller(self):
        """
        Get the controller (the client or the server)
        """
        return self._controller


    def eject(self):
        self._controller.eject(self)


    def update(self, prop):
        self.prop = prop
        self.device = str(prop.get('block.device'))
        self.mountpoint = str(prop.get('volume.mount_point'))
        if not self.mountpoint:
            self.mountpoint = self.device
        if not self.mountpoint.endswith('/'):
            self.mountpoint += '/'
        self.overlay = os.path.join(self._db.dbdir, self.id)
        self._beacon_media = weakref(self)
        # get basic information from database
        media, self._beacon_id, self.root = \
               self._db.query_media(self.id, self)
        prop['beacon.content'] = media['content']
        self._beacon_isdir = False
        if media['content'] == 'file':
            self._beacon_isdir = True
        self.thumbnails = os.path.join(self.overlay, '.thumbnails')
        if self.mountpoint == '/':
            self.thumbnails = os.path.join(os.environ['HOME'], '.thumbnails')
        if self.root.get('title'):
            self.label = self.root.get('title')
        elif prop.get('volume.label'):
            self.label = utils.get_title(prop.get('volume.label'))
        elif prop.get('info.parent'):
            self.label = u''
            parent = prop.get('info.parent')
            if parent.get('storage.vendor'):
                self.label += parent.get('storage.vendor') + u' '
            if parent.get('info.product'):
                self.label += parent.get('info.product')
            self.label.strip()
            if not self.label:
                self.label = self.id
        else:
            self.label = self.id

#     def __del__(self):
#         print 'del', self

    def get(self, key):
        return self.prop.get(key)

    def __getitem__(self, key):
        return self.prop[key]

    def __setitem__(self, key, value):
        self.prop[key] = value

    def __repr__(self):
        return '<kaa.beacon.Media %s>' % self.id


class MediaList(object):

    def __init__(self):
        self._dict = dict()
        self.idlist = []
        self.db = None
        self.controller = None


    def connect(self, db, controller):
        for media in self._dict.keys()[:]:
            self.remove(media)
        self.db = db
        self.controller = controller


    def add(self, id, prop):
        if not self.db:
            raise RuntimeError('not connected to database')
        if id in self._dict:
            return self._dict.get(id)
        media = Media(id, self.db, self.controller, prop)
        self._dict[id] = media
        self.idlist = [ m._beacon_id[1] for m in self._dict.values() ]
        return media


    def remove(self, id):
        if not id in self._dict:
            log.error('%s not in list' % id)
            return None
        media = self._dict.pop(id)
        self.idlist = [ m._beacon_id[1] for m in self._dict.values() ]
        return media


    def get(self, id):
        return self._dict.get(id)


    def mountpoint(self, dirname):
        if not dirname.endswith('/'):
            dirname += '/'
        all = self._dict.values()[:]
        all.sort(lambda x,y: -cmp(x.mountpoint, y.mountpoint))
        for m in all:
            if dirname.startswith(m.mountpoint):
                return m
        return None


    def beacon_id(self, id):
        for m in self._dict.values():
            if m._beacon_id == id:
                return m
        return None


    def __iter__(self):
        return self._dict.values().__iter__()


medialist = MediaList()
