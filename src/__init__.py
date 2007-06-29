# -*- coding: iso-8859-1 -*-
# -----------------------------------------------------------------------------
# __init__.py - interface to kaa.beacon
# -----------------------------------------------------------------------------
# $Id$
#
# -----------------------------------------------------------------------------
# kaa.beacon - A virtual filesystem with metadata
# Copyright (C) 2006-2007 Dirk Meyer
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

__all__ = [ 'connect', 'get', 'query', 'register_filter', 'Item',
            'THUMBNAIL_NORMAL', 'THUMBNAIL_LARGE' ]

# python imports
import os
import logging
import time

# kaa imports
import kaa.notifier

# kaa.beacon imports
from version import VERSION
from client import Client, CONNECTED, ConnectError
import thumbnail
from thumbnail import NORMAL as THUMBNAIL_NORMAL
from thumbnail import LARGE as THUMBNAIL_LARGE
from query import register_filter, wrap, Query
from item import Item
from media import medialist as media
from kaa.db import QExpr, ATTR_SIMPLE, ATTR_SEARCHABLE, ATTR_IGNORE_CASE, \
     ATTR_INDEXED, ATTR_INDEXED_IGNORE_CASE, ATTR_KEYWORDS

# get logging object
log = logging.getLogger('beacon')

# connected client object
_client = None
# signals of the client, only valid after calling connect()
signals = {}

debugging = """
------------------------------------------------------------------------
The system was unable to connect to beacon. Please check if the beacon
server is running properly. If beacon processes exist, please kill them.
Start beacon in an extra shell for better debugging. Start beacon with
beacon --start --verbose all --fg
------------------------------------------------------------------------
"""

def connect():
    """
    Connect to the beacon. A beacon server must be running. This function will
    raise an exception if the client is not connected and the server is not
    running for a connect.
    """
    global _client
    global signals

    if _client:
        return _client

    _client = Client()
    try:
        thumbnail.connect()
    except RuntimeError:
        # It was possible to connect to the beacon server but not
        # to the thumbnailer. Something is very wrong.
        log.error('unable to connect to beacon %s', debugging)
        raise RuntimeError('Unable to connect to beacon')
    signals = _client.signals
    log.info('beacon connected')
    return _client


def launch(autoshutdown=False, verbose='none'):
    """
    Lauch a beacon server.
    """
    beacon = os.path.dirname(__file__), '../../../../../bin/beacon'
    beacon = os.path.realpath(os.path.join(*beacon))
    if not os.path.isfile(beacon):
        # we hope it is in the PATH somewhere
        beacon = 'beacon'

    cmd = '%s --start --verbose=%s' % (beacon, verbose)
    if autoshutdown:
        cmd += ' --autoshutdown'
    if os.system(cmd):
        log.error('unable to connect to beacon %s', debugging)
        raise RuntimeError('Unable to connect to beacon')
    return connect()


def get(filename):
    """
    Get object for the given filename. This function will raise an exception if
    the client is not connected and the server is not running for a connect.
    If the client is still connecting or reconnecting, this function will block
    using kaa.notifier.step.
    """
    if not _client:
        connect()
    return _client.get(filename)


def query(**args):
    """
    Query the database. This function will raise an exception if the
    client is not connected and the server is not running for a connect.
    """
    if not _client:
        connect()
    return _client.query(**args)


def monitor(directory):
    """
    Monitor a directory with subdirectories for changes. This is done in
    the server and will keep the database up to date.
    """
    if not _client:
        connect()
    return _client.monitor(directory)


def register_file_type_attrs(name, **kwargs):
    """
    Register new attrs and types for files.
    """
    if not _client:
        connect()
    return _client.register_file_type_attrs(name, **kwargs)


def register_track_type_attrs(name, **kwargs):
    """
    Register new attrs and types for files.
    """
    if not _client:
        connect()
    return _client.register_track_type_attrs(name, **kwargs)


def get_db_info():
    """
    Gets statistics about the database. This function will block using
    kaa.notifier.step() until the client is connected.
    """
    if not _client:
        connect()
    while not _client.status == CONNECTED:
        kaa.notifier.step()
    return _client._beacon_db_information()


def delete_media(id):
    """
    Delete media with the given id.
    """
    if not _client:
        connect()
    while not _client.status == CONNECTED:
        kaa.notifier.step()
    return _client.delete_media(id)
