# -*- coding: iso-8859-1 -*-
# -----------------------------------------------------------------------------
# parser.py - Parser for metadata
# -----------------------------------------------------------------------------
# $Id$
#
# Note: this file is only imported by the server
#
# -----------------------------------------------------------------------------
# kaa.beacon.server - A virtual filesystem with metadata
# Copyright (C) 2006-2009 Dirk Meyer
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


# Python imports
import os
import logging
import time

# kaa imports
import kaa
import kaa.metadata
import kaa.imlib2

# kaa.beacon imports
from .. import thumbnail
import utils

# get logging object
log = logging.getLogger('beacon.parser')

# load extra plugins in the plugins subdirectory
extention_plugins = {}

media_types = {
    kaa.metadata.MEDIA_AUDIO: 'audio',
    kaa.metadata.MEDIA_VIDEO: 'video',
    kaa.metadata.MEDIA_IMAGE: 'image',
    kaa.metadata.MEDIA_AV: 'video',
    kaa.metadata.MEDIA_DIRECTORY: 'dir'
}

# parsing thread
kaa.register_thread_pool('beacon::metadata', kaa.ThreadPool())
parse_thread = kaa.ThreadPoolCallable('beacon::metadata', kaa.metadata.parse)

def register(ext, function):
    """
    Register a plugin to the parser. This function gets called by the
    external plugins in the plugins subdir.
    """
    if not ext in extention_plugins:
        extention_plugins[ext] = []
    extention_plugins[ext].append(function)


def parse(db, item, force_thumbnail_check=False):
    """
    Main beacon parse function. Return the load this function produced:
    0 == nothing done
    1 == normal parsing (as InProgress object)
    2 == thumbnail storage (as InProgress object)
    """
    mtime = item._beacon_mtime
    if mtime == None:
        if item.isdir or item.isfile:
            log.warning('no mtime, skip %s' % item)
            return 0
        # neither dir nor file, it can not have a mtime
        image = item._beacon_data.get('image')
        if image and (image.startswith('http://') or os.path.exists(image)):
            t = thumbnail.Thumbnail(image, item._beacon_media)
            if t.needs_update:
                log.debug('create missing image %s for %s', image, item)
                t.create(t.PRIORITY_LOW)
        return 0

    parent = item._beacon_parent

    if not parent:
        log.warning('no parent, skip %s' % item)
        return 0

    if parent._beacon_id and not item._beacon_id:
        # check if the item is in the db now from a different
        # list of items.
        db.sync_item(item)

    if item._beacon_data.get('mtime') == mtime:
        # The item already is in the database and the mtime is unchanged.
        # This means we don't need to scan again, but we check if the
        # thumbnail is valid or not.
        if force_thumbnail_check and item._beacon_data.get('image'):
            image = item._beacon_data.get('image')
            if image.startswith('http://') or os.path.exists(image):
                t = thumbnail.Thumbnail(image, item._beacon_media)
                if t.needs_update:
                    log.info('create missing image %s for %s', image, item)
                    t.create(t.PRIORITY_LOW)
                return 0
            else:
                log.info('image "%s" for %s is gone, rescan', image, item)
        else:
            return 0

    # looks like we have more to do. Start the coroutine part of the parser
    return _parse(db, item, mtime)


@kaa.coroutine()
def _parse(db, item, mtime):
    """
    Parse the item, this can take a while.
    """
    produced_load = 0
    try:
        #
        # Parent checking
        #

        parent = item._beacon_parent
        if not parent._beacon_id:
            # There is a parent without id, update the parent now.
            r = parse(db, parent)
            if isinstance(r, kaa.InProgress):
                yield r
            if not parent._beacon_id:
                # This should never happen
                raise AttributeError('parent for %s has no dbid' % item)
            # we had no parent id which we have now. Restart the whole
            # parsing process. maye this item was in the db already
            r = parse(db, parent)
            if isinstance(r, kaa.InProgress):
                r = yield r
            yield r


        #
        # Metadata parsing
        #

        t1 = time.time()

        # FIXME: add force parameter from config file:
        # - always force (slow but best result)
        # - never force (faster but maybe wrong)
        # - only force on media 1 (good default)

        # Parse metadata in an extra named thread
        metadata = yield parse_thread(item.filename)
        if not metadata:
            metadata = {}

        attributes = { 'mtime': mtime, 'image': metadata.get('image') }

        if metadata.get('media') == kaa.metadata.MEDIA_DISC and \
               metadata.get('subtype') in db.list_object_types():
            type = metadata['subtype']
            if metadata.get('type'):
                attributes['scheme'] = metadata.get('type').lower()
            item._beacon_isdir = False
        elif media_types.get(metadata.get('media')) in db.list_object_types():
            type = media_types.get(metadata['media'])
        elif item._beacon_isdir:
            type = 'dir'
        else:
            type = 'file'

        if item._beacon_id and type != item._beacon_id[0]:
            # The item changed its type. Adjust the db
            yield kaa.inprogress(db.read_lock)
            data = db.update_object_type(item._beacon_id, type)
            if not data:
                log.info('item to change not in the db anymore')
            log.info('change item %s to %s' % (item._beacon_id, type))
            item._beacon_database_update(data)


        #
        # Thumbnail / Cover / Image stuff.
        #

        produced_load = 1

        if type == 'dir':
            # If the image was detected by the parser, do not override
            # it in add_directory_attributes
            attributes['image_from_parser'] = bool(attributes.get('image'))

        elif type == 'image':
            attributes['image'] = item.filename
            if metadata.get('thumbnail'):
                t = thumbnail.Thumbnail(item.filename, item._beacon_media)
                if t.needs_update:
                    # only store the normal version
                    try:
                        produced_load = 2
                        t.normal = kaa.imlib2.open_from_memory(metadata.get('thumbnail'))
                    except (ValueError, IOError):
                        log.exception('image thumbnail')
        else:
            base = os.path.splitext(item.filename)[0]
            if type == 'video' and not attributes.get('image') and thumbnail.SUPPORT_VIDEO:
                attributes['image'] = item.filename
            if metadata.get('thumbnail') and not attributes.get('image'):
                attributes['image'] = item.filename
                t = thumbnail.Thumbnail(item.filename, item._beacon_media)
                try:
                    produced_load = 2
                    t.image = kaa.imlib2.open_from_memory(metadata['thumbnail'])
                except (ValueError, IOError):
                    log.exception('raw thumbnail')
            for ext in ('.jpg', '.png'):
                if os.path.isfile(base + ext):
                    if type == 'video':
                        attributes['poster'] = base + ext
                    else:
                        attributes['image'] = base + ext
                    break
                if os.path.isfile(item.filename + ext):
                    if type == 'video':
                        attributes['poster'] = item.filename + ext
                    else:
                        attributes['image'] = item.filename + ext
                    break

        #
        # Type specific attributes
        #
        if type == 'video':
            # Normally db.add_object() will take care of assigning type
            # attributes from metadata, but some attributes for videos
            # aren't at the top-level attribute object.  For video
            # dimensions, take the dimensions of the first video track
            # (of the longest title, if applicable).
            video = None
            if metadata.get('video'):
                video = metadata.video[0]
            elif metadata.get('tracks'):
                # Find the longest title with a video track.
                for title in sorted(metadata.tracks, key=lambda t: -t.length):
                    if title.get('video'):
                        video = title.video[0]
                        break
            if video:
                attributes['width'] = video.get('width')
                attributes['height'] = video.get('height')
            attributes['series'] = metadata.series
            attributes['season'] = metadata.season
            attributes['episode'] = metadata.episode

        attributes['metadata'] = metadata

        # now call extention plugins
        ext = os.path.splitext(item.filename)[1]
        for function in extention_plugins.get(ext, []) + extention_plugins.get(None, []):
            function(item, attributes, type)

        yield kaa.inprogress(db.read_lock)

        if attributes.get('image'):
            # create thumbnail
            t = thumbnail.Thumbnail(attributes.get('image'), item._beacon_media)
            if t.needs_update and (not type == 'video' or not hasattr(item, 'filename') or
                    utils.do_thumbnail(item.filename)):
                t.create(t.PRIORITY_LOW)



        #
        # Database code
        #
        # add kaa.metadata results, the db module will add everything known
        # to the db. After that add or update the database.
        #

        if item._beacon_id:
            # Update old db entry
            db.update_object(item._beacon_id, **attributes)
            item._beacon_data.update(attributes)
        else:
            # check if for some reasons the same item was parsed
            # parallel. If so, do not add it again and reuse the id
            entry = db._db.query(parent=parent._beacon_id, name=item._beacon_data['name'])
            if entry:
                # Update old db entry
                log.error('item already in db, re-use beacon_id')
                db.update_object((entry[0]['type'], entry[0]['id']), **attributes)
                obj = db._db.query(parent=parent._beacon_id, name=item._beacon_data['name'])[0]
            else:
                # Create new entry
                obj = db.add_object(type, name=item._beacon_data['name'], parent=parent, **attributes)
            item._beacon_database_update(obj)

        #
        # Additional track handling
        #

        if hasattr(metadata, 'tracks'):
            # The item has tracks, e.g. a dvd image on hd.
            if not metadata.get('type'):
                log.error('%s metadata has no type', item)
                yield produced_load

            # delete all known tracks before adding new
            result = yield db.query(parent=item)
            for track in result:
                db.delete_object(track)

            if not 'track_%s' % metadata.get('type').lower() in \
                   db.list_object_types():
                key = metadata.get('type').lower()
                log.error('track_%s not in database keys', key)
                yield produced_load
            type = 'track_%s' % metadata.get('type').lower()
            for track in metadata.tracks:
                db.add_object(type, name=str(track.trackno), parent=item, metadata=track)

        # parsing done
        log.info('scan %s (%0.3f)' % (item, time.time() - t1))

    except GeneratorExit:
        # Don't catch this, otherwise if the coroutine is aborted you get
        # "_parse() ignored GeneratorExit"
        raise
    except Exception, e:
        log.exception('parser error: %s', item)

    yield produced_load


@kaa.coroutine()
def add_directory_attributes(db, directory):
    """
    Add some extra attributes for a directory recursive. This function
    checkes album, artist, image and length. When there are changes,
    go up to the parent and check it, too.
    """
    data = { 'length': 0, 'artist': u'', 'album': u'', 'image': '', 'series': '', 'season': '' }
    check_attr = data.keys()[:]
    check_attr.remove('length')

    if directory._beacon_data['image_from_parser'] and \
       directory._beacon_data['image']:
        # The directory had an image defined and found by the parser.
        # Delete image from data, we don't want to override it.
        del data['image']

    items = { 'video': [], 'audio': [], 'image': [], 'dir': [], 'other': [] }
    for item in (yield db.query(parent=directory)):
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
    data = dict([ x for x in data.items() if x[1] is not None ])
    for attr in data.keys():
        if data[attr] != directory._beacon_data[attr]:
            break
    else:
        # no changes.
        yield True

    yield kaa.inprogress(db.read_lock)

    # update directory in database
    db.update_object(directory._beacon_id, **data)
    directory._beacon_data.update(data)

    # check parent
    if directory._beacon_parent:
        yield add_directory_attributes(db, directory._beacon_parent)
