# -*- coding: iso-8859-1 -*-
# -----------------------------------------------------------------------------
# db.py - Beacon database
# -----------------------------------------------------------------------------
# kaa.beacon - A virtual filesystem with metadata
# Copyright (C) 2006-2007 Dirk Meyer
#
# First Edition: Dirk Meyer <https://github.com/Dischi>
# Maintainer:    Dirk Meyer <https://github.com/Dischi>
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

# python imports
import os
import stat
import logging
import time
import hashlib

# kaa imports
import kaa
from kaa import db

# beacon imports
from item import Item
from media import MediaList, FakeMedia

# get logging object
log = logging.getLogger('beacon.db')

# Item generation mapping
from file import File
from item import Item

def create_item(data, parent):
    """
    Create an Item that is neither dir nor file.
    """
    data = dict(data)
    dbid = (data['type'], data['id'])
    if 'url' in data:
        # url is stored in the data
        return Item(dbid, data['url'], data, parent, parent._beacon_media)
    if '://' in data['name']:
        # url is stored in the name (remote items in directory)
        return Item(dbid, data['name'], data, parent, parent._beacon_media)
    # generate url based on name and parent url
    url = parent.url
    if data['name']:
        if parent.url.endswith('/'):
            url = parent.url + data['name']
        else:
            url = parent.url + '/' + data['name']
    if data.get('scheme'):
        url = data.get('scheme') + url[url.find('://')+3:]
    return Item(dbid, url, data, parent, parent._beacon_media)

def create_file(data, parent, isdir=False):
    """
    Create a File object representing either a file or directory.
    """
    if isinstance(data, str):
        # fake item, there is no database entry
        id = None
        filename = parent.filename + data
        data = { 'name': data }
        if parent and parent._beacon_id:
            data['parent_type'], data['parent_id'] = parent._beacon_id
        media = parent._beacon_media
        if isdir:
            filename += '/'
    elif isinstance(parent, File):
        # db data
        id = (data['type'], data['id'])
        media = parent._beacon_media
        filename = parent.filename + data['name']
        if isdir:
            filename += '/'
    elif not data['name']:
        # root directory
        id = (data['type'], data['id'])
        media = parent
        parent = None
        filename = media.mountpoint
    else:
        raise ValueError('unable to create File item from %s', data)
    return File(id, filename, data, parent, media, isdir)

def create_directory(data, parent):
    """
    Create a File object representing a directory.
    """
    return create_file(data, parent, True)

def create_by_type(data, parent, isdir=False):
    """
    Create file, directory or any other kind of item.
    If the data indicates it is not a file or the parent is not
    a directory, make it an Item, not a File.
    """
    if (data.get('name').find('://') > 0) or (parent and not parent.isdir):
        return create_item(data, parent)
    return create_file(data, parent, isdir)


class Database(kaa.Object):
    """
    Database API for the client side, providing read-only access to the
    beacon database.

    This class is subclassed by the server for the read/write database.
    """
    def __init__(self, dbdir):
        """
        Init function
        """
        super(Database, self).__init__()
        # internal db dir, it contains the real
        self.directory = dbdir
        self.medialist = MediaList()
        # create or open db
        self._db = db.Database(self.directory + '/db')

    def commit():
        """
        Stub on the client side: implemented in the server db
        """
        pass

    def add_object(*args, **kwargs):
        """
        Stub on the client side: implemented in the server db
        """
        pass

    def delete_object(*args, **kwargs):
        """
        Stub on the client side: implemented in the server db
        """
        pass

    def acquire_read_lock(self):
        """
        Stub on the client side: implemented in the server db
        """
        return kaa.InProgress().finish(None)

    def md5url(self, url, subdir):
        """
        Convert url into md5 sum
        """
        if url.startswith('http://'):
            subdir += '/%s/' % url[7:url[7:].find('/')+7]
        fname = hashlib.md5(url).hexdigest() + os.path.splitext(url)[1]
        return os.path.join(self.directory, subdir, fname)

    def get_db_info(self):
        """
        Returns information about the database.  Look at
        kaa.db.Database.get_db_info() for more details.
        """
        info = self._db.get_db_info()
        info['directory'] = self.directory
        return info

    # -------------------------------------------------------------------------
    # Query functions
    #
    # The query functions can modify the database when in server mode. E.g.
    # a directory query could detect deleted files and will delete them in
    # the database. In client mode, the query functions will use the database
    # read only.
    # -------------------------------------------------------------------------

    def query(self, **query):
        """
        Main query function. This function will call one of the specific
        query functions in this class depending on the query. This function
        returns an InProgress.
        """
        # Remove non-true recursive attribute from query (non-recursive is default).
        if not query.get('recursive', True):
            del query['recursive']
        # Passed by caller to collect list of deleted items for directory query.
        garbage = query.pop('garbage', None)
        # do query based on type
        if query.keys() == ['filename']:
            fname = os.path.realpath(query['filename'])
            return kaa.InProgress().execute(self.query_filename, fname)
        if query.keys() == ['id']:
            return kaa.InProgress().execute(self._db_query_id, query['id'])
        if sorted(query.keys()) == ['parent', 'recursive']:
            if not query['parent']._beacon_isdir:
                raise AttributeError('parent is no directory')
            return self._db_query_dir_recursive(query['parent'], garbage)
        if 'parent' in query:
            if len(query) == 1:
                if query['parent']._beacon_isdir:
                    return self._db_query_dir(query['parent'], garbage)
            query['parent'] = query['parent']._beacon_id
        if 'media' not in query and query.get('type') != 'media':
            # query only media we have right now
            query['media'] = db.QExpr('in', self.medialist.get_all_beacon_ids())
        elif query.get('media') == 'all':
            del query['media']
        if 'attr' in query:
            return kaa.InProgress().execute(self._db_query_attr, query)
        if query.get('type') == 'media':
            return kaa.InProgress().execute(self._db.query, **query)
        return self._db_query_raw(query)

    def query_media(self, media):
        """
        Get media information.
        """
        if hasattr(media, 'id'):
            # object is a media object
            id = media.id
        else:
            # object is only an id
            id = media
            media = None
        result = self._db.query(type="media", name=id)
        if not result:
            return None
        result = result[0]
        if not media:
            return result
        # TODO: it's a bit ugly to set url here, but we have no other choice
        media.url = result['content'] + '://' + media.mountpoint
        dbid = ('media', result['id'])
        media._beacon_id = dbid
        root = self._db.query(parent=dbid)[0]
        if root['type'] == 'dir':
            media.root = create_directory(root, media)
        else:
            media.root = create_item(root, media)
        return result

    @kaa.coroutine()
    def _db_query_dir(self, parent, garbage):
        """
        A query to get all files in a directory. The parameter parent is a
        directort object.
        """
        if parent._beacon_islink:
            # WARNING: parent is a link, we need to follow it
            dirname = os.path.realpath(parent.filename)
            parent = self.query_filename(dirname)
            if not parent._beacon_isdir:
                # oops, this is not directory anymore, return nothing
                yield []
        else:
            dirname = parent.filename[:-1]
        listing = parent._beacon_listdir()
        items = []
        if parent._beacon_id:
            items = [ create_by_type(i, parent, i['type'] == 'dir') \
                      for i in self._db.query(parent = parent._beacon_id) ]
        # sort items based on name. The listdir is also sorted by name,
        # that makes checking much faster
        items.sort(lambda x,y: cmp(x._beacon_name, y._beacon_name))
        # TODO: use parent mtime to check if an update is needed. Maybe call
        # it scan time or something like that. Also make it an option so the
        # user can turn the feature off.
        yield self.acquire_read_lock()
        pos = -1
        for f, fullname, stat_res in listing[0]:
            pos += 1
            isdir = stat.S_ISDIR(stat_res[stat.ST_MODE])
            if pos == len(items):
                # new file at the end
                if isdir:
                    items.append(create_directory(f, parent))
                    continue
                items.append(create_file(f, parent))
                continue
            while pos < len(items) and f > items[pos]._beacon_name:
                # file deleted
                i = items[pos]
                if not i.isdir and not i.isfile:
                    # A remote URL in the directory
                    pos += 1
                    continue
                items.remove(i)
                # Server only: delete from database by adding it to the
                # internal changes list. It will be deleted right before the
                # next commit.
                self.delete_object(i)
                if garbage is not None:
                    garbage.append(i)
            if pos < len(items) and f == items[pos]._beacon_name:
                # same file
                continue
            # new file
            if isdir:
                items.insert(pos, create_directory(f, parent))
                continue
            items.insert(pos, create_file(f, parent))
        if pos + 1 < len(items):
            # deleted files at the end
            for i in items[pos+1-len(items):]:
                if not i.isdir and not i.isfile:
                    # A remote URL in the directory
                    continue
                items.remove(i)
                # Server only: delete from database by adding it to the
                # internal changes list. It will be deleted right before the
                # next commit.
                self.delete_object(i)
                if garbage is not None:
                    garbage.append(i)
        # no need to sort the items again, they are already sorted based
        # on name, let us keep it that way. And name is unique in a directory.
        # items.sort(lambda x,y: cmp(x.url, y.url))
        yield items

    @kaa.coroutine()
    def _db_query_dir_recursive(self, parent, garbage):
        """
        Return all files in the directory 'parent' including files in
        subdirectories (and so on). The directories itself will not be
        returned. If a subdir is a softlink, it will be skipped. This
        query does not check if the files are still there and if the
        database list is up to date.
        """
        if parent._beacon_islink:
            # WARNING: parent is a link, we need to follow it
            dirname = os.path.realpath(parent.filename)
            parent = self.query_filename(dirname)
            if not parent._beacon_isdir:
                # oops, this is not directory anymore, return nothing
                yield []
        else:
            dirname = parent.filename[:-1]
        timer = time.time()
        items = []
        # A list of all directories we will look at. If a link is in the
        # directory it will be ignored.
        directories = [ parent ]
        while directories:
            parent = directories.pop(0)
            for i in (yield self._db_query_dir(parent, garbage)):
                if i.isdir and not i._beacon_islink:
                    directories.append(child)
                items.append(i)
            if time.time() > timer + 0.1:
                # we used too much time. Call yield NotFinished at
                # this point to continue later.
                timer = time.time()
                yield kaa.NotFinished
        # sort items based on name. The listdir is also sorted by name,
        # that makes checking much faster
        items.sort(lambda x,y: cmp(x._beacon_name, y._beacon_name))
        yield items

    def _db_query_id(self, (type, id), cache=None):
        """
        Return item based on (type,id). Use given cache if provided.
        """
        i = self._db.query(type=type, id=id)[0]
        # now we need a parent
        if i['name'] == '':
            # root node found, find correct mountpoint
            m = self.medialist.get_by_beacon_id(i['parent'])
            if not m:
                # media not mounted, make it an Item, not a File
                result = self._db.query(type="media", id=i['parent'][1])
                if not result:
                    raise AttributeError('bad media %s' % str(i['parent']))
                return create_item(i, FakeMedia(result[0]['name']))
            return create_directory(i, m)
        # query for parent
        pid = i['parent']
        if cache is not None and pid in cache:
            parent = cache[pid]
        else:
            parent = self._db_query_id(pid)
            if cache is not None:
                cache[pid] = parent
        if i['type'] == 'dir':
            # it is a directory, make a dir item
            return create_directory(i, parent)
        return create_by_type(i, parent)

    def _db_query_attr(self, query):
        """
        A query to get a list of possible values of one attribute. Special
        keyword 'attr' the query is used for that. This query will not return
        a list of items.
        """
        attr = query['attr']
        del query['attr']
        result = self._db.query(attrs=[attr], distinct=True, **query)
        result = [ x[attr] for x in result if x[attr] ]
        # sort results and return
        result.sort()
        return result

    @kaa.coroutine()
    def _db_query_raw(self, query):
        """
        Do a 'raw' query. This means to query the database and create
        a list of items from the result. The items will have a complete
        parent structure. For files / directories this function won't check
        if they are still there.
        """
        # FIXME: this function needs optimizing; adds at least 6 times the
        # overhead on top of kaa.db.query
        result = []
        cache = {}
        counter = 0
        timer = time.time()
        for media in self.medialist:
            cache[media._beacon_id] = media
            cache[media.root._beacon_id] = media.root
        for r in self._db.query(**query):
            # get parent
            pid = r['parent']
            if pid in cache:
                parent = cache[pid]
            else:
                parent = self._db_query_id(pid, cache)
                cache[pid] = parent
            # create item
            if r['type'] == 'dir':
                # it is a directory, make a dir item
                result.append(create_directory(r, parent))
            else:
                # file or something else
                result.append(create_by_type(r, parent))
            counter += 1
            if not counter % 50 and time.time() > timer + 0.05:
                # We used too much time. Call yield NotFinished at
                # this point to continue later.
                timer = time.time()
                yield kaa.NotFinished
        if not 'keywords' in query:
            # sort results by url (name is not unique) and return
            result.sort(lambda x,y: cmp(x.url, y.url))
        yield result

    def query_filename(self, filename):
        """
        Return item for filename, This function will
        never return an InProgress object.
        """
        dirname = os.path.dirname(filename)
        basename = os.path.basename(filename)
        m = self.medialist.get_by_directory(filename)
        if not m:
            raise AttributeError('mountpoint not found')
        if (os.path.isdir(filename) and \
            m != self.medialist.get_by_directory(dirname)) or filename == '/':
            # the filename is the mountpoint itself
            e = self._db.query(parent=m._beacon_id, name='')
            return create_directory(e[0], m)
        parent = self._query_filename_get_dir(dirname, m)
        if parent._beacon_id:
            # parent is a valid db item, query
            e = self._db.query(parent=parent._beacon_id, name=basename)
            if e:
                # entry is in the db
                return create_file(e[0], parent, e[0]['type'] == 'dir')
        return create_file(basename, parent, os.path.isdir(filename))

    def _query_filename_get_dir(self, dirname, media):
        """
        Get database entry for the given directory. Called recursive to
        find the current entry. Do not cache results, they could change.
        """
        if dirname == media.mountpoint or dirname +'/' == media.mountpoint:
            # we know that '/' is in the db
            c = self._db.query(type="dir", name='', parent=media._beacon_id)[0]
            return create_directory(c, media)
        if dirname == '/':
            raise RuntimeError('media %s not found' % media)
        parent = self._query_filename_get_dir(os.path.dirname(dirname), media)
        name = os.path.basename(dirname)
        if not parent._beacon_id:
            return create_directory(name, parent)
        c = self._db.query(type="dir", name=name, parent=parent._beacon_id)
        if c:
            return create_directory(c[0], parent)
        return self._query_filename_get_dir_create(name, parent)

    def _query_filename_get_dir_create(self, name, parent):
        """
        Stub on the client side: implemented in the server db
        """
        return create_directory(name, parent)
