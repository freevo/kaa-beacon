# -*- coding: iso-8859-1 -*-
# -----------------------------------------------------------------------------
# db.py - Database for the VFS
# -----------------------------------------------------------------------------
# $Id: device.py 799 2005-09-16 14:27:36Z rshortt $
#
# TODO: handle all the FIXME and TODO comments inside this file and
#       add docs for functions, variables and how to use this file
#
# -----------------------------------------------------------------------------
# kaa-vfs - A virtual filesystem with metadata
# Copyright (C) 2005 Dirk Meyer
#
# First Edition: Dirk Meyer <dmeyer@tzi.de>
# Maintainer:    Dirk Meyer <dmeyer@tzi.de>
#
# Please see the file doc/CREDITS for a complete list of authors.
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
import threading
import logging
import time

# kaa imports
import kaa.notifier
from kaa.base import db
from kaa.base.db import *

# kaa.vfs imports
import item
import util

# get logging object
log = logging.getLogger('vfs')

class Database(threading.Thread):
    """
    A kaa.db based database in a thread.
    """

    class Query(object):
        """
        A query for the database with async callbacks to handle
        the results from the thread in the main loop.
        """
        def __init__(self, db, function):
            self.db = db
            self.function = function
            self.value = None
            self.valid = False
            self.exception = False
            self.callbacks = []

        def __call__(self, *args, **kwargs):
            self.db.condition.acquire()
            self.db.jobs.append((self, self.function, args, kwargs))
            self.db.condition.notify()
            self.db.condition.release()
            return self

        def connect(self, function, *args, **kwargs):
            if self.valid:
                return function(*args, **kwargs)
            cb = kaa.notifier.MainThreadCallback(function, *args, **kwargs)
            self.callbacks.append(cb)

        def set_value(self, value, exception=False):
            self.value = value
            self.exception = exception
            self.valid = True
            for callback in self.callbacks:
                callback()
            self.callbacks = []

        def get(self):
            while not self.valid:
                kaa.notifier.step()
            return self.value


    def __init__(self, dbdir):
        """
        Init function for the threaded database.
        """
        # threading setup
        threading.Thread.__init__(self)
        self.setDaemon(True)
        self.stopped = False

        # internal db dir, it contains the real db and the
        # overlay dir for the vfs
        self.dbdir = dbdir

        # list of jobs for the thread and the condition to
        # change that list
        self.jobs = [ None ]
        self.condition = threading.Condition()

        # flag if the db should be read only
        self.read_only = False

        # handle changes in a list and add them to the database
        # on commit. This needs a lock because objects can be added
        # from the main loop and commit is called inside a thread
        self.changes_lock = threading.Lock()
        self.changes = []

        # start thread
        self.start()

        # wait for complete database setup. Do this by adding an
        # empty query to the db and wait for the retured data.
        Database.Query(self, None)().get()


    def __getattr__(self, attr):
        """
        Interface to the db. All calls to the db are wrapped into
        Database.Query objects to be handled in a thread.
        """
        if attr == 'object_types':
            # return the attribute _object_types from the db
            return self._db._object_types
        if attr in ('commit', 'query'):
            # commit and query are not used from the db but from this
            # class to do something special
            return Database.Query(self, getattr(self, '_' + attr))
        return Database.Query(self, getattr(self._db, attr))


    def _get_dir(self, dirname):
        """
        Get database entry for the given directory. Called recursive to
        find the current entry. Do not cache results, they could change.
        """
        if dirname == '/':
            # we know that '/' is in the db
            current = self._db.query(type="dir", name='/')[0]
            return item.create(current, None, self._db)
        parent = self._get_dir(os.path.dirname(dirname))
        if not parent:
            return None

        # TODO: handle dirs on romdrives which don't have '/'
        # as basic parent

        name = os.path.basename(dirname)
        current = self._db.query(type="dir", name=name, parent=parent.dbid)
        if not current and self.read_only:
            return
        if not current:
            current = self._db.add_object("dir", name=name, parent=parent.dbid)
            self._db.commit()
        else:
            current = current[0]
        return item.create(current, parent, self._db)


    def _commit(self):
        """
        Commit changes to the database. All changes in the internal list
        are done first to reduce the time the db is locked.
        """
        self.changes_lock.acquire()
        changes = self.changes
        self.changes = []
        self.changes_lock.release()
        for c in changes:
            # It could be possible that an item is added twice. But this is no
            # problem because the duplicate will be removed at the
            # next query. It can also happen that a dir is added because of
            # _getdir and because of the parser. We can't avoid that but the
            # db should clean itself up.
            c[0](*c[1], **c[2])
        self._db.commit()
        log.info('db.commit')


    def _delete(self, entry):
        """
        Delete item with the given id from the db and all items with that
        items as parent (and so on). To avoid internal problems, make sure
        commit is called just after this function is called.
        """
        log.debug('DELETE %s' % entry)
        for child in self._db.query(parent = (entry['type'], entry['id'])):
            self._delete(child)
        self.delete_object((entry['type'], entry['id']))


    def _query_dirname(self, *args, **kwargs):
        """
        A query to get all files in a directory. Special keyword 'dirname' in
        the query is used for that.
        """
        dirname = kwargs['dirname']
        del kwargs['dirname']
        parent = self._get_dir(dirname)
        if parent:
            files = self._db.query(parent = ("dir", parent["id"]))
        else:
            files = []
            parent = dirname + '/'

        fs_listing = util.listdir(dirname, self.dbdir)
        need_commit = False

        items = []
        for f in files[:]:
            if f['name'] in fs_listing:
                # file still there
                fs_listing.remove(f['name'])
                items.append(item.create(f, parent, self))
            else:
                # file deleted
                files.remove(f)
                if not self.read_only:
                    # delete from database by adding it to the internal changes
                    # list. It will be deleted right before the next commit.
                    self.changes_lock.acquire()
                    self.changes.append((self._delete, [f], {}))
                    self.changes_lock.release()
                    need_commit = True

        for f in fs_listing:
            # new files
            items.append(item.create(f, parent, self))

        if need_commit:
            # need commit because some items were deleted from the db
            self._commit()

        # sort result
        items.sort(lambda x,y: cmp(x.url, y.url))
        return items

    def _query_files(self, *args, **kwargs):
        """
        A query to get a list of files. Special keyword 'filenames' (list) in
        the query is used for that.
        """
        files = kwargs['files']
        del kwargs['files']
        items = []
        for f in files:
            dirname = os.path.dirname(f)
            basename = os.path.basename(f)
            # TODO: cache parents here
            parent = self._get_dir(dirname)
            if parent:
                dbentry = self._db.query(parent = parent.dbid, name=basename)
                if not dbentry:
                    dbentry = basename
                else:
                    dbentry = dbentry[0]
            else:
                parent = dirname
                dbentry = basename
            items.append(item.create(dbentry, parent, self))
        return items


    def _query_attr(self, *args, **kwargs):
        """
        A query to get a list of possible values of one attribute. Special
        keyword 'attr' the query is used for that. This query will not return
        a list of items.
        """
        kwargs['distinct'] = True
        kwargs['attrs'] = [ kwargs['attr'] ]
        del kwargs['attr']
        return [ x[1] for x in self._db.query_raw(**kwargs)[1] if x[1] ]


    def _query(self, *args, **kwargs):
        """
        Internal query function inside the thread. This function will use the
        corrent internal query function based on special keywords.
        """
        if 'dirname' in kwargs:
            return self._query_dirname(*args, **kwargs)
        if 'files' in kwargs:
            return self._query_files(*args, **kwargs)
        if 'attr' in kwargs:
            return self._query_attr(*args, **kwargs)
        return self._db.query(*args, **kwargs)


    def add_object(self, *args, **kwargs):
        """
        Add an object to the db. If the keyword 'vfs_immediately' is set, the
        object will be added now and the db will be locked until the next commit.
        To avoid locking, do not se the keyword, but this means that a requery on
        the object won't find it before the next commit.
        """
        if 'vfs_immediately' in kwargs:
            del kwargs['vfs_immediately']
            return Database.Query(self, self._db.add_object)(*args, **kwargs)
        self.changes_lock.acquire()
        self.changes.append((self._db.add_object, args, kwargs))
        self.changes_lock.release()


    def update_object(self, *args, **kwargs):
        """
        Update an object to the db. If the keyword 'vfs_immediately' is set, the
        object will be updated now and the db will be locked until the next commit.
        To avoid locking, do not se the keyword, but this means that a requery on
        the object will return the old values.
        """
        if 'vfs_immediately' in kwargs:
            del kwargs['vfs_immediately']
            return Database.Query(self, self._db.update_object)(*args, **kwargs)
        self.changes_lock.acquire()
        self.changes.append((self._db.update_object, args, kwargs))
        self.changes_lock.release()


    def register_object_type_attrs(self, *args, **kwargs):
        """
        Register a new object with attributes. Special keywords like name and
        mtime are added by default.
        """
        kwargs['name'] = (str, ATTR_KEYWORDS_FILENAME)
        kwargs['mtime'] = (int, ATTR_SIMPLE)
        return Database.Query(self, self._db.register_object_type_attrs)(*args, **kwargs)


    def run(self):
        """
        Main loop for the thread handling the db. SQLLite objects can only be used
        in the thread they are created, that's why everything is wrapped.
        """
        if not os.path.isdir(self.dbdir):
            os.makedirs(self.dbdir)
        self._db = db.Database(self.dbdir + '/db')

        self._db.register_object_type_attrs("dir",
            name = (str, ATTR_KEYWORDS_FILENAME),
            mtime = (int, ATTR_SIMPLE))

        self._db.register_object_type_attrs("file",
            name = (str, ATTR_KEYWORDS_FILENAME),
            mtime = (int, ATTR_SIMPLE))

        root = self._db.query(type="dir", name="/")
        if not root:
            root = self._db.add_object("dir", name="/")
        else:
            root = root[0]
        root['url'] = 'file:/'
        root = item.create(root, None, self._db)

        # remove dummy job for startup
        self.jobs = self.jobs[1:]

        while not self.stopped:
            self.condition.acquire()
            while not self.jobs and not self.stopped:
                # free memory
                callback = function = r = None
                self.condition.wait()
            if self.stopped:
                self.condition.release()
                continue
            callback, function, args, kwargs = self.jobs[0]
            self.jobs = self.jobs[1:]
            self.condition.release()
            try:
                r = None
                if function:
                    t1 = time.time()
                    r = function(*args, **kwargs)
                    t2 = time.time()
                callback.set_value(r)
                kaa.notifier.wakeup()
            except Exception, e:
                log.exception("database error")
                callback.set_value(e, True)
                kaa.notifier.wakeup()
