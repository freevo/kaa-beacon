# -*- coding: iso-8859-1 -*-
# -----------------------------------------------------------------------------
# videothumb - create a thumbnail for video files
# -----------------------------------------------------------------------------
# $Id$
#
# This file provides a function to create video thumbnails in the
# background.  It will start a mplayer to create the thumbnail. It
# uses the generic mainloop to do this without blocking.
#
# Loosly based on videothumb.py commited to the freevo wiki
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
import glob
import os
import stat
import logging
import random

# kaa imports
import kaa
import kaa.metadata
import kaa.imlib2

# kaa.beacon imports
from .. import libthumb
import cpuinfo

# get logging object
log = logging.getLogger('beacon.thumbnail')


class VideoThumb(object):
    """
    Class to handle video thumbnailing.
    """
    def __init__(self, thumbnailer):
        self.jobs = []
        self._current = None
        self.notify_client = thumbnailer.notify_client
        self.create_failed = thumbnailer.create_failed
        self.mplayer = kaa.Process(['mplayer', '-nosound', '-vo', 'png:z=2',
               '-frames', '10', '-osdlevel', '0', '-nocache', '-zoom', '-ss' ])
        self.mplayer.signals['stdout'].connect(self._handle_mplayer_debug)
        self.mplayer.signals['stderr'].connect(self._handle_mplayer_debug)

    def append(self, job):
        """
        Add a new video thumbnail job
        """
        self.jobs.append(job)
        self.start_mplayer()

    def _handle_mplayer_debug(self, line):
        """
        Handle stdout for debugging
        """
        pass

    def start_mplayer(self):
        """
        Start mplayer for the next job
        """
        if self.mplayer.is_alive() or not self.jobs or self._current or \
               kaa.main.is_shutting_down():
            return True
        self._current = self.jobs.pop(0)
        for size in ('large', 'normal'):
            imagefile = self._current.imagefile % size
            if not os.path.isfile(imagefile):
                break
            metadata = kaa.metadata.parse(imagefile)
            mtime = metadata.get('Thumb::MTime')
            if mtime != str(os.stat(self._current.filename)[stat.ST_MTIME]):
                break
        else:
            # not changed, refuse the recreate thumbnail
            self._current = None
            return self.start_mplayer()

        try:
            mpargs = [self._current.filename]
            pos = 0
            try:
                mminfo = self._current.metadata
                length = mminfo.length
                if mminfo.type == u'DVD':
                    # Find longest title.
                    track = sorted(mminfo.tracks, key = lambda x: x.length)[-1]
                    length = track.video[0].length
                    mpargs[0] = 'dvd://%d' % track.trackno
                    mpargs.extend(['-dvd-device', self._current.filename])
                elif mminfo.video[0].length:
                    length = mminfo.video[0].length

                # Pick a random position between 40-60%.  By randomizing, we give
                # the user the option to delete the original thumbnail and regenerate
                # a (likely) new one, in case the previous one wasn't very representative.
                pos = length * random.randrange(40, 60) / 100.0
                if hasattr(mminfo, 'type'):
                    # FIXME: dischi, this logic needs a comment.
                    if mminfo.type in ('MPEG-TS', 'MPEG-PES'):
                        pos = length / 20.0
            except (AttributeError, IndexError, TypeError):
                # else arbitrary consider that file is 1Mbps and grab position at 10%
                try:
                    pos = os.stat(self._current.filename)[stat.ST_SIZE]/1024/1024/10.0
                except (OSError, IOError):
                    # send message to client, we are done here
                    self.create_failed(self._current)
                    self.notify_client()
                    return

                if pos < 10:
                    # FIXME: needs another comment; is this because keyframes tend to be
                    # every 10 seconds?  But if pos < 10, won't we risk seeking to EOF and
                    # not getting any thumbnail at all?"
                    pos = 10

            self.mplayer.start([str(pos)] + mpargs).connect(self.create_thumbnail)
        except:
            log.exception('Thumbnail generation failure')


    def create_thumbnail(self, code):
        """
        Create thumbnail based on the captures
        """
        job = self._current
        self._current = None
        # find thumbnails
        captures = glob.glob('000000??.png')
        if not captures:
            # strange, no image files found
            self.create_failed(job)
            self.notify_client(job)
            job = None
            if (cpuinfo.cpuinfo()[cpuinfo.IDLE] < 40 or \
                cpuinfo.cpuinfo()[cpuinfo.IOWAIT] > 20):
                # too much CPU load, slow down
                return kaa.OneShotTimer(self.start_mplayer).start(1)
            self.start_mplayer()
            return
        # find the best image
        current_capture = captures[0], os.stat(captures[0])[stat.ST_SIZE]
        for c in captures[1:]:
            if os.stat(c)[stat.ST_SIZE] > current_capture[1]:
                current_capture = c, os.stat(c)[stat.ST_SIZE]
        current_capture = current_capture[0]
        try:
            # scale thumbnail
            for size, width, height in (('large',256,256), ('normal',128,128)):
                image = kaa.imlib2.open_without_cache(current_capture)
                if image.width > width or image.height > height:
                    image = image.scale_preserve_aspect((width,height))

                # XXX: why are we assuming 4:3 here?  This is a display policy
                # and belongs at the display layer.  Thumbnails should retain the
                # native aspect ratio of the source.
                #
                #if image.width * 3 > image.height * 4:
                #    # fix image with blank bars to be 4:3
                #    nh = (image.width*3)/4
                #    ni = kaa.imlib2.new((image.width, nh))
                #    ni.draw_rectangle((0,0), (image.width, nh), (0,0,0,255), True)
                #    ni.blend(image, dst_pos=(0,(nh- image.height) / 2))
                #    image = ni
                #elif image.width * 3 < image.height * 4:
                #    # strange aspect, let's guess it's 4:3
                #    new_size = (image.width, (image.width*3)/4)
                #    image = image.scale((new_size))
                try:
                    libthumb.png(job.filename, job.imagefile % size, (width, height), image._image)
                except (IOError, ValueError):
                    self.create_failed(job)
                    break
            # remove old stuff
            for capture in captures:
                os.remove(capture)
        except Exception, e:
            log.exception('video')
        # notify client and start next video
        self.notify_client(job)
        kaa.OneShotTimer(self.start_mplayer).start(1)
