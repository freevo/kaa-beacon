# -*- coding: iso-8859-1 -*-
# -----------------------------------------------------------------------------
# plugins - Plugin interface for the Beacon client
# -----------------------------------------------------------------------------
# $Id: __init__.py 4193 2009-06-30 20:43:11Z tack $
#
# -----------------------------------------------------------------------------
# kaa.beacon - A virtual filesystem with metadata
# Copyright (C) 2011 Dirk Meyer
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

import kaa.utils

def load(client):
    """
    Load external plugins. Called by client on creating.
    """
    interface = {}
    plugins = kaa.utils.get_plugins('kaa.beacon.plugins', __file__, attr='Plugin')
    for name, plugin in plugins.items():
        if isinstance(plugin, Exception):
            log.error('Failed to import plugin %s: %s', name, plugin)
        else:
            interface.update(plugin.init(client))
    return interface
