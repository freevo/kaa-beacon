__all__ = [ 'signals', 'Device', 'start' ]

import sys
import os
import time
import signal
import logging

import kaa.notifier
import kaa.metadata

# check for dbus and it's version
import dbus 
if getattr(dbus, 'version', (0,0,0)) < (0,51,0):
    raise ImportError('dbus >= 0.51.0 not found')
import dbus.glib

# use gtk main loop
kaa.notifier.init('gtk')

from utils import fstab
from cdrom import eject

# get logging object
log = logging.getLogger('beacon.hal')

# HAL signals
signals = { 'add': kaa.notifier.Signal(),
            'remove': kaa.notifier.Signal(),
            'changed': kaa.notifier.Signal(),
            'failed': kaa.notifier.Signal()
          }

class Device(object):
    """
    A device object
    """
    def __init__(self, prop, bus):
        self.udi = prop['info.udi']
        self.prop = prop
        self._eject = False
        self._bus = bus
        
    # -------------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------------

    def mount(self, umount=False):
        """
        Mount or umount the device.
        """
        if self.prop.get('volume.mount_point') and not umount:
            # already mounted
            return False
        for device, mountpoint, type, options in fstab():
            if device == self.prop['block.device'] and \
                   (options.find('users') >= 0 or os.getuid() == 0):
                cmd = ('mount', self.prop['block.device'])
                if umount:
                    cmd = ('umount', self.prop['block.device'])
                break
        else:
            if umount:
                cmd = ("pumount-hal", self.udi)
            else:
                cmd = ("pmount-hal", self.udi)
        proc = kaa.notifier.Process(cmd)
        proc.signals['stdout'].connect(log.warning)
        proc.signals['stderr'].connect(log.error)
        proc.start()
        return True


    def eject(self):
        """
        Eject the device. This includes umounting and removing from
        the list. Devices that can't be ejected (USB sticks) are only
        umounted and removed from the list.
        """
        if self.prop.get('volume.mount_point'):
            # umount before eject
            self._eject = True
            return self.mount(umount=True)
        # remove from list
        _device_remove(self.udi)
        if self.prop.get('volume.is_disc'):
            eject(self.prop['block.device'])
        

    def __getattr__(self, attr):
        return getattr(self.prop, attr)


    # -------------------------------------------------------------------------
    # Callbacks
    # -------------------------------------------------------------------------

    def _modified (self, num_changes, change_list):
        """
        Device was modified (mount, umount..)
        """
        for c in change_list:
            if c[0] == 'volume.mount_point':
                obj = self._bus.get_object('org.freedesktop.Hal', self.udi)
                obj = dbus.Interface(obj, 'org.freedesktop.Hal.Device')
                obj.GetAllProperties(reply_handler=self._property_update,
                                     error_handler=log.error)


    def _property_update(self, prop):
        """
        Update internal property list and call signal.
        """
        if not prop.get('volume.mount_point') and self._eject:
            self.prop = prop
            return self.eject()
        signals['changed'].emit(self, prop)
        self.prop = prop



# -----------------------------------------------------------------------------
# Connection handling
# -----------------------------------------------------------------------------

_bus = None
_connection_timeout = 5

def _connect_to_hal():
    global _bus
    global _connection_timeout
    _connection_timeout -= 1
    try:
        if not _bus:
            _bus = dbus.SystemBus()
    except Exception, e:
        # unable to connect to dbus
        if not _connection_timeout:
            # give up
            signals['failed'].emit('unable to connect to dbus')
            return False
        kaa.notifier.OneShotTimer(_connect_to_hal).start(2)
        return False
    obj = _bus.get_object('org.freedesktop.Hal', '/org/freedesktop/Hal/Manager')
    # DONT ASK! dbus sucks!
    kaa.notifier.Timer(_connect_to_hal_because_dbus_sucks, obj).start(0.01)
    return False


def _connect_to_hal_because_dbus_sucks(obj):
    if obj._introspect_state == obj.INTROSPECT_STATE_INTROSPECT_IN_PROGRESS:
        return True
    if obj._introspect_state == obj.INTROSPECT_STATE_DONT_INTROSPECT:
        if not _connection_timeout:
            # give up
            signals['failed'].emit('unable to connect to hal')
            return False
        kaa.notifier.OneShotTimer(_connect_to_hal).start(2)
        return False
    hal = dbus.Interface(obj, 'org.freedesktop.Hal.Manager')
    hal.GetAllDevices(reply_handler=_device_all, error_handler=log.error)
    hal.connect_to_signal('DeviceAdded', _device_new)
    hal.connect_to_signal('DeviceRemoved', _device_remove)
    return False
    

# -----------------------------------------------------------------------------
# Device handling
# -----------------------------------------------------------------------------

_devices = []

#list all devices
def _device_all(device_names):
    #first build list of all Device objects
    for name in device_names:
        obj = _bus.get_object("org.freedesktop.Hal", str(name))
        obj.GetAllProperties(dbus_interface="org.freedesktop.Hal.Device",
                             reply_handler=_device_add,
                             error_handler=log.error)


def _device_new(udi):
    obj = _bus.get_object("org.freedesktop.Hal", udi)
    obj.GetAllProperties(dbus_interface="org.freedesktop.Hal.Device",
                         reply_handler=_device_add,
                         error_handler=log.error)
    

#lost device
def _device_remove(udi):
    for dev in _devices:
        if dev.udi == udi:
            break
    else:
        return True
    sig = _bus.remove_signal_receiver
    sig(dev._modified, "PropertyModified", 'org.freedesktop.Hal.Device',
        "org.freedesktop.Hal", udi)
    _devices.remove(dev)
    # signal changes
    signals['remove'].emit(dev)
    

#add new device
def _device_add(prop):
    # only handle mountable devices
    if not "volume.mount_point" in prop:
        return
    if not prop.get('volume.is_disc'):
        # no disc, check if the device is removable
        try:
            fd = open(os.path.dirname(prop["linux.sysfs_path_device"]) + '/removable')
            rm = fd.read(1)
            fd.close()
            if rm != '1':
                # not removable
                return
        except (OSError, KeyError):
            # Error reading info. Either file not found, linux.sysfs_path_device
            # not in prop or no read permissions. So not removable in that case.
            return
    elif prop.get('block.device'):
        # set nice beacon unique id
        try:
            prop['volume.uuid'] = kaa.metadata.getid(prop.get('block.device'))[1]
        except Exception, e:
            log.exception('device checking')
            return
        
    dev = Device(prop, _bus)
    _devices.append(dev)
    sig = _bus.add_signal_receiver
    sig(dev._modified, "PropertyModified", 'org.freedesktop.Hal.Device',
        "org.freedesktop.Hal", prop['info.udi'])
    # signal changes
    signals['add'].emit(dev)
        


# connect to hal
def start():
    _connect_to_hal()