from __future__ import with_statement

import os
import sys
import stat
import errno
import inspect
import logging
import calendar

from fuse import FUSE, FuseOSError, Operations, fuse_get_context

from ntfs.filesystem import NTFSFilesystem
from ntfs.filesystem import ChildNotFoundError

PERMISSION_ALL_READ = int("444", 8)

g_logger = logging.getLogger("ntfs.examples.mount")


def unixtimestamp(ts):
    """
    unixtimestamp converts a datetime.datetime to a UNIX timestamp.
    @type ts: datetime.datetime
    @rtype: int
    """
    return calendar.timegm(ts.utctimetuple())


def log(func):
    """
    log is a decorator that logs the a function call with its
      parameters and return value.
    """
    def inner(*args, **kwargs):
        func_name = inspect.stack()[3][3]
        if func_name == "_wrapper":
            func_name = inspect.stack()[2][3]
        (uid, gid, pid) = fuse_get_context()
        pre = "(%s: UID=%d GID=%d PID=%d ARGS=(%s) KWARGS=(%s))" % (
            func_name, uid, gid, pid,
            ", ".join(map(str, list(args)[1:])), str(**kwargs))
        try:
            g_logger.debug("log: call: %s",  pre)
            ret = func(*args, **kwargs)
            g_logger.debug("log: result: %s", ret)
            return ret
        except Exception as e:
            g_logger.warning("log: exception: %s", str(e))
            raise e
    return inner


class NTFSFuseOperations(Operations):
    def __init__(self, filesystem):
        self._fs = filesystem
        self._opened_files = {}

    def _get_path_entry(self, path):
        root = self._fs.get_root_directory()
        if path == "/":
            g_logger.debug("asking for root")
            entry = root
        else:
            _, __, rest = path.partition("/")
            g_logger.debug("asking for: %s", rest)
            try:
                entry = root.get_path_entry(rest)
            except ChildNotFoundError:
                raise FuseOSError(errno.ENOENT)
        return entry

    # Filesystem methods
    # ==================
    @log
    def getattr(self, path, fh=None):
        (uid, gid, pid) = fuse_get_context()
        entry = self._get_path_entry(path)

        if entry.is_directory():
            mode = (stat.S_IFDIR | PERMISSION_ALL_READ)
            nlink = 2
        else:
            mode = (stat.S_IFREG | PERMISSION_ALL_READ)
            nlink = 1

        return {
            "st_atime": unixtimestamp(entry.get_si_accessed_timestamp()),
            "st_ctime": unixtimestamp(entry.get_si_changed_timestamp()),
            "st_crtime": unixtimestamp(entry.get_si_created_timestamp()),
            "st_mtime": unixtimestamp(entry.get_si_modified_timestamp()),
            "st_size": entry.get_size(),
            "st_uid": uid,
            "st_gid": gid,
            "st_mode": mode,
            "st_nlink": nlink,
        }

    @log
    def readdir(self, path, fh):
        dirents = ['.', '..']
        entry = self._get_path_entry(path)

        dirents.extend(map(lambda r: r.get_name(), entry.get_children()))
        return dirents

    @log
    def readlink(self, path):
        return path

    @log
    def statfs(self, path):
        return dict((key, 0) for key in ('f_bavail', 'f_bfree',
                                         'f_blocks', 'f_bsize', 'f_favail',
                                         'f_ffree', 'f_files', 'f_flag',
                                         'f_frsize', 'f_namemax'))

    @log
    def chmod(self, path, mode):
        return errno.EROFS

    @log
    def chown(self, path, uid, gid):
        return errno.EROFS

    @log
    def mknod(self, path, mode, dev):
        return errno.EROFS

    @log
    def rmdir(self, path):
        return errno.EROFS

    @log
    def mkdir(self, path, mode):
        return errno.EROFS

    @log
    def unlink(self, path):
        return errno.EROFS

    @log
    def symlink(self, target, name):
        return errno.EROFS

    @log
    def rename(self, old, new):
        return errno.EROFS

    @log
    def link(self, target, name):
        return errno.EROFS

    @log
    def utimens(self, path, times=None):
        return errno.EROFS

    # File methods
    # ============

    def _get_available_fh(self):
        """
        _get_available_fh returns an unused fh
        The caller must be careful to handle race conditions.
        @rtype: int
        """
        for i in xrange(65534):
            if i not in self._opened_files:
                return i

    @log
    def open(self, path, flags):
        if flags & os.O_WRONLY > 0:
            return errno.EROFS
        if flags & os.O_RDWR > 0:
            return errno.EROFS

        entry = self._get_path_entry(path)

        # TODO(wb): race here on fh used/unused
        fh = self._get_available_fh()
        self._opened_files[fh] = entry

        return fh

    @log
    def read(self, path, length, offset, fh):
        entry = self._opened_files[fh]
        return entry.read(offset, length)

    @log
    def flush(self, path, fh):
        return ""

    @log
    def release(self, path, fh):
        del self._opened_files[fh]

    @log
    def create(self, path, mode, fi=None):
        return errno.EROFS

    @log
    def write(self, path, buf, offset, fh):
        return errno.EROFS

    @log
    def truncate(self, path, length, fh=None):
        return errno.EROFS

    @log
    def fsync(self, path, fdatasync, fh):
        return errno.EPERM


def main(image_filename, volume_offset, mountpoint):
    from ntfs.volume import FlatVolume
    from ntfs.BinaryParser import Mmap

    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger("ntfs.mft").setLevel(logging.INFO)

    with Mmap(image_filename) as buf:
        v = FlatVolume(buf, volume_offset)
        fs = NTFSFilesystem(v)
        handler = NTFSFuseOperations(fs)
        FUSE(handler, mountpoint, foreground=True)


if __name__ == '__main__':
    import sys
    main(sys.argv[1], int(sys.argv[2]), sys.argv[3])

