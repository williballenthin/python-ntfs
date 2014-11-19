"""
Dump the directory index for a directory.
"""
import logging

from ntfs.volume import FlatVolume
from ntfs.BinaryParser import Mmap
from ntfs.filesystem import NTFSFilesystem
from ntfs.mft.MFT import AttributeNotFoundError
from ntfs.mft.MFT import ATTR_TYPE
from ntfs.mft.MFT import MREF
from ntfs.mft.MFT import INDEX_ALLOCATION
from ntfs.mft.MFT import INDEX_ROOT


g_logger = logging.getLogger("ntfs.examples.indxparse")


class InvalidArgumentError(Exception):
    pass


def get_directory_index_active_entries(fs, directory):
    """
    get the active MFT_INDEX_ENTRYs from a directory's
    INDEX_ROOT and INDEX_ALLOCATION attributes
    """
    if not directory.is_directory():
        raise InvalidArgumentError()

    # sorry, reaching
    record = directory._record

    ret = []

    try:
        indx_alloc_attr = record.attribute(ATTR_TYPE.INDEX_ALLOCATION)
        indx_alloc = INDEX_ALLOCATION(fs.get_attribute_data(indx_alloc_attr), 0)
        for block in indx_alloc.blocks():
            for entry in block.index().entries():
                ret.append(entry)
    except AttributeNotFoundError:
        pass

    try:
        indx_root_attr = record.attribute(ATTR_TYPE.INDEX_ROOT)
        indx_root = INDEX_ROOT(fs.get_attribute_data(indx_root_attr), 0)
        for entry in indx_root.index().entries():
            ret.append(entry)
    except AttributeNotFoundError:
        pass

    return ret


def get_directory_index_inactive_entries(fs, directory):
    """
    get the inactive (slack) MFT_INDEX_ENTRYs from a directory's
    INDEX_ROOT and INDEX_ALLOCATION attributes
    """
    if not directory.is_directory():
        raise InvalidArgumentError()

    # sorry, reaching
    record = directory._record

    ret = []

    try:
        indx_alloc_attr = record.attribute(ATTR_TYPE.INDEX_ALLOCATION)
        indx_alloc = INDEX_ALLOCATION(fs.get_attribute_data(indx_alloc_attr), 0)
        for block in indx_alloc.blocks():
            for entry in block.index().slack_entries():
                ret.append(entry)
    except AttributeNotFoundError:
        pass

    try:
        indx_root_attr = record.attribute(ATTR_TYPE.INDEX_ROOT)
        indx_root = INDEX_ROOT(fs.get_attribute_data(indx_root_attr), 0)
        for entry in indx_root.index().slack_entries():
            ret.append(entry)
    except AttributeNotFoundError:
        pass

    return ret


def make_dump_directory_indices_visitor(formatter):
    """
    `formatter` is a function that accepts a dict, and returns a string.
    the string is dumped via print().
    the schema for the dict is:
      active: bool
      path: str
      entry: MFT_INDEX_ENTRY

    this function returns a function that applies the format to the
      given FileSystem and Directory and dumps it out.
    """
    def dump_directory_indices_visitor(fs, directory):
        for e in get_directory_index_active_entries(fs, directory):
            print(formatter({
                "active": True,
                "path": directory.get_full_path(),
                "entry": e}))
        for e in get_directory_index_inactive_entries(fs, directory):
            print(formatter({
                "active": False,
                "path": directory.get_full_path(),
                "entry": e}))
    return dump_directory_indices_visitor


def walk_directories(fs, directory, visitor):
    """
    `visitor` is a function that accepts two parameters: a FileSystem
      and a Directory

    this function applies the function `visitor` to each directory
      in the file system recursively.
    """
    visitor(fs, directory)
    for d in directory.get_directories():
        walk_directories(fs, d, visitor)


def safe_date(f):
    try:
        return f()
    except ValueError:
        return datetime(1970, 1, 1, 0, 0, 0)


def csv_directory_index_formatter(e):
    entry = e["entry"].filename_information()
    fn = entry.filename()
    if e["active"]:
        f = u"active,{path},{filename},{physical_size},{logical_size},{mtime},{atime},{ctime},{crtime}"
    else:
        f = u"slack,{path},{filename},{physical_size},{logical_size},{mtime},{atime},{ctime},{crtime}"

    return f.format(
        path=e["path"],
        filename=entry.filename(),
        physical_size=entry.physical_size(),
        logical_size=entry.logical_size(),
        mtime=safe_date(entry.modified_time),
        atime=safe_date(entry.accessed_time),
        ctime=safe_date(entry.changed_time),
        crtime=safe_date(entry.created_time))


def bodyfile_directory_index_formatter(e):
    pass


def main(image_filename, volume_offset, path):
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger("ntfs.mft").setLevel(logging.INFO)

    with Mmap(image_filename) as buf:
        v = FlatVolume(buf, volume_offset)
        fs = NTFSFilesystem(v)
        root = fs.get_root_directory()

        if path == "/":
            entry = root
        else:
            entry = root.get_path_entry(path)

        v = make_dump_directory_indices_visitor(csv_directory_index_formatter)
        v(fs, entry)

if __name__ == '__main__':
    import sys
    main(sys.argv[1], int(sys.argv[2]), sys.argv[3])

