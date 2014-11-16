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


g_logger = logging.getLogger("ntfs.examples.inspect_directory")


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

        if not entry.is_directory():
            g_logger.error("not a directory")
            return

        # sorry, reaching
        record = entry._record

        entries = {}
        try:
            indx_alloc_attr = record.attribute(ATTR_TYPE.INDEX_ALLOCATION)
            indx_alloc = INDEX_ALLOCATION(fs.get_attribute_data(indx_alloc_attr), 0)
            g_logger.debug("INDEX_ALLOCATION len: %s", hex(len(indx_alloc)))
            g_logger.debug("alloc:\n%s", indx_alloc.get_all_string(indent=2))
            indx = indx_alloc

            g_logger.info("found:")
            for block in indx.blocks():
                for entry in block.index().entries():
                    ref = MREF(entry.header().mft_reference())
                    entries[ref] = entry.filename_information().filename()

        except AttributeNotFoundError:
            indx_root_attr = record.attribute(ATTR_TYPE.INDEX_ROOT)
            indx_root = INDEX_ROOT(fs.get_attribute_data(indx_root_attr), 0)
            g_logger.debug("INDEX_ROOT len: %s", hex(len(indx_root)))
            g_logger.debug("root:\n%s", indx_root.get_all_string(indent=2))
            indx = indx_root

            g_logger.info("found:")
            for entry in indx.index().entries():
                ref = MREF(entry.header().mft_reference())
                entries[ref] = entry.filename_information().filename()

        for k, v in entries.iteritems():
            g_logger.info("  - %s", v)


if __name__ == '__main__':
    import sys
    main(sys.argv[1], int(sys.argv[2]), sys.argv[3])

