"""
Dump stuff related to a single record.
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


g_logger = logging.getLogger("ntfs.examples.inspect_record")


def main(image_filename, volume_offset, record_number):
    logging.basicConfig(level=logging.DEBUG)
    #logging.getLogger("ntfs.mft").setLevel(logging.INFO)

    with Mmap(image_filename) as buf:
        v = FlatVolume(buf, volume_offset)
        fs = NTFSFilesystem(v)
        record = fs.get_record(record_number)
        print(record.get_all_string())


if __name__ == '__main__':
    import sys
    main(sys.argv[1], int(sys.argv[2]), int(sys.argv[3]))

