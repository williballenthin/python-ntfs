"""
Dump stuff related to a single record.
"""
import logging

from ntfs.BinaryParser import Mmap
from ntfs.mft.MFT import MFTRecord
from ntfs.mft.MFT import Attribute
from ntfs.mft.MFT import ATTR_TYPE
from ntfs.mft.MFT import StandardInformation
from ntfs.mft.MFT import FilenameAttribute


g_logger = logging.getLogger("ntfs.examples.inspect_record")


def main(record_filename):
    logging.basicConfig(level=logging.DEBUG)
    #logging.getLogger("ntfs.mft").setLevel(logging.INFO)

    with Mmap(record_filename) as buf:
        record = MFTRecord(buf, 0, None)
        print("=== MFT Record Header")
        print(record.get_all_string())

        for attribute in record.attributes():
            print("=== Attribute Header (type: {:s}) at offset {:s}".format(
                Attribute.TYPES[attribute.type()],
                hex(attribute.offset())))
            print(attribute.get_all_string())

            if attribute.type() == ATTR_TYPE.STANDARD_INFORMATION:
                print("=== STANDARD INFORMATION value")
                si = StandardInformation(attribute.value(), 0, None)
                print(si.get_all_string())

            elif attribute.type() == ATTR_TYPE.FILENAME_INFORMATION:
                print("=== FILENAME INFORMATION value")
                fn = FilenameAttribute(attribute.value(), 0, None)
                print(fn.get_all_string())

if __name__ == '__main__':
    import sys
    main(sys.argv[1])

