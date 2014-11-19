"""
Dump the NTFS VBR for a volume.
"""
import logging

from ntfs.volume import FlatVolume
from ntfs.BinaryParser import Mmap
from ntfs.filesystem import NTFSVBR


g_logger = logging.getLogger("ntfs.examples.inspect_vbr")


def main(image_filename, volume_offset):
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger("ntfs.mft").setLevel(logging.INFO)

    with Mmap(image_filename) as buf:
        v = FlatVolume(buf, volume_offset)
        vbr = NTFSVBR(v)
        print(vbr.get_all_string())


if __name__ == '__main__':
    import sys
    main(sys.argv[1], int(sys.argv[2]))

