import logging

from ntfs.BinaryParser import hex_dump
from ntfs.BinaryParser import Block
from ntfs.mft.MFT import MFTRecord
from ntfs.mft.MFT import MFT_RECORD_SIZE


g_logger = logging.getLogger("ntfs.filesystem")


class File(object):
    """
    interface
    """
    pass


class Directory(object):
    """
    interface
    """
    pass


class Filesystem(object):
    """
    interface
    """

class NtfsVBR(Block):
    def __init__(self, volume):
        super(NtfsVBR, self).__init__(volume, 0)
        self.declare_field("byte", "jump", offset=0x0, count=3)
        self.declare_field("qword", "oem_id")
        self.declare_field("word", "bytes_per_sector")
        self.declare_field("byte", "sectors_per_cluster")
        self.declare_field("word", "reserved_sectors")
        self.declare_field("byte", "zero0", count=3)
        self.declare_field("word", "unused0")
        self.declare_field("byte", "media_descriptor")
        self.declare_field("word", "zero1")
        self.declare_field("word", "sectors_per_track")
        self.declare_field("word", "number_of_heads")
        self.declare_field("dword", "hidden_sectors")
        self.declare_field("dword", "unused1")
        self.declare_field("dword", "unused2")
        self.declare_field("qword", "total_sectors")
        self.declare_field("qword", "mft_lcn")
        self.declare_field("qword", "mftmirr_lcn")
        self.declare_field("dword", "clusters_per_file_record_segment")
        self.declare_field("byte", "clusters_per_index_buffer")
        self.declare_field("byte", "unused3", count=3)
        self.declare_field("qword", "volume_serial_number")
        self.declare_field("dword", "checksum")
        self.declare_field("byte", "bootstrap_code", count=426)
        self.declare_field("word", "end_of_sector")


class ClusterAccessor(object):
    """
    index volume data using `cluster_size` units
    """
    def __init__(self, volume, cluster_size):
        super(ClusterAccessor, self).__init__()
        self._volume = volume
        self._cluster_size = cluster_size

    def __getitem__(self, index):
        return self._volume[index * self._cluster_size]

    def __getslice__(self, start, end):
        return self._volume[start * self._cluster_size:end * self._cluster_size]

    def __len__(self):
        return len(self._volume) / self._cluster_size


MFT_INODE = 5
MFT_CLUSTER_OFFSET = 0x30


class NtfsFilesystem(object):
    def __init__(self, volume, cluster_size=4096):
        super(NtfsFilesystem, self).__init__()
        self._volume = volume
        self._cluster_size = cluster_size
        self._vbr = NtfsVBR(self._volume)


        self._logger = logging.getLogger("NtfsFilesystem")
        self._logger.debug("bps: %s", hex(self._vbr.bytes_per_sector))
        self._logger.debug("spc: %s", hex(self._vbr.sectors_per_cluster))

        self._clusters = ClusterAccessor(self._volume, self._cluster_size)

    def get_mft_buf(self):
        mft_chunk = self._clusters[MFT_CLUSTER_OFFSET:MFT_CLUSTER_OFFSET + 2]
        #mft_mft_record = MFTRecord(mft_chunk, MFT_INODE * MFT_RECORD_SIZE, None, inode=MFT_INODE)
        self._logger.debug("\n%s", hex_dump(mft_chunk[MFT_INODE * MFT_RECORD_SIZE:(MFT_INODE + 1) * MFT_RECORD_SIZE]))


def main():
    import sys
    from ntfs.volume import FlatVolume
    from ntfs.BinaryParser import Mmap
    logging.basicConfig(level=logging.DEBUG)

    with Mmap(sys.argv[1]) as buf:
        v = FlatVolume(buf, int(sys.argv[2]))
        fs = NtfsFilesystem(v)
        fs.get_mft_buf()



if __name__ == "__main__":
    main()
