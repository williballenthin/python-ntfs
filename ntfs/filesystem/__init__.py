import sys
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
    pass


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
        return self._volume[index * self._cluster_size:(index + 1) * self._cluster_size]

    def __getslice__(self, start, end):
        return self._volume[start * self._cluster_size:end * self._cluster_size]

    def __len__(self):
        return len(self._volume) / self._cluster_size

    def get_cluster_size(self):
        return self._cluster_size


INODE_MFT = 0
INODE_MFTMIRR = 1


class CorruptNtfsFilesystemError(Exception):
    def __init__(self, msg="no details"):
        super(CorruptNtfsFilesystemError, self).__init__(self)
        self._msg = msg

    def __str__(self):
        return "%s(%s)" % (self.__class__.__name__, self._msg)


class MFTDataIsResidentError(CorruptNtfsFilesystemError):
    pass


class NonResidentAttributeData(object):
    """
    expose a potentially non-continuous set of data runs as a single
      logical buffer

    implementation note: this is likely a good place to optimize
    """
    __unpackable__ = True
    def __init__(self, clusters, runlist):
        self._clusters = clusters
        self._runlist = runlist
        self._runentries = [(a, b) for a, b in self._runlist.runs()]

    def __getitem__(self, index):
        current_run_start_offset = 0
        for cluster_offset, num_clusters in self._runentries:
            run_length = num_clusters * self._clusters.get_cluster_size()

            if current_run_start_offset <= index < current_run_start_offset + run_length:
                i = index - current_run_start_offset
                cluster = self._clusters[cluster_offset]
                return cluster[i]

            current_run_start_offset += run_length
        raise IndexError("%d is greater than the non resident attribute data length %s" %
            (index, len(self)))

    def __getslice__(self, start, end):
        """
        TODO: there are some pretty bad inefficiencies here, i believe
        """
        ret = []
        current_run_start_offset = 0
        have_found_start = False

        if end == sys.maxint:
            end = len(self)

        if max(start, end) > len(self):
             raise IndexError("(%d, %d) is greater than the non resident attribute data length %s" %
                (start, end, len(self)))

        for cluster_offset, num_clusters in self._runentries:
            run_length = num_clusters * self._clusters.get_cluster_size()

            if not have_found_start:
                if current_run_start_offset <= start < current_run_start_offset + run_length:
                    if end <= current_run_start_offset + run_length:
                        # everything is in this run
                        i = start - current_run_start_offset
                        j = end - current_run_start_offset
                        cluster = self._clusters[cluster_offset:cluster_offset + num_clusters]
                        return cluster[i:j]
                    else:
                        # starts in this cluster, continues on to other clusters
                        i = start - current_run_start_offset
                        cluster = self._clusters[cluster_offset]
                        ret.append(cluster[i:])
            else:  # have found start
                if current_run_start_offset <= end < current_run_start_offset + run_length:
                    j = end - current_run_start_offset
                    cluster = self._clusters[cluster_offset:cluster_offset + num_clusters]
                    ret.append(cluster[:j])
                    return "".join(ret)
                else:
                    cluster = self._clusters[cluster_offset]
                    ret.append(cluster)

            current_run_start_offset += run_length
        return "".join(ret)

    def __len__(self):
        ret = 0
        for _, num_clusters in self._runentries:
            ret += num_clusters * self._clusters.get_cluster_size()
        return ret


class NtfsFilesystem(object):
    def __init__(self, volume, cluster_size=None):
        super(NtfsFilesystem, self).__init__()
        self._volume = volume
        self._cluster_size = cluster_size
        self._vbr = NtfsVBR(self._volume)
        if cluster_size is not None:
            self._cluster_size = cluster_size
        else:
            self._cluster_size = self._vbr.bytes_per_sector() * self._vbr.sectors_per_cluster()

        self._clusters = ClusterAccessor(self._volume, self._cluster_size)
        self._logger = logging.getLogger("NtfsFilesystem")

    def get_mft_buf(self):
        mft_chunk = self._clusters[self._vbr.mft_lcn()]
        mft_mft_record = MFTRecord(mft_chunk, 0, None)
        mft_data_attribute = mft_mft_record.data_attribute()
        if mft_data_attribute.non_resident() == 0:
            raise MFTDataIsResidentError()

        mft_data = NonResidentAttributeData(self._clusters, mft_data_attribute.runlist())
        return mft_data


def main():
    import sys
    from ntfs.volume import FlatVolume
    from ntfs.BinaryParser import Mmap
    from ntfs.mft.MFT import MFTEnumerator
    logging.basicConfig(level=logging.DEBUG)

    with Mmap(sys.argv[1]) as buf:
        v = FlatVolume(buf, int(sys.argv[2]))
        fs = NtfsFilesystem(v)
        # note optimization: copy entire mft buffer from NonResidentNtfsAttribute
        #  to avoid getslice lookups
        mft_data = fs.get_mft_buf()[:]
        enum = MFTEnumerator(mft_data)
        for record, path in enum.enumerate_paths():
            print(path)
            g_logger.debug("%s", path)




if __name__ == "__main__":
    main()
