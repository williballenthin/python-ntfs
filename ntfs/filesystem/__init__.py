import sys
import logging

from ntfs.BinaryParser import hex_dump
from ntfs.BinaryParser import Block
from ntfs.mft.MFT import MREF
from ntfs.mft.MFT import MSEQNO
from ntfs.mft.MFT import MFTRecord
from ntfs.mft.MFT import ATTR_TYPE
from ntfs.mft.MFT import INDEX_ROOT
from ntfs.mft.MFT import MFTEnumerator
from ntfs.mft.MFT import MFT_RECORD_SIZE
from ntfs.mft.MFT import INDEX_ALLOCATION


g_logger = logging.getLogger("ntfs.filesystem")


class FileSystemError(Exception):
    def __init__(self, msg="no details"):
        super(FileSystemError, self).__init__(self)
        self._msg = msg

    def __str__(self):
        return "%s(%s)" % (self.__class__.__name__, self._msg)


class CorruptNTFSFilesystemError(FileSystemError):
    pass


class NoParentError(FileSystemError):
    pass


class File(object):
    """
    interface
    """
    def get_name(self):
        raise NotImplementedError()

    def get_parent_directory(self):
        """
        @raise NoParentError:
        """
        raise NotImplementedError()


class NTFSFileMetadataMixin(object):
    def __init__(self, record):
        self._record = record

    def get_si_birth_timestamp(self):
        pass

    # etc



class NTFSFile(File, NTFSFileMetadataMixin):
    def __init__(self, filesystem, mft_record):
        File.__init__(self)
        NTFSFileMetadataMixin.__init__(self, mft_record)
        self._fs = filesystem
        self._record = mft_record

    def get_name(self):
        return self._record.filename_information().filename()

    def get_parent_directory(self):
        return self._fs.get_record_parent(self._record)

    def __str__(self):
        return "File(name: %s)" % (self.get_name())


class ChildNotFoundError(Exception):
    pass


class Directory(object):
    """
    interface
    """
    def get_name(self):
        raise NotImplementedError()

    def get_children(self):
        raise NotImplementedError()

    def get_files(self):
        raise NotImplementedError()

    def get_directories(self):
        raise NotImplementedError()

    def get_parent_directory(self):
        """
        @raise NoParentError:
        """
        raise NotImplementedError()

    def get_child(self, name):
        """
        @raise ChildNotFoundError: if the given filename is not found.
        """
        name_lower = name.lower()
        for child in self.get_children():
            if name_lower == child.get_name().lower():
                return child
        raise ChildNotFoundError()


class NTFSDirectory(Directory, NTFSFileMetadataMixin):
    def __init__(self, filesystem, mft_record):
        Directory.__init__(self)
        NTFSFileMetadataMixin.__init__(self, mft_record)
        self._fs = filesystem
        self._record = mft_record

    def get_name(self):
        return self._record.filename_information().filename()

    def get_children(self):
        ret = []
        for child in self._fs.get_record_children(self._record):
            if child.is_directory():
                ret.append(NTFSDirectory(self._fs, child))
            else:
                ret.append(NTFSFile(self._fs, child))
        return ret

    def get_files(self):
        return filter(lambda c: isinstance(c, NTFSFile),
                      self.get_children())

    def get_directories(self):
        return filter(lambda c: isinstance(c, NTFSDirectory),
                      self.get_children())

    def get_parent_directory(self):
        return self._fs.get_record_parent(self._record)

    def __str__(self):
        return "Directory(name: %s)" % (self.get_name())


class Filesystem(object):
    """
    interface
    """
    def get_root_directory(self):
        raise NotImplementedError()


class NTFSVBR(Block):
    def __init__(self, volume):
        super(NTFSVBR, self).__init__(volume, 0)
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
INODE_ROOT = 5


class NonResidentAttributeData(object):
    """
    expose a potentially non-continuous set of data runs as a single
      logical buffer

    once constructed, use this like a bytestring.
    you can unpack from it, slice it, etc.

    implementation note: this is likely a good place to optimize
    """
    __unpackable__ = True
    def __init__(self, clusters, runlist):
        self._clusters = clusters
        self._runlist = runlist
        self._runentries = [(a, b) for a, b in self._runlist.runs()]

    def __getitem__(self, index):
        # TODO: clarify variable names and their units
        # units: bytes
        current_run_start_offset = 0

        # units: clusters
        for cluster_offset, num_clusters in self._runentries:
            # units: bytes
            run_length = num_clusters * self._clusters.get_cluster_size()

            if current_run_start_offset <= index < current_run_start_offset + run_length:
                # units: bytes
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
        # TODO: clarify variable names and their units
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


class NTFSFilesystem(object):
    def __init__(self, volume, cluster_size=None):
        super(NTFSFilesystem, self).__init__()
        self._volume = volume
        self._cluster_size = cluster_size
        self._vbr = NTFSVBR(self._volume)
        if cluster_size is not None:
            self._cluster_size = cluster_size
        else:
            self._cluster_size = self._vbr.bytes_per_sector() * \
                                   self._vbr.sectors_per_cluster()

        self._clusters = ClusterAccessor(self._volume, self._cluster_size)
        self._logger = logging.getLogger("NTFSFilesystem")

        # balance memory usage with performance
        b = self.get_mft_buffer()[:]
        if len(b) > 1024 * 1024 * 500:
            self._mft_data = b
        else:
            # note optimization: copy entire mft buffer from NonResidentNTFSAttribute
            #  to avoid getslice lookups
            self._mft_data = b[:]
        self._enumerator = MFTEnumerator(self._mft_data)

    def get_attribute_data(self, attribute):
        if attribute.non_resident() == 0:
            return attribute.value()
        else:
            return NonResidentAttributeData(self._clusters, attribute.runlist())

    def get_mft_buffer(self):
        g_logger.debug("mft: %s", hex(self._vbr.mft_lcn()))
        mft_chunk = self._clusters[self._vbr.mft_lcn()]
        mft_mft_record = MFTRecord(mft_chunk, 0, None)
        mft_data_attribute = mft_mft_record.data_attribute()
        return self.get_attribute_data(mft_data_attribute)

    def get_root_directory(self):
        return NTFSDirectory(self, self._enumerator.get_record(INODE_ROOT))

    def get_record_parent(self, record):
        """
        @raises NoParentError: on various error conditions
        """
        if record.mft_record_number() == 5:
            raise NoParentError("Root directory has no parent")

        fn = record.filename_information()
        if not fn:
            raise NoParentError("File has no filename attribute")

        parent_record_num = MREF(fn.mft_parent_reference())
        parent_seq_num = MSEQNO(fn.mft_parent_reference())

        try:
            parent_record = self._enumerator.get_record(parent_record_num)
        except (BinaryParser.OverrunBufferException, InvalidRecordException):
            raise NoParentError("Invalid parent MFT record")

        if parent_record.sequence_number() != parent_seq_num:
            raise NoParentError("Invalid parent MFT record (bad sequence number)")

        return NTFSDirectory(self, parent_record)

    def get_record_children(self, record):
        ret = []
        if not record.is_directory():
            return ret

        try:
            indx_alloc_attr = record.attribute(ATTR_TYPE.INDEX_ALLOCATION)
            indx_alloc = INDEX_ALLOCATION(self.get_attribute_data(indx_alloc_attr), 0)
            indx = indx_alloc
            # TODO: i'm not sure we're parsing all blocks here
        except AttributeNotFoundError:
            indx_root_attr = record.attribute(ATTR_TYPE.INDEX_ROOT)
            indx_root = INDEX_ROOT(self.get_attribute_data(indx_root_attr), 0)
            indx = indx_root

        for entry in indx.index().entries():
            ref = MREF(entry.header().mft_reference())
            if ref == INODE_ROOT and entry.filename_information().filename() == ".":
                continue
            ret.append(self._enumerator.get_record(ref))

        return ret


def main():
    import sys
    from ntfs.volume import FlatVolume
    from ntfs.BinaryParser import Mmap
    from ntfs.mft.MFT import MFTEnumerator
    logging.basicConfig(level=logging.DEBUG)

    with Mmap(sys.argv[1]) as buf:
        v = FlatVolume(buf, int(sys.argv[2]))
        fs = NTFSFilesystem(v)
        root = fs.get_root_directory()
        g_logger.debug("root dir: %s", root)
        for c in root.get_children():
            g_logger.debug("  - %s", c.get_name())



if __name__ == "__main__":
    main()
