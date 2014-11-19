import sys
import logging

from ntfs.BinaryParser import hex_dump
from ntfs.BinaryParser import Block
from ntfs.BinaryParser import OverrunBufferException
from ntfs.mft.MFT import MREF
from ntfs.mft.MFT import MSEQNO
from ntfs.mft.MFT import MFTRecord
from ntfs.mft.MFT import ATTR_TYPE
from ntfs.mft.MFT import INDEX_ROOT
from ntfs.mft.MFT import MFTEnumerator
from ntfs.mft.MFT import MFT_RECORD_SIZE
from ntfs.mft.MFT import INDEX_ALLOCATION
from ntfs.mft.MFT import AttributeNotFoundError


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


class UnsupportedPathError(FileSystemError):
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

    def read(self, offset, length):
        raise NotImplementedError()

    def get_full_path(self):
        raise NotImplementedError()


class NTFSFileMetadataMixin(object):
    def __init__(self, record):
        self._record = record

    def get_filenames(self):
        ret = []
        for fn in self._record.filename_informations():
            ret.append(fn.filename())
        return ret

    def get_si_created_timestamp(self):
        return self._record.standard_information().created_time()

    def get_si_accessed_timestamp(self):
        return self._record.standard_information().accessed_time()

    def get_si_changed_timestamp(self):
        return self._record.standard_information().changed_time()

    def get_si_modified_timestamp(self):
        return self._record.standard_information().modified_time()

    def get_fn_created_timestamp(self):
        return self._record.filename_information().created_time()

    def get_fn_accessed_timestamp(self):
        return self._record.filename_information().accessed_time()

    def get_fn_changed_timestamp(self):
        return self._record.filename_information().changed_time()

    def get_fn_modified_timestamp(self):
        return self._record.filename_information().modified_time()

    def is_file(self):
        return self._record.is_file()

    def is_directory(self):
        return self._record.is_directory()

    def get_size(self):
        if self.is_directory():
            return 0
        else:
            data_attribute = self._record.data_attribute()
            if data_attribute is not None:
                if data_attribute.non_resident() == 0:
                    size = len(data_attribute.value())
                else:
                    size = data_attribute.data_size()
            else:
                size = self._record.filename_information().logical_size()
        return size


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

    def read(self, offset, length):
        data_attribute = self._record.data_attribute()
        data = self._fs.get_attribute_data(data_attribute)
        return data[offset:offset+length]

    def get_full_path(self):
        return self._fs.get_record_path(self._record)


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
        raise NotImplementedError()

    def get_full_path(self):
        raise NotImplementedError()


class PathDoesNotExistError(Exception):
    pass


class DirectoryDoesNotExistError(PathDoesNotExistError):
    pass


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

    def get_child(self, name):
        name_lower = name.lower()
        for child in self.get_children():
            if len(child.get_filenames()) > 1:
                g_logger.debug("file names: %s -> %s",
                  child.get_name(), child.get_filenames())
            for fn in child.get_filenames():
                if name_lower == fn.lower():
                    return child
        raise ChildNotFoundError()

    def _split_path(self, path):
        """
        Hack to try to support both types of file system paths:
          - forward slash, /etc
          - backslash, C:\windows\system32

        Linux uses forward slashes, so we'd like that when working with FUSE.
        The original file system used backslashes, so we'd also like that.

        This is a poor attempt at doing both:
          - detect which slash type is in use
          - don't support both at the same time

        This works like string.partition(PATH_SEPARATOR)
        """
        if "\\" in path:
            if "/" in path:
                raise UnsupportedPathError(path)
            return path.partition("\\")

        elif "/" in path:
            if "\\" in path:
                raise UnsupportedPathError(path)
            return path.partition("/")
        else:
            return path, "", ""

    def get_path_entry(self, path):
        g_logger.debug("get_path_entry: path: %s", path)
        imm, slash, rest = self._split_path(path)
        if slash == "":
            return self.get_child(path)
        else:
            if rest == "":
                return self

            child = self.get_child(imm)
            if not isinstance(child, NTFSDirectory):
                raise DirectoryDoesNotExistError()

            return child.get_path_entry(rest)

    def get_full_path(self):
        return self._fs.get_record_path(self._record)


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
INODE_LOGFILE = 2
INODE_VOLUME = 3
INODE_ATTR_DEF = 4
INODE_ROOT = 5
INODE_BITMAP = 6
INODE_BOOT = 7
INODE_BADCLUS = 8
INODE_SECURE = 9
INODE_UPCASE = 10
INODE_EXTEND = 11
INODE_RESERVED0 = 12
INODE_RESERVED1 = 13
INODE_RESERVED2 = 14
INODE_RESERVED3 = 15
INODE_FIRST_USER = 16


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
        self._len = None

    def __getitem__(self, index):
        # TODO: clarify variable names and their units
        # units: bytes
        current_run_start_offset = 0

        if index < 0:
            index = len(self) + index

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

        g_logger.debug("NonResidentAttributeData: getslice: start: %s end: %s", hex(start), hex(end))

        if end == sys.maxint:
            end = len(self)

        if end < 0:
            end = len(self) + end

        if start < 0:
            start = len(self) + start

        if max(start, end) > len(self):
             raise IndexError("(%d, %d) is greater than the non resident attribute data length %s" %
                (start, end, len(self)))

        for cluster_offset, num_clusters in self._runentries:
            g_logger.debug("NonResidentAttributeData: getslice: runentry: start: %s len: %s",
                    hex(cluster_offset * self._clusters.get_cluster_size()),
                    hex(num_clusters * self._clusters.get_cluster_size()))
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
        if self._len is not None:
            return self._len
        ret = 0
        for cluster_start, num_clusters in self._runentries:
            g_logger.debug("NonResidentAttributeData: len: run: cluster: %s len: %s", hex(cluster_start), hex(num_clusters))
            ret += num_clusters * self._clusters.get_cluster_size()
        self._len = ret
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
        try:
            b = self.get_mft_buffer()

            # test we can access last MFT byte, demonstrating we can
            #   reach all runs
            _ = b[-1]
        except OverrunBufferException as e:
            g_logger.warning("failed to read MFT from image, will fall back to MFTMirr: %s", e)
            try:
                b = self.get_mftmirr_buffer()

                # test we can access last MFTMirr byte, demonstrating
                #   we can reach all runs
                _ = b[-1]
            except OverrunBufferException as e:
                g_logger.error("failed to read MFTMirr from image: %s", e)
                raise CorruptNTFSFilesystemError("failed to read MFT or MFTMirr from image")

        if len(b) > 1024 * 1024 * 500:
            self._mft_data = b
        else:
            # note optimization: copy entire mft buffer from NonResidentNTFSAttribute
            #  to avoid getslice lookups
            self._mft_data = b[:]
        self._enumerator = MFTEnumerator(self._mft_data)

        # test there's at least some user content (aside from root), or we'll
        #   assume something's up
        try:
            _ = self.get_record(INODE_FIRST_USER)
        except OverrunBufferException:
            g_logger.error("overrun reading first user MFT record")
            raise CorruptNTFSFilesystemError("failed to read first user record (MFT not large enough)")

    def get_attribute_data(self, attribute):
        if attribute.non_resident() == 0:
            return attribute.value()
        else:
            return NonResidentAttributeData(self._clusters, attribute.runlist())

    def get_mft_buffer(self):
        g_logger.debug("mft: %s", hex(self._vbr.mft_lcn() * 4096))
        mft_chunk = self._clusters[self._vbr.mft_lcn()]
        mft_mft_record = MFTRecord(mft_chunk, 0, None, inode=INODE_MFT)
        mft_data_attribute = mft_mft_record.data_attribute()
        return self.get_attribute_data(mft_data_attribute)

    def get_mftmirr_buffer(self):
        g_logger.debug("mft mirr: %s", hex(self._vbr.mftmirr_lcn() * 4096))
        mftmirr_chunk = self._clusters[self._vbr.mftmirr_lcn()]
        mftmirr_mft_record = MFTRecord(mftmirr_chunk, INODE_MFTMIRR * MFT_RECORD_SIZE, None, inode=INODE_MFTMIRR)
        mftmirr_data_attribute = mftmirr_mft_record.data_attribute()
        return self.get_attribute_data(mftmirr_data_attribute)

    def get_root_directory(self):
        return NTFSDirectory(self, self._enumerator.get_record(INODE_ROOT))

    def get_record(self, record_number):
        g_logger.debug("get_record: %d", record_number)
        return self._enumerator.get_record(record_number)

    def get_record_path(self, record):
        return self._enumerator.get_path(record)

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
        # we use a map here to de-dup entries with different filename types
        #  such as 8.3, POSIX, or Windows,  but the same ultimate MFT reference
        ret = {}  # type: dict(int, MFTRecord)
        if not record.is_directory():
            return ret.values()

        # TODO: cleanup the duplication here
        try:
            indx_alloc_attr = record.attribute(ATTR_TYPE.INDEX_ALLOCATION)
            indx_alloc = INDEX_ALLOCATION(self.get_attribute_data(indx_alloc_attr), 0)
            #g_logger.debug("INDEX_ALLOCATION len: %s", hex(len(indx_alloc)))
            #g_logger.debug("alloc:\n%s", indx_alloc.get_all_string(indent=2))
            indx = indx_alloc

            for block in indx.blocks():
                for entry in block.index().entries():
                    ref = MREF(entry.header().mft_reference())
                    if ref == INODE_ROOT and \
                       entry.filename_information().filename() == ".":
                        continue
                    ret[ref] = self._enumerator.get_record(ref)

        except AttributeNotFoundError:
            indx_root_attr = record.attribute(ATTR_TYPE.INDEX_ROOT)
            indx_root = INDEX_ROOT(self.get_attribute_data(indx_root_attr), 0)
            indx = indx_root

            for entry in indx.index().entries():
                ref = MREF(entry.header().mft_reference())
                if ref == INODE_ROOT and \
                   entry.filename_information().filename() == ".":
                    continue
                ret[ref] = self._enumerator.get_record(ref)

        return ret.values()


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
        g_logger.info("root dir: %s", root)
        for c in root.get_children():
            g_logger.info("  - %s", c.get_name())

        sys32 = root.get_path_entry("windows\\system32")
        g_logger.info("sys32: %s", sys32)



if __name__ == "__main__":
    main()
