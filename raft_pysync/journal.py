import os
import mmap
import struct
import shutil
from abc import ABC, abstractmethod
from .version import VERSION
from .pickle import to_bytes, loads, dumps


class Journal(ABC):
    """
    Base class for journal implementations.
    """

    @abstractmethod
    def add(self, command, idx, term):
        """
        Add a new entry to the journal.

        Args:
            command: The command to be added.
            idx: The index of the entry.
            term: The term of the entry.
        """
        pass

    @abstractmethod
    def clear(self):
        """
        Clear the journal, removing all entries.
        """
        pass

    @abstractmethod
    def deleteEntriesFrom(self, entryFrom):
        """
        Delete entries from the specified index onwards.

        Args:
            entryFrom: The index from which to delete entries.
        """
        pass

    @abstractmethod
    def deleteEntriesTo(self, entryTo):
        """
        Delete entries up to the specified index.

        Args:
            entryTo: The index up to which to delete entries.
        """
        pass

    @abstractmethod
    def __getitem__(self, item):
        """
        Get the entry at the specified index.

        Args:
            item: The index of the entry.

        Returns:
            The entry at the specified index.
        """
        pass

    @abstractmethod
    def __len__(self):
        """
        Get the number of entries in the journal.

        Returns:
            The number of entries in the journal.
        """
        pass

    @abstractmethod
    def _destroy(self):
        """
        Destroy the journal, releasing any resources.
        """
        pass

    @abstractmethod
    def setRaftCommitIndex(self, raftCommitIndex):
        """
        Set the Raft commit index.

        Args:
            raftCommitIndex: The Raft commit index.
        """
        pass

    @abstractmethod
    def getRaftCommitIndex(self):
        """
        Get the Raft commit index.

        Returns:
            The Raft commit index.
        """
        pass

    def onOneSecondTimer(self):
        """
        Perform any necessary actions on a one-second timer tick.
        """
        pass


class MemoryJournal(Journal):
    """
    In-memory journal implementation.
    """

    def __init__(self):
        self.__journal = []
        self.__bytesSize = 0
        self.__lastCommitIndex = 0

    def add(self, command, idx, term):
        self.__journal.append((command, idx, term))

    def clear(self):
        self.__journal = []

    def deleteEntriesFrom(self, entryFrom):
        del self.__journal[entryFrom:]

    def deleteEntriesTo(self, entryTo):
        self.__journal = self.__journal[entryTo:]

    def __getitem__(self, item):
        return self.__journal[item]

    def __len__(self):
        return len(self.__journal)

    def _destroy(self):
        pass

    def setRaftCommitIndex(self, raftCommitIndex):
        pass

    def getRaftCommitIndex(self):
        return 1


class ResizableFile(object):
    """
    Resizable file implementation.
    """

    def __init__(
        self, fileName, initialSize=1024, resizeFactor=2.0, defaultContent=None
    ):
        self.__fileName = fileName
        self.__resizeFactor = resizeFactor
        if not os.path.exists(fileName):
            with open(fileName, "wb") as f:
                if defaultContent is not None:
                    f.write(defaultContent)
        self.__f = open(fileName, "r+b")
        self.__mm = mmap.mmap(self.__f.fileno(), 0)
        currSize = self.__mm.size()
        if currSize < initialSize:
            try:
                self.__mm.resize(initialSize)
            except SystemError:
                self.__extend(initialSize - currSize)

    def write(self, offset, values):
        size = len(values)
        currSize = self.__mm.size()
        if offset + size > self.__mm.size():
            try:
                self.__mm.resize(int(self.__mm.size() * self.__resizeFactor))
            except SystemError:
                self.__extend(int(self.__mm.size() * self.__resizeFactor) - currSize)
        self.__mm[offset : offset + size] = values

    def read(self, offset, size):
        return self.__mm[offset : offset + size]

    def __extend(self, bytesToAdd):
        self.__mm.close()
        self.__f.close()
        with open(self.__fileName, "ab") as f:
            f.write(b"\0" * bytesToAdd)
        self.__f = open(self.__fileName, "r+b")
        self.__mm = mmap.mmap(self.__f.fileno(), 0)

    def _destroy(self):
        self.__mm.flush()
        self.__mm.close()
        self.__f.close()

    def flush(self):
        self.__mm.flush()


class MetaStorer(object):
    """
    Meta data storer implementation.
    """

    def __init__(self, path):
        self.__path = path

    def getMeta(self):
        meta = {}
        try:
            meta = loads(open(self.__path, "rb").read())
        except:
            pass
        return meta

    def storeMeta(self, meta):
        with open(self.__path + ".tmp", "wb") as f:
            f.write(dumps(meta))
            f.flush()
        shutil.move(self.__path + ".tmp", self.__path)

    def getPath(self):
        return self.__path


JOURNAL_FORMAT_VERSION = 1
APP_NAME = b"RaftPySync"
APP_VERSION = str.encode(VERSION)

NAME_SIZE = 24
VERSION_SIZE = 8
assert len(APP_NAME) < NAME_SIZE
assert len(APP_VERSION) < VERSION_SIZE
FIRST_RECORD_OFFSET = NAME_SIZE + VERSION_SIZE + 4 + 4
LAST_RECORD_OFFSET_OFFSET = NAME_SIZE + VERSION_SIZE + 4

#
#  APP_NAME (24b) + APP_VERSION (8b) + FORMAT_VERSION (4b) + LAST_RECORD_OFFSET (4b) +
#      record1size + record1 + record1size   +  record2size + record2 + record2size   +  ...
#                (record1)                   |               (record2)                |  ...
#


class FileJournal(Journal):
    """
    File-based journal implementation.
    """

    def __init__(self, journalFile):
        self.__journalFile = ResizableFile(
            journalFile, defaultContent=self.__getDefaultHeader()
        )
        self.__journal = []
        self.__metaStorer = MetaStorer(journalFile + ".meta")
        self.__meta = self.__metaStorer.getMeta()
        self.__metaSaved = True
        currentOffset = FIRST_RECORD_OFFSET
        lastRecordOffset = self.__getLastRecordOffset()
        while currentOffset < lastRecordOffset:
            nextRecordSize = struct.unpack(
                "<I", self.__journalFile.read(currentOffset, 4)
            )[0]
            nextRecordData = self.__journalFile.read(currentOffset + 4, nextRecordSize)
            command = nextRecordData[16:]
            idx, term = struct.unpack("<QQ", nextRecordData[:16])
            self.__journal.append((command, idx, term))
            currentOffset += nextRecordSize + 8
        self.__currentOffset = currentOffset

    def __getDefaultHeader(self):
        appName = APP_NAME + b"\0" * (NAME_SIZE - len(APP_NAME))
        appVersion = APP_VERSION + b"\0" * (VERSION_SIZE - len(APP_VERSION))
        header = (
            appName
            + appVersion
            + struct.pack("<II", JOURNAL_FORMAT_VERSION, FIRST_RECORD_OFFSET)
        )
        return header

    def __getLastRecordOffset(self):
        return struct.unpack(
            "<I", self.__journalFile.read(LAST_RECORD_OFFSET_OFFSET, 4)
        )[0]

    def __setLastRecordOffset(self, offset):
        self.__journalFile.write(LAST_RECORD_OFFSET_OFFSET, struct.pack("<I", offset))

    def add(self, command, idx, term):
        self.__journal.append((command, idx, term))
        cmdData = struct.pack("<QQ", idx, term) + to_bytes(command)
        cmdLenData = struct.pack("<I", len(cmdData))
        cmdData = cmdLenData + cmdData + cmdLenData
        self.__journalFile.write(self.__currentOffset, cmdData)
        self.__currentOffset += len(cmdData)
        self.__setLastRecordOffset(self.__currentOffset)

    def clear(self):
        self.__journal = []
        self.__setLastRecordOffset(FIRST_RECORD_OFFSET)
        self.__currentOffset = FIRST_RECORD_OFFSET

    def __getitem__(self, idx):
        return self.__journal[idx]

    def __len__(self):
        return len(self.__journal)

    def deleteEntriesFrom(self, entryFrom):
        entriesToRemove = len(self.__journal) - entryFrom
        del self.__journal[entryFrom:]
        currentOffset = self.__currentOffset
        removedEntries = 0
        while removedEntries < entriesToRemove:
            prevRecordSize = struct.unpack(
                "<I", self.__journalFile.read(currentOffset - 4, 4)
            )[0]
            currentOffset -= prevRecordSize + 8
            removedEntries += 1
            if removedEntries % 10 == 0:
                self.__setLastRecordOffset(currentOffset)
        self.__currentOffset = currentOffset
        self.__setLastRecordOffset(currentOffset)

    def deleteEntriesTo(self, entryTo):
        journal = self.__journal[entryTo:]
        self.clear()
        for entry in journal:
            self.add(*entry)

    def _destroy(self):
        self.__journalFile._destroy()

    def flush(self):
        self.__journalFile.flush()

    def setRaftCommitIndex(self, raftCommitIndex):
        self.__meta["raftCommitIndex"] = raftCommitIndex
        self.__metaSaved = False

    def getRaftCommitIndex(self):
        return self.__meta.get("raftCommitIndex", 1)

    def onOneSecondTimer(self):
        if not self.__metaSaved:
            self.__metaStorer.storeMeta(self.__meta)
            self.__metaSaved = True


def create_journal(journal_file=None):
    """
    Factory function to create a journal.

    Args:
        journal_file: The file path for the journal. If None, a MemoryJournal is created.

    Returns:
        An instance of the appropriate journal implementation.
    """
    if journal_file is None:
        return MemoryJournal()
    return FileJournal(journal_file)
