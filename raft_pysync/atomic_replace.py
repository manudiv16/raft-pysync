import os
import sys
import ctypes
from ctypes import wintypes

if hasattr(ctypes, "windll"):
    # If running on Windows, use the Windows API for atomic file replacement

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)

    def atomicReplace(oldPath, newPath):
        # Convert paths to strings if they are not already
        if not isinstance(oldPath, str):
            oldPath = str(oldPath, sys.getfilesystemencoding())
        if not isinstance(newPath, str):
            newPath = str(newPath, sys.getfilesystemencoding())

        # Define move flags for atomic replacement
        MOVEFILE_REPLACE_EXISTING = 1
        MOVEFILE_WRITE_THROUGH = 8
        move_flags = wintypes.DWORD(MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)

        # Create a transaction for atomic file operations
        transaction = kernel32.CreateTransaction(
            None, 0, 0, 0, 0, 1000, "atomic_replace"
        )
        if transaction == ctypes.c_void_p(-1).value:
            return False

        try:
            # Move the file atomically within the transaction
            res = kernel32.MoveFileTransactedW(
                oldPath,
                newPath,
                None,
                None,
                ctypes.byref(move_flags),
                transaction,
            )
            if not res:
                return False

            # Commit the transaction to make the file replacement permanent
            res = kernel32.CommitTransaction(transaction)
            return bool(res)
        finally:
            # Close the transaction handle
            kernel32.CloseHandle(transaction)

else:
    # If not running on Windows, use os.rename for atomic file replacement
    atomicReplace = os.rename
