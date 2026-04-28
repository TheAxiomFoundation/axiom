"""Storage backends for the law archive."""

from axiom.storage.base import StorageBackend
from axiom.storage.sqlite import SQLiteStorage

# PostgreSQL is optional - only import if installed
try:
    from axiom.storage.postgres import PostgresStorage
except ImportError:  # pragma: no cover
    PostgresStorage = None  # type: ignore

# R2 is optional - only import if boto3 is installed
try:
    from axiom.storage.r2 import R2Storage, get_r2
except ImportError:  # pragma: no cover
    R2Storage = None  # type: ignore
    get_r2 = None  # type: ignore

__all__ = ["StorageBackend", "SQLiteStorage"]
if PostgresStorage is not None:
    __all__.append("PostgresStorage")
if R2Storage is not None:
    __all__.extend(["R2Storage", "get_r2"])
