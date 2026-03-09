"""Constants for file operations and permissions."""

from enum import Enum


class FileAction(str, Enum):
    """File operation actions."""

    READ = "read"
    WRITE = "write"
    DELETE = "delete"
    LIST = "list"
    UPDATE = "update"
    PARTIAL_UPDATE = "partial_update"
