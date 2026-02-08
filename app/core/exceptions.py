class CoreServiceError(Exception):
    """Error communicating with core service."""


class S3ServiceError(Exception):
    """Error communicating with S3 service."""


class InvalidFileError(Exception):
    """File validation error."""
