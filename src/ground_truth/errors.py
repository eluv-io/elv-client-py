"""Application-level exceptions.

Raised by the service layer and mapped to HTTP status codes by the API layer
(see ``ground_truth.routes``).
"""


class BadRequestException(Exception):
    """A request resolved to an invalid value; mapped to HTTP 400."""
