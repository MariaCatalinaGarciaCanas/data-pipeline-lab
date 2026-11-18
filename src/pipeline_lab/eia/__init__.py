"""Cliente de la API v2 de EIA."""

from pipeline_lab.eia.client import (
    BASE_URL,
    DATA_ROUTE,
    MAX_LENGTH,
    EIAClient,
    EIAPage,
    RetryPolicy,
    Timeouts,
)
from pipeline_lab.eia.exceptions import (
    EIAAuthError,
    EIABadRequestError,
    EIAClientClosedError,
    EIAError,
    EIAExtractionError,
    EIAHTTPError,
    EIARateLimitError,
    EIAResponseError,
    EIAServerError,
    EIATransportError,
)
from pipeline_lab.eia.extractor import (
    DataSource,
    ExtractionRequest,
    ExtractionResult,
    extract_region_data,
)

__all__ = [
    "BASE_URL",
    "DATA_ROUTE",
    "MAX_LENGTH",
    "EIAAuthError",
    "EIABadRequestError",
    "EIAClient",
    "EIAClientClosedError",
    "DataSource",
    "EIAError",
    "EIAExtractionError",
    "EIAHTTPError",
    "EIAPage",
    "EIARateLimitError",
    "EIAResponseError",
    "EIAServerError",
    "EIATransportError",
    "ExtractionRequest",
    "ExtractionResult",
    "RetryPolicy",
    "Timeouts",
    "extract_region_data",
]
