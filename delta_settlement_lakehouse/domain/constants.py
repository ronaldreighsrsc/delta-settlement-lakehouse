"""Domain constants, ISO standard sets, and error codes for financial lakehouse."""

import re
from decimal import Decimal

# ISO 4217 Supported Currencies in B2B Settlements
SUPPORTED_CURRENCIES: frozenset[str] = frozenset({
    "USD", "EUR", "GBP", "JPY", "CAD", "AUD", "CHF", "SGD", "CLP", "BRL", "MXN"
})

# ISO 9362 SWIFT BIC format: 4 bank + 2 country + 2 location + optional 3 branch
SWIFT_BIC_PATTERN: re.Pattern = re.compile(r"^[A-Z]{6}[A-Z0-9]{2}([A-Z0-9]{3})?$")
SWIFT_BIC_REGEX: str = r"^[A-Z]{6}[A-Z0-9]{2}([A-Z0-9]{3})?$"

MIN_SETTLEMENT_AMOUNT: Decimal = Decimal("0.0001")

# Standard Rejection Error Codes
ERR_INVALID_CURRENCY: str = "ERR_INVALID_CURRENCY"
ERR_INVALID_BIC: str = "ERR_INVALID_SWIFT_BIC"
ERR_NON_POSITIVE_AMOUNT: str = "ERR_NON_POSITIVE_AMOUNT"
ERR_INVALID_TIMESTAMP: str = "ERR_INVALID_TIMESTAMP"
ERR_MALFORMED_JSON: str = "ERR_MALFORMED_JSON"
ERR_MISSING_REQUIRED_FIELD: str = "ERR_MISSING_REQUIRED_FIELD"
