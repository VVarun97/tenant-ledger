from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Literal
from pydantic import BaseModel, Field

ALLOWED_CURRENCIES = {"USD", "EUR", "GBP"}
ALLOWED_CATEGORIES = {"deposit", "withdrawal", "fee", "transfer"}
ALLOWED_DIRECTIONS = {"in", "out"}


def parse_amount_to_minor(raw_amount: Any) -> int:
    if raw_amount is None or isinstance(raw_amount, bool):
        raise ValueError("amount is required")
    try:
        dec = Decimal(str(raw_amount))
    except InvalidOperation as exc:
        raise ValueError("amount must be a valid decimal number") from exc

    if not dec.is_finite() or dec <= Decimal("0"):
        raise ValueError("amount must be strictly greater than zero")

    if dec.as_tuple().exponent < -2:
        raise ValueError("amount cannot have more than 2 decimal places")

    minor = int((dec * 100).to_integral_value(rounding=ROUND_HALF_UP))
    return minor


def minor_to_decimal_str(minor: int) -> str:
    sign = "-" if minor < 0 else ""
    abs_minor = abs(minor)
    return f"{sign}{abs_minor // 100}.{abs_minor % 100:02d}"


def normalize_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


class BatchIngestionRequest(BaseModel):
    # Accept arbitrary dicts so one bad row never fails Pydantic parsing of the whole batch
    rows: list[dict[str, Any]] = Field(..., min_length=1, max_length=5000)


class BatchRowResult(BaseModel):
    row_index: int
    external_id: str | None = None
    status: Literal["accepted", "duplicate", "rejected"]
    transaction_id: str | None = None
    error: str | None = None


class BatchIngestionResponse(BaseModel):
    accepted_count: int
    duplicate_count: int
    rejected_count: int
    results: list[BatchRowResult]


class ReversalRequest(BaseModel):
    reason: str = Field(..., min_length=3, max_length=255)


class LedgerEntryOut(BaseModel):
    id: str
    client_ref: str
    external_id: str
    category: str
    direction: str
    currency: str
    amount: str
    is_reversal: bool
    reverses_tx_id: str | None
    reason: str | None
    effective_at: datetime
    recorded_at: datetime
    recorded_by: str


class BalanceResponse(BaseModel):
    client_ref: str
    as_of: datetime
    balances: dict[str, str]


class MonthlySummaryItem(BaseModel):
    currency: str
    inflows: str
    outflows: str
    fees: str
    net_change: str
    entry_count: int


class MonthlySummaryResponse(BaseModel):
    client_ref: str
    month: str
    summaries: list[MonthlySummaryItem]


class AuditLogOut(BaseModel):
    id: str
    actor_id: str
    actor_role: str
    action: str
    target_id: str
    metadata_json: str
    created_at: datetime
