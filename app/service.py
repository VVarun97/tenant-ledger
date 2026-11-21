import json
import uuid
from datetime import datetime, timezone
from typing import Any
from sqlalchemy import and_, case, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.auth import ActorContext
from app.models import AuditLog, LedgerEntry
from app.schemas import (
    ALLOWED_CATEGORIES,
    ALLOWED_CURRENCIES,
    ALLOWED_DIRECTIONS,
    BatchRowResult,
    MonthlySummaryItem,
    minor_to_decimal_str,
    normalize_utc,
    parse_amount_to_minor,
)


def record_audit(
    db: Session,
    actor: ActorContext,
    action: str,
    target_id: str,
    metadata: dict[str, Any] | None = None,
) -> None:
    db.add(
        AuditLog(
            tenant_id=actor.tenant_id,
            actor_id=actor.actor_id,
            actor_role=actor.role,
            action=action,
            target_id=target_id,
            metadata_json=json.dumps(metadata or {}, default=str),
        )
    )


def _validate_and_normalize_row(
    raw: dict[str, Any], now: datetime
) -> tuple[str, str, str, str, int, datetime]:
    if not isinstance(raw, dict):
        raise ValueError("row must be an object")

    external_id = str(raw.get("external_id") or "").strip()
    if not external_id:
        raise ValueError("external_id is required")

    client_ref = str(raw.get("client_ref") or "").strip()
    if not client_ref:
        raise ValueError("client_ref is required")

    category = str(raw.get("type") or raw.get("category") or "").strip().lower()
    if category not in ALLOWED_CATEGORIES:
        raise ValueError(f"type must be one of {sorted(ALLOWED_CATEGORIES)}")

    currency = str(raw.get("currency") or "").strip().upper()
    if currency not in ALLOWED_CURRENCIES:
        raise ValueError(f"currency must be one of {sorted(ALLOWED_CURRENCIES)}")

    minor = parse_amount_to_minor(raw.get("amount"))

    raw_dir = raw.get("direction")
    if category == "deposit":
        if raw_dir and str(raw_dir).lower() != "in":
            raise ValueError("deposit direction must be 'in'")
        direction = "in"
    elif category in {"withdrawal", "fee"}:
        if raw_dir and str(raw_dir).lower() != "out":
            raise ValueError(f"{category} direction must be 'out'")
        direction = "out"
    else:  # transfer
        direction = str(raw_dir or "").strip().lower()
        if direction not in ALLOWED_DIRECTIONS:
            raise ValueError("transfer requires direction 'in' or 'out'")

    raw_ts = raw.get("effective_at")
    if not raw_ts:
        raise ValueError("effective_at is required")
    try:
        effective_at = normalize_utc(datetime.fromisoformat(str(raw_ts)))
    except ValueError as exc:
        raise ValueError("effective_at must be a valid ISO-8601 timestamp") from exc

    if effective_at > now:
        raise ValueError("effective_at cannot be in the future")

    signed_minor = minor if direction == "in" else -minor
    return external_id, client_ref, category, currency, signed_minor, effective_at


def ingest_batch(
    db: Session, actor: ActorContext, rows: list[dict[str, Any]]
) -> list[BatchRowResult]:
    now = datetime.now(timezone.utc)
    results: list[BatchRowResult] = []
    seen_in_payload: set[str] = set()

    for idx, raw in enumerate(rows):
        ext_preview = str(raw.get("external_id")).strip() if isinstance(raw, dict) and raw.get("external_id") else None
        try:
            (
                external_id,
                client_ref,
                category,
                currency,
                signed_minor,
                effective_at,
            ) = _validate_and_normalize_row(raw, now)
        except ValueError as exc:
            results.append(
                BatchRowResult(
                    row_index=idx,
                    external_id=ext_preview,
                    status="rejected",
                    error=str(exc),
                )
            )
            continue

        if external_id in seen_in_payload:
            existing = db.execute(
                select(LedgerEntry.id).where(
                    LedgerEntry.tenant_id == actor.tenant_id,
                    LedgerEntry.external_id == external_id,
                )
            ).scalar_one_or_none()
            results.append(
                BatchRowResult(
                    row_index=idx,
                    external_id=external_id,
                    status="duplicate",
                    transaction_id=existing,
                )
            )
            continue

        seen_in_payload.add(external_id)
        direction = "in" if signed_minor > 0 else "out"
        entry = LedgerEntry(
            id=str(uuid.uuid4()),
            tenant_id=actor.tenant_id,
            client_ref=client_ref,
            external_id=external_id,
            category=category,
            direction=direction,
            currency=currency,
            signed_amount_minor=signed_minor,
            is_reversal=False,
            effective_at=effective_at,
            recorded_by=actor.actor_id,
        )

        # Savepoint per row isolates IntegrityError on duplicates without rolling back valid rows
        try:
            with db.begin_nested():
                db.add(entry)
                db.flush()
            results.append(
                BatchRowResult(
                    row_index=idx,
                    external_id=external_id,
                    status="accepted",
                    transaction_id=entry.id,
                )
            )
        except IntegrityError:
            existing = db.execute(
                select(LedgerEntry.id).where(
                    LedgerEntry.tenant_id == actor.tenant_id,
                    LedgerEntry.external_id == external_id,
                )
            ).scalar_one_or_none()
            results.append(
                BatchRowResult(
                    row_index=idx,
                    external_id=external_id,
                    status="duplicate",
                    transaction_id=existing,
                )
            )

    accepted = sum(1 for r in results if r.status == "accepted")
    duplicates = sum(1 for r in results if r.status == "duplicate")
    rejected = sum(1 for r in results if r.status == "rejected")

    record_audit(
        db,
        actor,
        action="batch_ingest",
        target_id=f"batch:{len(rows)}",
        metadata={"accepted": accepted, "duplicate": duplicates, "rejected": rejected},
    )
    db.commit()
    return results


def reverse_transaction(
    db: Session, actor: ActorContext, tx_id: str, reason: str
) -> LedgerEntry:
    original = db.execute(
        select(LedgerEntry).where(
            LedgerEntry.tenant_id == actor.tenant_id,
            LedgerEntry.id == tx_id,
        )
    ).scalar_one_or_none()

    if not original:
        raise LookupError("Transaction not found")
    if original.is_reversal:
        raise ValueError("Cannot reverse a reversal entry")

    reversal = LedgerEntry(
        id=str(uuid.uuid4()),
        tenant_id=actor.tenant_id,
        client_ref=original.client_ref,
        external_id=f"rev:{original.id}",
        category=original.category,
        direction=original.direction,
        currency=original.currency,
        signed_amount_minor=-original.signed_amount_minor,
        is_reversal=True,
        reverses_tx_id=original.id,
        reason=reason,
        effective_at=original.effective_at,
        recorded_by=actor.actor_id,
    )

    try:
        db.add(reversal)
        record_audit(
            db,
            actor,
            action="reverse_transaction",
            target_id=original.id,
            metadata={"reversal_id": reversal.id, "reason": reason},
        )
        db.commit()
        db.refresh(reversal)
        return reversal
    except IntegrityError as exc:
        db.rollback()
        raise ValueError("Transaction has already been reversed") from exc


def get_client_balance(
    db: Session, tenant_id: str, client_ref: str, as_of: datetime
) -> dict[str, str]:
    rows = db.execute(
        select(
            LedgerEntry.currency,
            func.coalesce(func.sum(LedgerEntry.signed_amount_minor), 0),
        )
        .where(
            LedgerEntry.tenant_id == tenant_id,
            LedgerEntry.client_ref == client_ref,
            LedgerEntry.effective_at <= as_of,
        )
        .group_by(LedgerEntry.currency)
    ).all()

    return {currency: minor_to_decimal_str(int(total)) for currency, total in rows}


def get_monthly_summary(
    db: Session, tenant_id: str, client_ref: str, month_start: datetime, month_end: datetime
) -> list[MonthlySummaryItem]:
    inflow_expr = func.coalesce(
        func.sum(
            case(
                (
                    and_(
                        LedgerEntry.category.in_(["deposit", "transfer"]),
                        LedgerEntry.direction == "in",
                    ),
                    LedgerEntry.signed_amount_minor,
                ),
                else_=0,
            )
        ),
        0,
    )
    outflow_expr = func.coalesce(
        func.sum(
            case(
                (
                    and_(
                        LedgerEntry.category.in_(["withdrawal", "transfer"]),
                        LedgerEntry.direction == "out",
                    ),
                    -LedgerEntry.signed_amount_minor,
                ),
                else_=0,
            )
        ),
        0,
    )
    fee_expr = func.coalesce(
        func.sum(
            case(
                (LedgerEntry.category == "fee", -LedgerEntry.signed_amount_minor),
                else_=0,
            )
        ),
        0,
    )
    net_expr = func.coalesce(func.sum(LedgerEntry.signed_amount_minor), 0)

    rows = db.execute(
        select(
            LedgerEntry.currency,
            inflow_expr,
            outflow_expr,
            fee_expr,
            net_expr,
            func.count(LedgerEntry.id),
        )
        .where(
            LedgerEntry.tenant_id == tenant_id,
            LedgerEntry.client_ref == client_ref,
            LedgerEntry.effective_at >= month_start,
            LedgerEntry.effective_at < month_end,
        )
        .group_by(LedgerEntry.currency)
        .order_by(LedgerEntry.currency)
    ).all()

    return [
        MonthlySummaryItem(
            currency=currency,
            inflows=minor_to_decimal_str(int(inflows)),
            outflows=minor_to_decimal_str(int(outflows)),
            fees=minor_to_decimal_str(int(fees)),
            net_change=minor_to_decimal_str(int(net_change)),
            entry_count=int(count),
        )
        for currency, inflows, outflows, fees, net_change, count in rows
    ]
