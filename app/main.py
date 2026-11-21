from datetime import datetime, timezone
from fastapi import Depends, FastAPI, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.auth import (
    ActorContext,
    get_actor,
    require_full_history_access,
    require_write_access,
)
from app.database import Base, engine, get_db
from app.models import AuditLog, LedgerEntry
from app.schemas import (
    AuditLogOut,
    BalanceResponse,
    BatchIngestionRequest,
    BatchIngestionResponse,
    LedgerEntryOut,
    MonthlySummaryResponse,
    ReversalRequest,
    minor_to_decimal_str,
    normalize_utc,
)
from app.service import (
    get_client_balance,
    get_monthly_summary,
    ingest_batch,
    record_audit,
    reverse_transaction,
)

Base.metadata.create_all(bind=engine)

app = FastAPI(title="Northbridge Multi-Tenant Ledger", version="1.0.0")


def _to_entry_out(entry: LedgerEntry) -> LedgerEntryOut:
    return LedgerEntryOut(
        id=entry.id,
        client_ref=entry.client_ref,
        external_id=entry.external_id,
        category=entry.category,
        direction=entry.direction,
        currency=entry.currency,
        amount=minor_to_decimal_str(entry.signed_amount_minor),
        is_reversal=entry.is_reversal,
        reverses_tx_id=entry.reverses_tx_id,
        reason=entry.reason,
        effective_at=entry.effective_at,
        recorded_at=entry.recorded_at,
        recorded_by=entry.recorded_by,
    )


@app.post(
    "/v1/transactions/batch",
    response_model=BatchIngestionResponse,
    status_code=status.HTTP_200_OK,
)
def post_batch_transactions(
    payload: BatchIngestionRequest,
    actor: ActorContext = Depends(get_actor),
    db: Session = Depends(get_db),
):
    require_write_access(actor)
    results = ingest_batch(db, actor, payload.rows)
    return BatchIngestionResponse(
        accepted_count=sum(1 for r in results if r.status == "accepted"),
        duplicate_count=sum(1 for r in results if r.status == "duplicate"),
        rejected_count=sum(1 for r in results if r.status == "rejected"),
        results=results,
    )


@app.post(
    "/v1/transactions/{tx_id}/reverse",
    response_model=LedgerEntryOut,
    status_code=status.HTTP_201_CREATED,
)
def post_reverse_transaction(
    tx_id: str,
    payload: ReversalRequest,
    actor: ActorContext = Depends(get_actor),
    db: Session = Depends(get_db),
):
    require_write_access(actor)
    try:
        reversal = reverse_transaction(db, actor, tx_id, payload.reason)
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    return _to_entry_out(reversal)


@app.get("/v1/clients/{client_ref}/balance", response_model=BalanceResponse)
def get_balance(
    client_ref: str,
    as_of: datetime | None = Query(default=None),
    actor: ActorContext = Depends(get_actor),
    db: Session = Depends(get_db),
):
    # Support role can only check current balance, never historical snapshots
    if actor.role == "support" and as_of is not None:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Support role cannot query historical balances",
        )
    target_ts = normalize_utc(as_of) if as_of else datetime.now(timezone.utc)
    balances = get_client_balance(db, actor.tenant_id, client_ref, target_ts)
    if not balances:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Client not found")

    record_audit(
        db,
        actor,
        action="read_balance",
        target_id=client_ref,
        metadata={"as_of": target_ts.isoformat()},
    )
    db.commit()
    return BalanceResponse(client_ref=client_ref, as_of=target_ts, balances=balances)


@app.get("/v1/clients/{client_ref}/transactions", response_model=list[LedgerEntryOut])
def list_client_transactions(
    client_ref: str,
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    actor: ActorContext = Depends(get_actor),
    db: Session = Depends(get_db),
):
    require_full_history_access(actor)
    entries = (
        db.execute(
            select(LedgerEntry)
            .where(
                LedgerEntry.tenant_id == actor.tenant_id,
                LedgerEntry.client_ref == client_ref,
            )
            .order_by(LedgerEntry.effective_at.desc(), LedgerEntry.recorded_at.desc())
            .limit(limit)
            .offset(offset)
        )
        .scalars()
        .all()
    )
    if not entries:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Client not found")
    return [_to_entry_out(e) for e in entries]


@app.get("/v1/clients/{client_ref}/summary", response_model=MonthlySummaryResponse)
def get_client_monthly_summary(
    client_ref: str,
    month: str = Query(..., pattern=r"^\d{4}-(0[1-9]|1[0-2])$"),
    actor: ActorContext = Depends(get_actor),
    db: Session = Depends(get_db),
):
    require_full_history_access(actor)
    year, mon = map(int, month.split("-"))
    month_start = datetime(year, mon, 1, tzinfo=timezone.utc)
    next_year, next_mon = (year + 1, 1) if mon == 12 else (year, mon + 1)
    month_end = datetime(next_year, next_mon, 1, tzinfo=timezone.utc)

    summaries = get_monthly_summary(db, actor.tenant_id, client_ref, month_start, month_end)
    if not summaries:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No transactions for client in requested month",
        )
    return MonthlySummaryResponse(client_ref=client_ref, month=month, summaries=summaries)


@app.get("/v1/audit-logs", response_model=list[AuditLogOut])
def list_audit_logs(
    limit: int = Query(default=100, ge=1, le=500),
    actor: ActorContext = Depends(get_actor),
    db: Session = Depends(get_db),
):
    if actor.role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Audit logs require admin role",
        )
    logs = (
        db.execute(
            select(AuditLog)
            .where(AuditLog.tenant_id == actor.tenant_id)
            .order_by(AuditLog.created_at.desc())
            .limit(limit)
        )
        .scalars()
        .all()
    )
    return [
        AuditLogOut(
            id=log.id,
            actor_id=log.actor_id,
            actor_role=log.actor_role,
            action=log.action,
            target_id=log.target_id,
            metadata_json=log.metadata_json,
            created_at=log.created_at,
        )
        for log in logs
    ]
