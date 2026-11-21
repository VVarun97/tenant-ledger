from datetime import datetime, timedelta, timezone
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.database import Base, get_db
from app.main import app

SQLALCHEMY_TEST_URL = "sqlite://"
engine = create_engine(
    SQLALCHEMY_TEST_URL,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
    future=True,
)
TestingSessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


@pytest.fixture(autouse=True)
def setup_db():
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)


@pytest.fixture
def client():
    def override_get_db():
        db = TestingSessionLocal()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


def headers(tenant="northbridge", actor="advisor_1", role="advisor"):
    return {"X-Tenant-Id": tenant, "X-Actor-Id": actor, "X-Role": role}


def test_partial_batch_ingestion_and_idempotency(client):
    future_ts = (datetime.now(timezone.utc) + timedelta(days=5)).isoformat()
    payload = {
        "rows": [
            {
                "external_id": "cust-001",
                "client_ref": "CLI-100",
                "type": "deposit",
                "currency": "USD",
                "amount": "1000.50",
                "effective_at": "2026-09-15T10:00:00Z",
            },
            {
                "external_id": "cust-bad-neg",
                "client_ref": "CLI-100",
                "type": "deposit",
                "currency": "USD",
                "amount": "-50.00",
                "effective_at": "2026-09-15T10:00:00Z",
            },
            {
                "external_id": "cust-bad-curr",
                "client_ref": "CLI-100",
                "type": "fee",
                "currency": "XYZ",
                "amount": "10.00",
                "effective_at": "2026-09-15T10:00:00Z",
            },
            {
                "external_id": "cust-bad-future",
                "client_ref": "CLI-100",
                "type": "withdrawal",
                "currency": "USD",
                "amount": "20.00",
                "effective_at": future_ts,
            },
            {
                "external_id": "cust-002",
                "client_ref": "CLI-100",
                "type": "fee",
                "currency": "USD",
                "amount": "25.00",
                "effective_at": "2026-09-20T12:00:00Z",
            },
        ]
    }

    res = client.post("/v1/transactions/batch", json=payload, headers=headers())
    assert res.status_code == 200
    body = res.json()
    assert body["accepted_count"] == 2
    assert body["rejected_count"] == 3
    assert body["duplicate_count"] == 0

    # Re-sending the exact same batch marks valid rows as duplicate and never double-counts
    retry = client.post("/v1/transactions/batch", json=payload, headers=headers())
    retry_body = retry.json()
    assert retry_body["accepted_count"] == 0
    assert retry_body["duplicate_count"] == 2
    assert retry_body["rejected_count"] == 3

    bal = client.get("/v1/clients/CLI-100/balance", headers=headers())
    assert bal.status_code == 200
    assert bal.json()["balances"] == {"USD": "975.50"}


def test_strict_tenant_isolation_and_support_least_privilege(client):
    client.post(
        "/v1/transactions/batch",
        json={
            "rows": [
                {
                    "external_id": "shared-id-1",
                    "client_ref": "CLI-SECRET",
                    "type": "deposit",
                    "currency": "USD",
                    "amount": "500.00",
                    "effective_at": "2026-10-01T09:00:00Z",
                }
            ]
        },
        headers=headers(tenant="northbridge"),
    )

    # Another firm using the exact same external_id & client_ref cannot see Northbridge data
    other_bal = client.get(
        "/v1/clients/CLI-SECRET/balance", headers=headers(tenant="rival_firm")
    )
    assert other_bal.status_code == 404

    # Same external_id in rival_firm is allowed because idempotency is scoped per tenant
    other_ingest = client.post(
        "/v1/transactions/batch",
        json={
            "rows": [
                {
                    "external_id": "shared-id-1",
                    "client_ref": "CLI-SECRET",
                    "type": "deposit",
                    "currency": "USD",
                    "amount": "120.00",
                    "effective_at": "2026-10-01T09:00:00Z",
                }
            ]
        },
        headers=headers(tenant="rival_firm"),
    )
    assert other_ingest.json()["accepted_count"] == 1

    # Support staff can read current balance, but cannot read full transaction history or write
    sup_headers = headers(tenant="northbridge", actor="support_9", role="support")
    sup_bal = client.get("/v1/clients/CLI-SECRET/balance", headers=sup_headers)
    assert sup_bal.status_code == 200
    assert sup_bal.json()["balances"] == {"USD": "500.00"}

    sup_txs = client.get("/v1/clients/CLI-SECRET/transactions", headers=sup_headers)
    assert sup_txs.status_code == 403

    sup_write = client.post("/v1/transactions/batch", json={"rows": []}, headers=sup_headers)
    assert sup_write.status_code in {403, 422}


def test_immutable_reversal_and_audit_trail(client):
    ingest = client.post(
        "/v1/transactions/batch",
        json={
            "rows": [
                {
                    "external_id": "err-tx-1",
                    "client_ref": "CLI-200",
                    "type": "deposit",
                    "currency": "EUR",
                    "amount": "300.00",
                    "effective_at": "2026-10-05T10:00:00Z",
                }
            ]
        },
        headers=headers(),
    )
    tx_id = ingest.json()["results"][0]["transaction_id"]

    rev = client.post(
        f"/v1/transactions/{tx_id}/reverse",
        json={"reason": "Duplicate custodian wire posting"},
        headers=headers(actor="advisor_2"),
    )
    assert rev.status_code == 201
    assert rev.json()["amount"] == "-300.00"
    assert rev.json()["reverses_tx_id"] == tx_id

    # Reversing a second time is rejected
    rev_again = client.post(
        f"/v1/transactions/{tx_id}/reverse",
        json={"reason": "Accidental second click"},
        headers=headers(actor="advisor_2"),
    )
    assert rev_again.status_code == 409

    # Balance nets to 0.00 while preserving both entries in history
    bal = client.get("/v1/clients/CLI-200/balance", headers=headers())
    assert bal.json()["balances"] == {"EUR": "0.00"}

    txs = client.get("/v1/clients/CLI-200/transactions", headers=headers()).json()
    assert len(txs) == 2

    # Admin audit trail records both ingestion and reversal
    audits = client.get("/v1/audit-logs", headers=headers(role="admin")).json()
    actions = [a["action"] for a in audits]
    assert "reverse_transaction" in actions
    assert "batch_ingest" in actions


def test_quarter_end_balance_and_monthly_summary(client):
    client.post(
        "/v1/transactions/batch",
        json={
            "rows": [
                {
                    "external_id": "q3-dep",
                    "client_ref": "CLI-300",
                    "type": "deposit",
                    "currency": "USD",
                    "amount": "2000.00",
                    "effective_at": "2026-09-28T15:00:00Z",
                },
                {
                    "external_id": "q4-in",
                    "client_ref": "CLI-300",
                    "type": "transfer",
                    "direction": "in",
                    "currency": "USD",
                    "amount": "500.00",
                    "effective_at": "2026-10-10T12:00:00Z",
                },
                {
                    "external_id": "q4-out",
                    "client_ref": "CLI-300",
                    "type": "withdrawal",
                    "currency": "USD",
                    "amount": "300.00",
                    "effective_at": "2026-10-12T12:00:00Z",
                },
                {
                    "external_id": "q4-fee",
                    "client_ref": "CLI-300",
                    "type": "fee",
                    "currency": "USD",
                    "amount": "15.00",
                    "effective_at": "2026-10-31T23:00:00Z",
                },
            ]
        },
        headers=headers(),
    )

    q3_bal = client.get(
        "/v1/clients/CLI-300/balance",
        params={"as_of": "2026-09-30T23:59:59Z"},
        headers=headers(),
    )
    assert q3_bal.json()["balances"] == {"USD": "2000.00"}

    oct_summary = client.get(
        "/v1/clients/CLI-300/summary",
        params={"month": "2026-10"},
        headers=headers(),
    )
    assert oct_summary.status_code == 200
    item = oct_summary.json()["summaries"][0]
    assert item["inflows"] == "500.00"
    assert item["outflows"] == "300.00"
    assert item["fees"] == "15.00"
    assert item["net_change"] == "185.00"
    assert item["entry_count"] == 3
