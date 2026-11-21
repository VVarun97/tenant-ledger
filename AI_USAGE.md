# AI Usage Disclosure

## Tools Used
- Gemini / LLM coding assistant for FastAPI boilerplate, SQLAlchemy 2.0 schema layout, and pytest fixture scaffolding.

## Key Prompts
- "Design a multi-tenant append-only ledger schema in SQLAlchemy supporting row-level partial batch ingestion, per-tenant idempotency, and single-reversal enforcement."
- "Write pytest cases verifying cross-tenant 404 isolation, support role least-privilege restrictions, point-in-time quarter-end balances, and monthly SQL summaries."

## What Was Corrected or Rejected
- **Rejected float/Numeric SQLite storage:** Initial draft suggested `Float` or SQLite `Numeric`; replaced with strict `Decimal` input parsing converted to integer minor units (`signed_amount_minor`) so arithmetic is exact across SQLite and PostgreSQL.
- **Fixed batch rollback behavior:** Initial batch loop committed at the end without per-row savepoints, meaning a concurrent `IntegrityError` rolled back valid rows. Added `db.begin_nested()` savepoints per row so duplicates never abort valid rows in the same batch.
- **Tightened support role access:** Restricted `support` from querying historical `as_of` timestamps or raw transaction lines so support agents only see current account standing.
