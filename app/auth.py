from dataclasses import dataclass
from typing import Literal
from fastapi import Header, HTTPException, status

Role = Literal["admin", "advisor", "support"]
VALID_ROLES = {"admin", "advisor", "support"}


@dataclass(frozen=True)
class ActorContext:
    tenant_id: str
    actor_id: str
    role: Role


def get_actor(
    x_tenant_id: str | None = Header(default=None),
    x_actor_id: str | None = Header(default=None),
    x_role: str | None = Header(default=None),
) -> ActorContext:
    if not x_tenant_id or not x_tenant_id.strip():
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing X-Tenant-Id header",
        )
    if not x_actor_id or not x_actor_id.strip():
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing X-Actor-Id header",
        )
    role = (x_role or "").strip().lower()
    if role not in VALID_ROLES:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Invalid X-Role. Must be one of: {sorted(VALID_ROLES)}",
        )
    return ActorContext(
        tenant_id=x_tenant_id.strip(),
        actor_id=x_actor_id.strip(),
        role=role,  # type: ignore[arg-type]
    )


def require_write_access(actor: ActorContext) -> None:
    if actor.role not in {"admin", "advisor"}:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Write access requires admin or advisor role",
        )


def require_full_history_access(actor: ActorContext) -> None:
    if actor.role not in {"admin", "advisor"}:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Support role is restricted to current balance lookups",
        )
