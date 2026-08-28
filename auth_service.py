import secrets
from fastapi import Header, HTTPException, status, Depends
from sqlalchemy.orm import Session
from database import get_db
from models import ApiClient

def generate_api_key(tier: str = "starter") -> str:
    token = secrets.token_urlsafe(32)
    return f"am_{tier}_{token}"

def verify_api_key(
    x_api_key: str = Header(None, alias="X-API-Key"),
    db: Session = Depends(get_db)
) -> ApiClient:
    if not x_api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing X-API-Key header"
        )

    clean_key = x_api_key.strip()

    if not clean_key.startswith("am_"):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API Key format"
        )

    client = db.query(ApiClient).filter(
        ApiClient.api_key == clean_key
    ).first()

    if not client:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Unauthorized API Key"
        )

    if not client.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Client account is deactivated"
        )

    used = client.used_requests_this_month or 0
    quota = client.monthly_quota or 50000

    if used >= quota:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Monthly quota exceeded"
        )

    return client
