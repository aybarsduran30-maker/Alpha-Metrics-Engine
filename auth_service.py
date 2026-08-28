import secrets
from fastapi import Header, HTTPException, status, Depends
from sqlalchemy.orm import Session
from database import get_db
from models import ApiClient

def generate_api_key(tier: str = "starter") -> str:
    token = secrets.token_urlsafe(32)
    return f"am_{tier}_{token}"

def verify_api_key(
    x_api_key: str = Header(..., description="Enterprise API Key for AlphaMetrics"),
    db: Session = Depends(get_db)
) -> ApiClient:
    if not x_api_key.startswith("am_"):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key format. Key must start with 'am_'."
        )

    client = db.query(ApiClient).filter(
        ApiClient.api_key == x_api_key,
        ApiClient.is_active == True
    ).first()

    if not client:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Unauthorized: Invalid or inactive API key."
        )

    if client.used_requests_this_month >= client.monthly_quota:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Monthly quota exceeded. Upgrade your plan at /billing."
        )

    return client
