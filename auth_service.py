import hashlib
import secrets
from fastapi import HTTPException, Security, status, Depends
from fastapi.security.api_key import APIKeyHeader
from sqlalchemy.orm import Session
from database import get_db
from models import ApiClient

API_KEY_NAME = "X-API-Key"
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)

def generate_api_key(tier: str = "starter") -> str:
    random_hex = secrets.token_hex(32)
    return f"am_{tier}_{random_hex}"


def hash_api_key(key: str) -> str:
    """Return the stable, one-way representation stored in the database."""
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
    return f"sha256${digest}"


def find_client_by_api_key(db: Session, key: str) -> ApiClient | None:
    """Find a new hashed key or migrate a legacy plaintext key on first use."""
    key_hash = hash_api_key(key)
    client = db.query(ApiClient).filter(ApiClient.api_key_hash == key_hash).first()
    if client:
        return client

    client = db.query(ApiClient).filter(ApiClient.api_key_hash == key).first()
    if client:
        client.api_key_hash = key_hash
        try:
            db.commit()
        except Exception:
            db.rollback()
    return client

def verify_api_key(
    key: str = Security(api_key_header),
    db: Session = Depends(get_db)
) -> ApiClient:
    if not key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API Key missing in request headers (X-API-Key required)"
        )
    
    client = find_client_by_api_key(db, key)
    
    if not client:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API Key"
        )
        
    if not client.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="API Key has been revoked or deactivated"
        )
        
    if client.used_requests_this_month >= client.monthly_quota:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Monthly quota limit reached ({client.monthly_quota} requests)"
        )
        
    return client
