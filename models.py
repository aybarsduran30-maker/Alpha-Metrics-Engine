from datetime import datetime
from sqlalchemy import Boolean, Column, DateTime, Float, Integer, String
from database import Base

class AssetMetricHistory(Base):
    __tablename__ = "asset_metrics_history"

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String(32), index=True)
    price = Column(Float)
    change_24h = Column(Float)
    fourteen_d_rsi = Column(Float)
    risk_status = Column(String(32))
    recorded_at = Column(DateTime, default=datetime.utcnow)

class ApiClient(Base):
    __tablename__ = "api_clients"

    id = Column(Integer, primary_key=True, index=True)
    company_name = Column(String(100), nullable=False)
    email = Column(String(100), unique=True, index=True, nullable=False)
    # Retain the physical column name for existing databases; it now stores a hash.
    api_key_hash = Column("api_key", String(128), unique=True, index=True, nullable=False)
    plan_tier = Column(String(32), default="starter")
    rate_limit_per_min = Column(Integer, default=60)
    monthly_quota = Column(Integer, default=500)
    used_requests_this_month = Column(Integer, default=0)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class EmailVerification(Base):
    __tablename__ = "email_verifications"

    id = Column(Integer, primary_key=True, index=True)
    email = Column(String(254), unique=True, index=True, nullable=False)
    company_name = Column(String(100), nullable=False)
    token_hash = Column(String(71), nullable=False)
    expires_at_epoch = Column(Float, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
