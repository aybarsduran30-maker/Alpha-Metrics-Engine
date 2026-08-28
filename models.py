from datetime import datetime
from sqlalchemy import Column, Integer, String, Boolean, DateTime
from database import Base

class ApiClient(Base):
    __tablename__ = "api_clients"

    id = Column(Integer, primary_key=True, index=True)
    company_name = Column(String(100), nullable=False)
    email = Column(String(100), unique=True, index=True, nullable=False)
    api_key = Column(String(128), unique=True, index=True, nullable=False)
    plan_tier = Column(String(32), default="starter")
    rate_limit_per_min = Column(Integer, default=60)
    monthly_quota = Column(Integer, default=50000)
    used_requests_this_month = Column(Integer, default=0)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
