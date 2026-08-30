import time
import secrets
import sqlite3
from typing import Optional, Dict

DB_PATH = "alphametrics_vault.db"

TIER_CONFIG = {
    "free": {"rpm": 10, "monthly_quota": 500},
    "pro": {"rpm": 60, "monthly_quota": 25000},
    "enterprise": {"rpm": 300, "monthly_quota": 500000}
}

def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS api_keys (
                key TEXT PRIMARY KEY,
                tier TEXT NOT NULL,
                rpm_limit INTEGER NOT NULL,
                monthly_quota INTEGER NOT NULL,
                used_this_month INTEGER DEFAULT 0,
                created_at REAL NOT NULL
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS usage_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                key TEXT NOT NULL,
                endpoint TEXT NOT NULL,
                timestamp REAL NOT NULL,
                FOREIGN KEY(key) REFERENCES api_keys(key)
            )
        """)
        cursor.execute("SELECT COUNT(*) FROM api_keys WHERE key = 'am_enterprise_devkey001'")
        if cursor.fetchone()[0] == 0:
            cursor.execute("""
                INSERT INTO api_keys (key, tier, rpm_limit, monthly_quota, used_this_month, created_at)
                VALUES ('am_enterprise_devkey001', 'enterprise', 300, 500000, 0, ?)
            """, (time.time(),))
        conn.commit()

init_db()

def validate_api_key(key: str) -> Optional[Dict]:
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT tier, rpm_limit, monthly_quota, used_this_month FROM api_keys WHERE key = ?", (key,))
        row = cursor.fetchone()
        if not row:
            return None
        return {
            "key": key,
            "tier": row[0],
            "rpm_limit": row[1],
            "monthly_quota": row[2],
            "used_this_month": row[3]
        }

def record_request_with_endpoint(key: str, endpoint: str):
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("UPDATE api_keys SET used_this_month = used_this_month + 1 WHERE key = ?", (key,))
        cursor.execute("INSERT INTO usage_logs (key, endpoint, timestamp) VALUES (?, ?, ?)", (key, endpoint, time.time()))
        conn.commit()

def upgrade_key_tier(key: str, new_tier: str) -> bool:
    if new_tier not in TIER_CONFIG:
        return False
    cfg = TIER_CONFIG[new_tier]
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE api_keys 
            SET tier = ?, rpm_limit = ?, monthly_quota = ?
            WHERE key = ?
        """, (new_tier, cfg["rpm"], cfg["monthly_quota"], key))
        conn.commit()
        return cursor.rowcount > 0

def get_usage_metrics(key: str) -> Dict:
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT endpoint, COUNT(*) 
            FROM usage_logs 
            WHERE key = ? 
            GROUP BY endpoint
        """, (key,))
        breakdown = {row[0]: row[1] for row in cursor.fetchall()}
        return breakdown

RATE_LIMIT_CACHE = {}

def check_rate_limit(key: str, ip: str, rpm_limit: int) -> bool:
    now = time.time()
    tracking_key = f"{key}_{ip}"
    if tracking_key not in RATE_LIMIT_CACHE:
        RATE_LIMIT_CACHE[tracking_key] = []
    
    RATE_LIMIT_CACHE[tracking_key] = [t for t in RATE_LIMIT_CACHE[tracking_key] if now - t < 60]
    
    if len(RATE_LIMIT_CACHE[tracking_key]) >= rpm_limit:
        return False
    
    RATE_LIMIT_CACHE[tracking_key].append(now)
    return True
