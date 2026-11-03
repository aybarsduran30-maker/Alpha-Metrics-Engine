from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

import main
from database import Base, get_db
from models import ApiClient, EmailVerification


def test_email_registration_creates_key_only_after_verification(monkeypatch):
    test_engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=test_engine)
    session_factory = sessionmaker(bind=test_engine)

    def test_db():
        with session_factory() as db:
            yield db

    delivered = {}

    def capture_email(email, token):
        delivered[email] = token

    monkeypatch.setattr(main, "send_email_verification", capture_email)
    main.app.dependency_overrides[get_db] = test_db
    try:
        client = TestClient(main.app)
        payload = {"company_name": "Example Co", "email": "owner@example.com"}
        response = client.post("/api/v1/auth/register", json=payload)
        assert response.status_code == 202
        assert response.json()["status"] == "verification_sent"

        with session_factory() as db:
            assert db.query(ApiClient).count() == 0
            assert db.query(EmailVerification).count() == 1

        wrong_code = client.post(
            "/api/v1/auth/verify-email",
            json={"email": payload["email"], "token": "wrong"},
        )
        assert wrong_code.status_code == 400

        verified = client.post(
            "/api/v1/auth/verify-email",
            json={"email": payload["email"], "token": delivered[payload["email"]]},
        )
        assert verified.status_code == 201
        api_key = verified.json()["api_key"]
        assert api_key.startswith("am_starter_")

        usage = client.get("/api/v1/auth/usage", headers={"X-API-Key": api_key})
        assert usage.status_code == 200
        assert usage.json()["company"] == "Example Co"

        with session_factory() as db:
            assert db.query(ApiClient).count() == 1
            assert db.query(EmailVerification).count() == 0
            assert db.query(ApiClient).first().api_key_hash != api_key
    finally:
        main.app.dependency_overrides.pop(get_db, None)
        test_engine.dispose()
