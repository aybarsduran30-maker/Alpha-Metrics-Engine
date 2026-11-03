import asyncio
import json

import httpx
import pytest

import main


def test_binance_418_uses_coinbase_book(monkeypatch):
    messages = []
    requests = []

    class FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            return None

        async def get(self, url, **kwargs):
            requests.append(url)
            request = httpx.Request("GET", url)
            if "binance" in url:
                response = httpx.Response(418, request=request)
                raise httpx.HTTPStatusError("blocked", request=request, response=response)
            return httpx.Response(
                200,
                json={
                    "bids": [["100.00", "2", 1]],
                    "asks": [["100.10", "3", 1]],
                    "time": "2026-11-03T23:09:28Z",
                },
                request=request,
            )

    class EndPoll(Exception):
        pass

    async def capture_message(message):
        messages.append(json.loads(message))

    async def end_after_first_poll(_):
        raise EndPoll

    monkeypatch.setattr(main.httpx, "AsyncClient", lambda **_: FakeClient())
    monkeypatch.setattr(main.engine_mem, "broadcast_hft", capture_message)
    monkeypatch.setattr(main.asyncio, "sleep", end_after_first_poll)

    with pytest.raises(EndPoll):
        asyncio.run(main.background_lob_engine())

    assert len(requests) == 2
    assert messages[0]["status"] == "live"
    assert messages[0]["exchange"] == "Coinbase Exchange"
    assert messages[0]["current_price"] == pytest.approx(100.05)
    assert messages[0]["spread"] == pytest.approx(0.1)
