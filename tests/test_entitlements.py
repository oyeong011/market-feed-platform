"""구독 권한 규칙 — 조용히 전체를 주지 않는다."""
from __future__ import annotations

from mdfeed import entitlements as ent


def make(text: str) -> ent.Entitlements:
    return ent.parse(text, source="test")


def test_disabled_when_no_file():
    e = ent.load("")
    assert e.enabled is False
    # 꺼져 있으면 요청을 그대로 쓴다 (예전 동작)
    assert e.resolve("", ["A"]) == ({"A"}, [], "")
    assert e.resolve("", None) == (None, [], "")


def test_unknown_or_missing_token_gets_nothing():
    e = make("good  A,B")
    for token in ("", "nope"):
        granted, denied, why = e.resolve(token, ["A"])
        assert granted == set(), (token, granted)      # 빈 집합 = 아무것도 안 준다
        assert why in ("TOKEN_REQUIRED", "UNKNOWN_TOKEN")
    # 필터를 비워도(전체 구독 요청) 전체를 주지 않는다
    assert e.resolve("nope", None)[0] == set()


def test_partial_entitlement_grants_intersection_and_reports_denied():
    e = make("desk  A,B")
    granted, denied, why = e.resolve("desk", ["A", "C"])
    assert granted == {"A"} and denied == ["C"] and why == "NOT_ENTITLED"


def test_empty_request_narrows_to_allowed_set():
    """전체 구독을 요청해도 허용 집합으로 좁힌다 — 여기서 새면 권한이 의미가 없다."""
    e = make("desk  A,B")
    assert e.resolve("desk", None) == ({"A", "B"}, [], "")


def test_wildcard_token_keeps_request_as_is():
    e = make("full  *")
    assert e.resolve("full", None) == (None, [], "")
    assert e.resolve("full", ["Z"]) == ({"Z"}, [], "")


def test_parse_ignores_comments_and_blank_lines():
    e = make("# 주석\n\n  desk   A , B \n full *\n")
    assert e.enabled and e.allowed("desk") == frozenset({"A", "B"}) and e.allows_all("full")
