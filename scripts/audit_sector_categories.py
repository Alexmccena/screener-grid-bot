from __future__ import annotations

import csv
import json
import time
import urllib.parse
import urllib.request
from urllib.error import HTTPError, URLError
from pathlib import Path
from typing import Any

import yaml


ROOT = Path(__file__).resolve().parents[1]
SECTORS_PATH = ROOT / "data" / "sectors.yaml"
CACHE_PATH = ROOT / ".tmp" / "sector_audit_cache.json"
REPORT_PATH = ROOT / "sector_audit_report.csv"

DELAY_SECONDS = 2.2
MAX_RETRIES = 4

SECTOR_KEYWORDS = {
    "AI": ("artificial intelligence", "ai", "ai agents", "ai agent", "ai memes"),
    "MEME": ("meme", "dog-themed", "cat-themed", "animal", "politifi", "political"),
    "DEFI": (
        "decentralized finance",
        "defi",
        "dex",
        "derivatives",
        "lending",
        "yield",
        "liquid staking",
        "restaking",
        "synthetics",
        "perpetuals",
        "options",
    ),
    "L1_L2": (
        "layer 1",
        "layer 2",
        "smart contract platform",
        "rollup",
        "modular blockchain",
        "cosmos ecosystem",
        "polkadot ecosystem",
        "ethereum ecosystem",
        "solana ecosystem",
        "avalanche ecosystem",
        "base ecosystem",
        "sui ecosystem",
        "aptos ecosystem",
    ),
    "RWA": ("real world assets", "rwa", "oracle"),
    "GAMEFI": ("gaming", "gamefi", "metaverse", "play to earn", "nft", "fan token"),
    "DEPIN": (
        "depin",
        "distributed computing",
        "storage",
        "iot",
        "data availability",
        "decentralized physical infrastructure",
    ),
    "WEB3_INFRA": (
        "infrastructure",
        "oracle",
        "data",
        "interoperability",
        "identity",
        "governance",
        "wallet",
        "privacy-preserving",
        "zero knowledge",
        "prediction markets",
    ),
    "TOKENIZED_ASSETS": (
        "tokenized stock",
        "tokenized stocks",
        "tokenized assets",
        "commodity",
        "gold",
        "silver",
    ),
    "DESCI": ("desci", "science"),
    "BTC_ECOSYSTEM": ("bitcoin ecosystem", "brc-20", "ordinals", "runes"),
    "PRIVACY": ("privacy", "privacy coins"),
    "EXCHANGE": ("centralized exchange", "exchange-based tokens", "exchange token"),
}

NORMALIZE_SYMBOLS = {
    "1000BONK": "BONK",
    "1000FLOKI": "FLOKI",
    "1000PEPE": "PEPE",
    "1000TURBO": "TURBO",
    "1000LUNC": "LUNC",
    "1000SATS": "SATS",
    "1000XEC": "XEC",
    "1000NEIROCTO": "NEIRO",
    "1000000MOG": "MOG",
    "SHIB1000": "SHIB",
}


def main() -> None:
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    cache = _load_cache()
    sectors = yaml.safe_load(SECTORS_PATH.read_text(encoding="utf-8")) or {}
    rows = []
    for current_sector, symbols in sectors.items():
        for symbol in symbols or []:
            symbol = str(symbol).upper()
            query_symbol = NORMALIZE_SYMBOLS.get(symbol, symbol)
            cg = _safe_source(_coingecko_data, query_symbol, cache)
            cp = _safe_source(_coinpaprika_data, query_symbol, cache)
            evidence = _evidence(cg, cp)
            suggested = _suggest_sector(evidence)
            status = _status(current_sector, suggested, evidence)
            rows.append(
                {
                    "symbol": symbol,
                    "query_symbol": query_symbol,
                    "current_sector": current_sector,
                    "suggested_sector": suggested,
                    "status": status,
                    "coingecko_id": cg.get("id", ""),
                    "coingecko_name": cg.get("name", ""),
                    "coingecko_categories": "; ".join(cg.get("categories", [])),
                    "coinpaprika_id": cp.get("id", ""),
                    "coinpaprika_name": cp.get("name", ""),
                    "coinpaprika_tags": "; ".join(cp.get("tags", [])),
                }
            )
            _save_cache(cache)
            _write_report(rows)
    summary: dict[str, int] = {}
    for row in rows:
        summary[row["status"]] = summary.get(row["status"], 0) + 1
    print(f"written: {REPORT_PATH}")
    print(summary)


def _safe_source(func: Any, symbol: str, cache: dict[str, Any]) -> dict[str, Any]:
    prefix = "cg" if func is _coingecko_data else "cp"
    key = f"{prefix}:{symbol}"
    if key in cache:
        return cache[key]
    try:
        return func(symbol, cache)
    except HTTPError as exc:
        if exc.code in {402, 404, 429}:
            cache[key] = {"error": f"HTTP {exc.code}"}
            return cache[key]
        raise
    except (TimeoutError, URLError) as exc:
        cache[key] = {"error": str(exc)}
        return cache[key]


def _write_report(rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    rows = sorted(rows, key=lambda row: (row["status"], row["current_sector"], row["symbol"]))
    with REPORT_PATH.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def _coingecko_data(symbol: str, cache: dict[str, Any]) -> dict[str, Any]:
    key = f"cg:{symbol}"
    if key in cache:
        return cache[key]
    search = _get_json(f"https://api.coingecko.com/api/v3/search?query={urllib.parse.quote(symbol)}")
    candidates = [
        item
        for item in search.get("coins", [])
        if str(item.get("symbol", "")).upper() == symbol.upper()
    ]
    candidates.sort(key=lambda item: item.get("market_cap_rank") or 10**9)
    if not candidates:
        cache[key] = {}
        return cache[key]
    candidate = candidates[0]
    coin_id = candidate["id"]
    params = urllib.parse.urlencode(
        {
            "localization": "false",
            "tickers": "false",
            "market_data": "false",
            "community_data": "false",
            "developer_data": "false",
            "sparkline": "false",
        }
    )
    detail = _get_json(f"https://api.coingecko.com/api/v3/coins/{coin_id}?{params}")
    cache[key] = {
        "id": coin_id,
        "name": detail.get("name") or candidate.get("name") or "",
        "categories": [str(item) for item in detail.get("categories", []) if item],
    }
    return cache[key]


def _coinpaprika_data(symbol: str, cache: dict[str, Any]) -> dict[str, Any]:
    key = f"cp:{symbol}"
    if key in cache:
        return cache[key]
    search = _get_json(
        "https://api.coinpaprika.com/v1/search?"
        + urllib.parse.urlencode({"q": symbol, "c": "currencies", "limit": 10})
    )
    candidates = [
        item
        for item in search.get("currencies", [])
        if str(item.get("symbol", "")).upper() == symbol.upper()
    ]
    candidates.sort(key=lambda item: item.get("rank") or 10**9)
    if not candidates:
        cache[key] = {}
        return cache[key]
    candidate = candidates[0]
    coin_id = candidate["id"]
    detail = _get_json(f"https://api.coinpaprika.com/v1/coins/{coin_id}")
    tags = []
    for item in detail.get("tags", []) or []:
        if isinstance(item, dict):
            tags.append(str(item.get("name") or item.get("id") or ""))
        else:
            tags.append(str(item))
    cache[key] = {
        "id": coin_id,
        "name": detail.get("name") or candidate.get("name") or "",
        "tags": [item for item in tags if item],
    }
    return cache[key]


def _get_json(url: str) -> Any:
    request = urllib.request.Request(url, headers={"User-Agent": "sector-audit/1.0"})
    for attempt in range(MAX_RETRIES):
        time.sleep(DELAY_SECONDS)
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            if exc.code == 429 and attempt < MAX_RETRIES - 1:
                time.sleep(60 + attempt * 30)
                continue
            raise
        except URLError:
            if attempt < MAX_RETRIES - 1:
                time.sleep(10 + attempt * 10)
                continue
            raise
    raise RuntimeError(f"request failed: {url}")


def _evidence(cg: dict[str, Any], cp: dict[str, Any]) -> list[str]:
    values = []
    values.extend(cg.get("categories", []))
    values.extend(cp.get("tags", []))
    return [str(item).lower() for item in values if item]


def _suggest_sector(evidence: list[str]) -> str:
    if not evidence:
        return ""
    scores: dict[str, int] = {}
    joined = " | ".join(evidence)
    for sector, keywords in SECTOR_KEYWORDS.items():
        for keyword in keywords:
            if keyword in joined:
                scores[sector] = scores.get(sector, 0) + 1
    if not scores:
        return ""
    return sorted(scores.items(), key=lambda item: (-item[1], item[0]))[0][0]


def _status(current: str, suggested: str, evidence: list[str]) -> str:
    if current == "UNCLASSIFIED_BYBIT_1M":
        return "UNCLASSIFIED_HAS_SUGGESTION" if suggested else "UNVERIFIED"
    if not evidence:
        return "UNVERIFIED"
    if not suggested:
        return "REVIEW_NO_SECTOR_SIGNAL"
    if suggested == current:
        return "OK"
    return "REVIEW_POSSIBLE_MISMATCH"


def _load_cache() -> dict[str, Any]:
    if not CACHE_PATH.exists():
        return {}
    return json.loads(CACHE_PATH.read_text(encoding="utf-8"))


def _save_cache(cache: dict[str, Any]) -> None:
    CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
