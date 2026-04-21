#!/usr/bin/env python3
"""
OVDP Monthly Analysis Script

1. Fetch active OVDP ISINs from www.inzhur.reit
2. For each ISIN query NBU for payment schedule
3. Filter bonds with coupon payments in the next 30 days
4. Fire Anthropic Claude Code routine with the filtered list

Usage:
    ANTHROPIC_ROUTINE_TOKEN=<token> python analyze_ovdp.py
"""

import json
import os
import sys
import time
from datetime import date, timedelta

import requests

# ── Configuration ──────────────────────────────────────────────────────────────

INZHUR_API_URL = (
    "https://www.inzhur.reit/api/asset-pages"
    "?filters[$and][0][slug][$eqi]=ovdp"
    "&populate[SEO]=false"
    "&populate[assets][fields][0]=isin"
    "&populate[assets][fields][1]=status"
)

NBU_API_TEMPLATE = "https://bank.gov.ua/depo_securities?json&isin={isin}"

ROUTINE_URL = (
    "https://api.anthropic.com/v1/claude_code/routines/"
    "trig_01TEs2S3TcShv7vDdxnjKmfx/fire"
)

COUPON_WINDOW_DAYS = 30
NBU_REQUEST_DELAY = 0.3   # seconds between NBU requests to avoid rate limiting

TOKEN = os.environ.get("ANTHROPIC_ROUTINE_TOKEN")
if not TOKEN:
    sys.exit(
        "Error: ANTHROPIC_ROUTINE_TOKEN environment variable is not set.\n"
        "  export ANTHROPIC_ROUTINE_TOKEN=<your-token>"
    )

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "OVDP-Analyzer/1.0", "Accept": "application/json"})


# ── Step 1: fetch active ISINs from inzhur ─────────────────────────────────────

def fetch_active_isins() -> list[str]:
    """Return list of active OVDP ISINs from inzhur.reit."""
    print("Fetching OVDP list from www.inzhur.reit …")
    resp = SESSION.get(INZHUR_API_URL, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    assets = data["data"][0]["attributes"]["assets"]["data"]
    active = [
        a["attributes"]["isin"]
        for a in assets
        if a["attributes"].get("status") == "active"
    ]
    print(f"  Total assets: {len(assets)}, active: {len(active)}")
    return active


# ── Step 2: fetch payment schedule from NBU ────────────────────────────────────

def fetch_nbu(isin: str) -> list[dict]:
    """Return raw payment records from NBU for a given ISIN."""
    url = NBU_API_TEMPLATE.format(isin=isin)
    try:
        resp = SESSION.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else []
    except Exception as exc:
        print(f"  [warn] NBU request failed for {isin}: {exc}")
        return []


def extract_coupon_rate(payments: list[dict]) -> float | None:
    """Extract annual coupon rate (%) from NBU payment records."""
    for p in payments:
        for field in ("couponrate", "coupon_rate", "cpn_percent", "rate", "coupon"):
            val = p.get(field)
            if val is not None:
                try:
                    return float(val)
                except (ValueError, TypeError):
                    pass
    return None


def find_upcoming_coupons(payments: list[dict]) -> list[dict]:
    """Return coupon payments (pay_type=1) within next COUPON_WINDOW_DAYS days."""
    today = date.today()
    cutoff = today + timedelta(days=COUPON_WINDOW_DAYS)
    result = []
    for p in payments:
        if str(p.get("pay_type", "")) != "1":
            continue
        raw_date = p.get("pay_date") or p.get("date") or ""
        try:
            pay_date = date.fromisoformat(str(raw_date)[:10])
        except ValueError:
            continue
        if today <= pay_date <= cutoff:
            result.append({
                "date": pay_date.isoformat(),
                "amount": p.get("pay_val") or p.get("amount"),
            })
    return result


# ── Step 3: build candidate list ───────────────────────────────────────────────

def build_candidates(isins: list[str]) -> list[dict]:
    """Query NBU for each ISIN and collect bonds with upcoming coupon payments."""
    candidates = []
    for isin in isins:
        payments = fetch_nbu(isin)
        time.sleep(NBU_REQUEST_DELAY)

        upcoming = find_upcoming_coupons(payments)
        if not upcoming:
            continue

        coupon_rate = extract_coupon_rate(payments)
        candidates.append({
            "isin": isin,
            "couponRate": coupon_rate,
            "upcomingCoupons": upcoming,
        })
        print(f"  {isin}  rate={coupon_rate}%  next_coupon={upcoming[0]['date']}")

    return candidates


# ── Step 4: fire routine ───────────────────────────────────────────────────────

def fire_routine(bonds: list[dict]) -> None:
    payload = {
        "text": json.dumps(
            {
                "analysisDate": date.today().isoformat(),
                "windowDays": COUPON_WINDOW_DAYS,
                "bonds": bonds,
            },
            ensure_ascii=False,
        )
    }
    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "anthropic-version": "2023-06-01",
        "anthropic-beta": "experimental-cc-routine-2026-04-01",
        "Content-Type": "application/json",
    }
    print("\nFiring Anthropic routine …")
    resp = requests.post(ROUTINE_URL, headers=headers, json=payload, timeout=60)
    resp.raise_for_status()
    print(f"  Done: HTTP {resp.status_code}")


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    today = date.today()
    print(f"Analysis date: {today.isoformat()}, window: {COUPON_WINDOW_DAYS} days\n")

    isins = fetch_active_isins()

    print(f"\nQuerying NBU for {len(isins)} active bonds …")
    candidates = build_candidates(isins)

    print(f"\nBonds with upcoming coupons: {len(candidates)}")

    if not candidates:
        print("No qualifying bonds found — nothing sent to routine.")
        sys.exit(0)

    fire_routine(candidates)
