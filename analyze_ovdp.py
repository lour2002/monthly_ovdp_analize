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
import logging
import os
import sys
import time
from datetime import date, timedelta

import requests

# ── Logging ────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("ovdp")

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
    log.info("GET %s", INZHUR_API_URL)
    resp = SESSION.get(INZHUR_API_URL, timeout=30)
    log.info("  → HTTP %s  %.0f ms", resp.status_code, resp.elapsed.total_seconds() * 1000)
    resp.raise_for_status()

    data = resp.json()
    assets = data["data"][0]["attributes"]["assets"]["data"]
    all_isins  = [a["attributes"]["isin"] for a in assets]
    active = [
        a["attributes"]["isin"]
        for a in assets
        if a["attributes"].get("status") == "active"
    ]

    log.info("  inzhur: %d total assets, %d active", len(all_isins), len(active))
    for a in assets:
        attrs = a["attributes"]
        marker = "✓" if attrs.get("status") == "active" else "✗"
        log.info("    %s  %s  [%s]", marker, attrs["isin"], attrs.get("status", "?"))

    return active


# ── Step 2: fetch payment schedule from NBU ────────────────────────────────────

def fetch_nbu(isin: str) -> list[dict]:
    """Return raw payment records from NBU for a given ISIN."""
    url = NBU_API_TEMPLATE.format(isin=isin)
    log.info("  GET NBU %s", url)
    try:
        resp = SESSION.get(url, timeout=15)
        elapsed = resp.elapsed.total_seconds() * 1000
        log.info("    → HTTP %s  %.0f ms  %d records", resp.status_code, elapsed, len(resp.json()) if resp.ok else 0)
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, list):
            log.warning("    unexpected NBU response type: %s", type(data).__name__)
            return []
        for rec in data:
            log.debug("    record: %s", json.dumps(rec, ensure_ascii=False))
        return data
    except Exception as exc:
        log.warning("    NBU request failed: %s", exc)
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
    today = date.today()
    cutoff = today + timedelta(days=COUPON_WINDOW_DAYS)

    log.info("\nQuerying NBU for %d ISINs (window: %s → %s) …", len(isins), today, cutoff)

    for idx, isin in enumerate(isins, 1):
        log.info("[%d/%d] %s", idx, len(isins), isin)
        payments = fetch_nbu(isin)
        time.sleep(NBU_REQUEST_DELAY)

        if not payments:
            log.info("    → no data from NBU, skipping")
            continue

        coupon_rate = extract_coupon_rate(payments)
        upcoming = find_upcoming_coupons(payments)

        all_coupons = [p for p in payments if str(p.get("pay_type", "")) == "1"]
        log.info(
            "    couponRate=%-6s  total_coupon_records=%d  upcoming=%d",
            f"{coupon_rate}%" if coupon_rate is not None else "n/a",
            len(all_coupons),
            len(upcoming),
        )

        if upcoming:
            for c in upcoming:
                log.info("    ✓ upcoming coupon: %s  amount=%s", c["date"], c["amount"])
            candidates.append({
                "isin": isin,
                "couponRate": coupon_rate,
                "upcomingCoupons": upcoming,
            })
        else:
            log.info("    – no coupons in window")

    return candidates


# ── Step 4: fire routine ───────────────────────────────────────────────────────

def fire_routine(bonds: list[dict]) -> None:
    text = json.dumps(
        {
            "analysisDate": date.today().isoformat(),
            "windowDays": COUPON_WINDOW_DAYS,
            "bonds": bonds,
        },
        ensure_ascii=False,
        indent=2,
    )
    log.info("\nPayload to routine:\n%s", text)

    payload = {"text": text}
    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "anthropic-version": "2023-06-01",
        "anthropic-beta": "experimental-cc-routine-2026-04-01",
        "Content-Type": "application/json",
    }

    log.info("POST %s", ROUTINE_URL)
    resp = requests.post(ROUTINE_URL, headers=headers, json=payload, timeout=60)
    elapsed = resp.elapsed.total_seconds() * 1000
    log.info("  → HTTP %s  %.0f ms", resp.status_code, elapsed)
    resp.raise_for_status()

    body = resp.json()
    log.info("  routine response: %s", json.dumps(body, ensure_ascii=False))


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    today = date.today()
    log.info("=" * 60)
    log.info("OVDP Analysis started")
    log.info("Date: %s   Window: %d days", today.isoformat(), COUPON_WINDOW_DAYS)
    log.info("=" * 60)

    isins = fetch_active_isins()

    candidates = build_candidates(isins)

    log.info("\n" + "=" * 60)
    log.info("SUMMARY: %d bond(s) with upcoming coupons", len(candidates))
    if candidates:
        log.info("%-16s  %-8s  %s", "ISIN", "Rate %", "Next coupon")
        log.info("-" * 45)
        for b in sorted(candidates, key=lambda x: float(x["couponRate"] or 0), reverse=True):
            log.info("%-16s  %-8s  %s", b["isin"], f"{b['couponRate']}%", b["upcomingCoupons"][0]["date"])
    log.info("=" * 60)

    if not candidates:
        log.warning("No qualifying bonds found — nothing sent to routine.")
        sys.exit(0)

    fire_routine(candidates)
    log.info("Done.")
