#!/usr/bin/env python3
"""
OVDP Monthly Analysis Script

1. Fetch active OVDP ISINs from www.inzhur.reit
2. Fetch ALL bonds in one request from NBU: bank.gov.ua/depo_securities?json
3. Filter NBU bonds by active ISINs; check payments[] for coupons in next 30 days
4. Fire Anthropic Claude Code routine with the filtered list

Usage:
    ANTHROPIC_ROUTINE_TOKEN=<token> python analyze_ovdp.py
"""

import json
import logging
import os
import sys
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

NBU_API_URL = "https://bank.gov.ua/depo_securities?json"

ROUTINE_URL = (
    "https://api.anthropic.com/v1/claude_code/routines/"
    "trig_01TEs2S3TcShv7vDdxnjKmfx/fire"
)

COUPON_WINDOW_DAYS = 30  # kept for payload metadata only

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
    log.info("GET %s", INZHUR_API_URL)
    resp = SESSION.get(INZHUR_API_URL, timeout=30)
    log.info("  → HTTP %s  %.0f ms", resp.status_code, resp.elapsed.total_seconds() * 1000)
    resp.raise_for_status()

    assets = resp.json()["data"][0]["attributes"]["assets"]["data"]
    active = []
    for a in assets:
        attrs = a["attributes"]
        status = attrs.get("status", "?")
        marker = "✓" if status == "active" else "✗"
        log.info("    %s  %s  [%s]", marker, attrs["isin"], status)
        if status == "active":
            active.append(attrs["isin"])

    log.info("  total: %d  active: %d", len(assets), len(active))
    return active


# ── Step 2: fetch all NBU bonds in one request ─────────────────────────────────

def fetch_nbu_all() -> list[dict]:
    log.info("GET %s", NBU_API_URL)
    resp = SESSION.get(NBU_API_URL, timeout=30)
    log.info("  → HTTP %s  %.0f ms  %d bonds", resp.status_code,
             resp.elapsed.total_seconds() * 1000, len(resp.json()) if resp.ok else 0)
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, list):
        log.error("  unexpected NBU response type: %s", type(data).__name__)
        return []
    return data


# ── Step 3: build candidate list ──────────────────────────────────────────────

def build_candidates(active_isins: list[str], nbu_bonds: list[dict]) -> list[dict]:
    today = date.today()

    log.info("\nMatching %d active ISINs against NBU data …", len(active_isins))

    nbu_index = {b["cpcode"]: b for b in nbu_bonds if "cpcode" in b}
    log.info("  NBU total bonds: %d  matched: %d",
             len(nbu_bonds), len(set(active_isins) & nbu_index.keys()))

    candidates = []
    for isin in active_isins:
        bond = nbu_index.get(isin)
        if not bond:
            log.info("  %-16s  → not found in NBU", isin)
            continue

        coupon_rate = bond.get("auk_proc")
        payments = bond.get("payments") or []

        # find nearest future coupon (pay_type=1)
        next_coupon = None
        for p in payments:
            if str(p.get("pay_type", "")) != "1":
                continue
            try:
                pay_date = date.fromisoformat(str(p["pay_date"])[:10])
            except (ValueError, KeyError):
                continue
            if pay_date >= today:
                if next_coupon is None or pay_date < date.fromisoformat(next_coupon["date"]):
                    next_coupon = {"date": pay_date.isoformat(), "amount": p.get("pay_val")}

        log.info(
            "  %-16s  rate=%-6s  next_coupon=%s",
            isin,
            f"{coupon_rate}%" if coupon_rate is not None else "n/a",
            next_coupon["date"] if next_coupon else "none",
        )

        candidates.append({
            "isin": isin,
            "couponRate": coupon_rate,
            "nextCoupon": next_coupon,
        })

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

    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "anthropic-version": "2023-06-01",
        "anthropic-beta": "experimental-cc-routine-2026-04-01",
        "Content-Type": "application/json",
    }
    log.info("POST %s", ROUTINE_URL)
    resp = requests.post(ROUTINE_URL, headers=headers, json={"text": text}, timeout=60)
    log.info("  → HTTP %s  %.0f ms", resp.status_code, resp.elapsed.total_seconds() * 1000)
    resp.raise_for_status()
    log.info("  routine response: %s", json.dumps(resp.json(), ensure_ascii=False))


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    log.info("=" * 60)
    log.info("OVDP Analysis started  |  date: %s  |  window: %d days",
             date.today().isoformat(), COUPON_WINDOW_DAYS)
    log.info("=" * 60)

    active_isins = fetch_active_isins()
    nbu_bonds    = fetch_nbu_all()
    candidates   = build_candidates(active_isins, nbu_bonds)

    log.info("\n" + "=" * 60)
    log.info("SUMMARY: %d bond(s) with upcoming coupons", len(candidates))
    if candidates:
        log.info("  %-16s  %-8s  %s", "ISIN", "Rate %", "Next coupon")
        log.info("  " + "-" * 42)
        for b in sorted(candidates, key=lambda x: float(x["couponRate"] or 0), reverse=True):
            next_date = b["nextCoupon"]["date"] if b["nextCoupon"] else "—"
            log.info("  %-16s  %-8s  %s", b["isin"], f"{b['couponRate']}%", next_date)
    log.info("=" * 60)

    if not candidates:
        log.warning("No active bonds found — nothing sent to routine.")
        sys.exit(0)

    fire_routine(candidates)
    log.info("Done.")
