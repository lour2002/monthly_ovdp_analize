#!/usr/bin/env python3
"""
OVDP Monthly Analysis Script

1. GET https://www.inzhur.reit/_api/assets → filter type=bond, status=active
   Provides: isin, availableQuantity, prices (buy/sell), paymentSchedule
2. Find nextCoupon from paymentSchedule; amounts are in kopecks → convert to UAH
3. Fire Anthropic Claude Code routine with the full payload

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

INZHUR_ASSETS_URL = "https://www.inzhur.reit/_api/assets"
ROUTINE_URL       = (
    "https://api.anthropic.com/v1/claude_code/routines/"
    "trig_01TEs2S3TcShv7vDdxnjKmfx/fire"
)

COUPON_WINDOW_DAYS = 30

TOKEN = os.environ.get("ANTHROPIC_ROUTINE_TOKEN")
if not TOKEN:
    sys.exit(
        "Error: ANTHROPIC_ROUTINE_TOKEN environment variable is not set.\n"
        "  export ANTHROPIC_ROUTINE_TOKEN=<your-token>"
    )

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "OVDP-Analyzer/1.0", "Accept": "application/json"})


# ── Step 1: fetch active bonds from inzhur ────────────────────────────────────

def fetch_inzhur_bonds() -> list[dict]:
    """
    Fetch all assets from inzhur._api/assets, filter type=bond & status=active.
    Returns list of dicts: {isin, availableQuantity, priceBuy, priceSell, paymentSchedule}.
    """
    log.info("GET %s", INZHUR_ASSETS_URL)
    resp = SESSION.get(INZHUR_ASSETS_URL, timeout=30)
    log.info("  → HTTP %s  %.0f ms", resp.status_code, resp.elapsed.total_seconds() * 1000)
    resp.raise_for_status()

    all_assets = resp.json()
    if not isinstance(all_assets, list):
        log.error("  unexpected response type: %s", type(all_assets).__name__)
        return []

    log.info("  total assets: %d", len(all_assets))

    bonds = []
    for asset in all_assets:
        asset_type = asset.get("type", "")
        status     = asset.get("status", "")
        details    = asset.get("assetDetails") or {}
        isin       = details.get("isin") or ""
        sp         = details.get("securityProperties") or {}
        qty        = sp.get("availableQuantity")
        prices     = details.get("prices") or {}
        schedule   = details.get("paymentSchedule") or []

        is_target = asset_type == "bond" and status == "active"
        marker = "✓" if is_target else "✗"
        log.info(
            "    %s  %-20s  type=%-6s  status=%-10s  isin=%-16s  qty=%s  buy=%s  sell=%s",
            marker, asset.get("slug", ""), asset_type, status,
            isin or "—", qty, prices.get("buy"), prices.get("sell"),
        )

        if is_target:
            bonds.append({
                "isin":              isin,
                "availableQuantity": qty,
                "priceBuy":          prices.get("buy"),
                "priceSell":         prices.get("sell"),
                "paymentSchedule":   schedule,
            })

    log.info("  active bonds (type=bond, status=active): %d", len(bonds))
    return bonds


# ── Step 2: find next coupon ──────────────────────────────────────────────────

def find_next_coupon(schedule: list[dict], isin: str = "") -> dict | None:
    """
    Return the nearest future coupon payment {date, amount} from a schedule.
    Amounts are stored in kopecks — converted to UAH (divided by 100, rounded to 2).
    Handles both inzhur field names (date/type) and NBU-style (pay_date/pay_type).
    """

    if not schedule:
        return None

    # Log first entry to reveal actual field names
    log.debug("  [%s] paymentSchedule sample: %s", isin, schedule[0])

    today = date.today()
    best = None
    for p in schedule:
        # accept both field name conventions
        raw_date = p.get("date") or ""

        if str(raw_date) == "":
            continue

        try:
            pay_date = date.fromisoformat(str(raw_date)[:10])
        except ValueError:
            log.debug("  [%s] unparseable date: %r", isin, raw_date)
            continue

        if pay_date >= today:
            if best is None or pay_date < date.fromisoformat(best["date"]):
                raw_amount = int(p.get("amount"))
                amount_uah = round(raw_amount / 100, 2) if raw_amount is not None else None
                best = {"date": pay_date.isoformat(), "amount": amount_uah}

    return best


def build_candidates(inzhur_bonds: list[dict]) -> list[dict]:
    today  = date.today()
    cutoff = today + timedelta(days=COUPON_WINDOW_DAYS)

    log.info("\nBuilding candidate list for %d active bonds …", len(inzhur_bonds))
    log.info("  Coupon window: %s → %s (🔥 marker)", today, cutoff)

    candidates = []
    for bond in inzhur_bonds:
        isin     = bond["isin"]
        schedule = bond["paymentSchedule"]

        if schedule:
            log.info("  [%s] schedule[0] raw: %s", isin, schedule[0])
        else:
            log.info("  [%s] paymentSchedule is empty", isin)

        next_coupon = find_next_coupon(schedule, isin)

        fire = (
            next_coupon is not None
            and today <= date.fromisoformat(next_coupon["date"]) <= cutoff
        )

        if next_coupon["date"] and fire:
            next_coupon["date"] += " 🔥"

        log.info(
            "  %-16s  qty=%-10s  buy=%-8s  sell=%-8s  next=%s%s",
            isin,
            bond["availableQuantity"] if bond["availableQuantity"] is not None else "n/a",
            bond["priceBuy"]  if bond["priceBuy"]  is not None else "—",
            bond["priceSell"] if bond["priceSell"] is not None else "—",
            next_coupon["date"] if next_coupon else "none",
            "  🔥" if fire else "",
        )

        candidates.append({
            "isin":              isin,
            "availableQuantity": bond["availableQuantity"],
            "priceBuy":          bond["priceBuy"],
            "priceSell":         bond["priceSell"],
            "nextCoupon":        next_coupon,
        })

    return candidates


# ── Step 4: fire routine ───────────────────────────────────────────────────────

def fire_routine(bonds: list[dict]) -> None:
    text = json.dumps(
        {
            "analysisDate": date.today().isoformat(),
            "windowDays":   COUPON_WINDOW_DAYS,
            "bonds":        bonds,
        },
        ensure_ascii=False,
        indent=2,
    )
    log.info("\nPayload to routine:\n%s", text)

    headers = {
        "Authorization":   f"Bearer {TOKEN}",
        "anthropic-version": "2023-06-01",
        "anthropic-beta":  "experimental-cc-routine-2026-04-01",
        "Content-Type":    "application/json",
    }
    log.info("POST %s", ROUTINE_URL)
    resp = requests.post(ROUTINE_URL, headers=headers, json={"text": text}, timeout=60)
    log.info("  → HTTP %s  %.0f ms", resp.status_code, resp.elapsed.total_seconds() * 1000)
    resp.raise_for_status()
    log.info("  routine response: %s", json.dumps(resp.json(), ensure_ascii=False))


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    log.info("=" * 60)
    log.info("OVDP Analysis  |  date: %s  |  window: %d days",
             date.today().isoformat(), COUPON_WINDOW_DAYS)
    log.info("=" * 60)

    inzhur_bonds = fetch_inzhur_bonds()
    candidates   = build_candidates(inzhur_bonds)

    log.info("\n" + "=" * 60)
    log.info("SUMMARY: %d bond(s)", len(candidates))
    if candidates:
        log.info("  %-16s  %-8s  %-8s  %-10s  %s",
                 "ISIN", "Buy", "Sell", "Qty", "Next coupon")
        log.info("  " + "-" * 60)
        today  = date.today()
        cutoff = today + timedelta(days=COUPON_WINDOW_DAYS)
        for b in candidates:
            nc = b["nextCoupon"]
            next_str = nc["date"] if nc else "—"
            if nc and today <= date.fromisoformat(nc["date"]) <= cutoff:
                next_str = "🔥 " + next_str
            log.info("  %-16s  %-8s  %-8s  %-10s  %s",
                     b["isin"],
                     b["priceBuy"]  if b["priceBuy"]  is not None else "—",
                     b["priceSell"] if b["priceSell"] is not None else "—",
                     b["availableQuantity"] if b["availableQuantity"] is not None else "—",
                     next_str)
    log.info("=" * 60)

    if not candidates:
        log.warning("No active bonds found — nothing sent to routine.")
        sys.exit(0)

    fire_routine(candidates)
    log.info("Done.")
