#!/usr/bin/env python3
"""
OVDP Monthly Analysis Script

1. GET https://www.inzhur.reit/_api/assets → filter type=bond, status=active
   Provides: isin, availableQuantity, prices (buy/sell), paymentSchedule
2. GET https://bank.gov.ua/depo_securities?json → couponRate (auk_proc) per ISIN
3. Merge data, find nextCoupon per bond
4. Fire Anthropic Claude Code routine with the full payload

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
NBU_API_URL       = "https://bank.gov.ua/depo_securities?json"
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


# ── Step 2: fetch coupon rates from NBU ───────────────────────────────────────

def fetch_nbu_coupon_rates() -> dict[str, float | None]:
    """
    Fetch all OVDP from NBU in one request.
    Returns dict {isin: couponRate} using auk_proc field.
    """
    log.info("GET %s", NBU_API_URL)
    resp = SESSION.get(NBU_API_URL, timeout=30)
    log.info("  → HTTP %s  %.0f ms  %d records",
             resp.status_code, resp.elapsed.total_seconds() * 1000,
             len(resp.json()) if resp.ok else 0)
    resp.raise_for_status()

    data = resp.json()
    if not isinstance(data, list):
        log.error("  unexpected NBU response type: %s", type(data).__name__)
        return {}

    return {
        rec["cpcode"]: rec.get("auk_proc")
        for rec in data
        if "cpcode" in rec
    }


# ── Step 3: merge and find next coupon ────────────────────────────────────────

def find_next_coupon(schedule: list[dict]) -> dict | None:
    """Return the nearest future coupon payment {date, amount} from a schedule."""
    today = date.today()
    best = None
    for p in schedule:
        if str(p.get("pay_type", "")) != "1":
            continue
        try:
            pay_date = date.fromisoformat(str(p.get("pay_date", ""))[:10])
        except ValueError:
            continue
        if pay_date >= today:
            if best is None or pay_date < date.fromisoformat(best["date"]):
                best = {"date": pay_date.isoformat(), "amount": p.get("pay_val")}
    return best


def build_candidates(inzhur_bonds: list[dict], nbu_rates: dict[str, float | None]) -> list[dict]:
    today  = date.today()
    cutoff = today + timedelta(days=COUPON_WINDOW_DAYS)

    log.info("\nBuilding candidate list for %d active bonds …", len(inzhur_bonds))
    log.info("  Coupon window: %s → %s (🔥 marker)", today, cutoff)

    candidates = []
    for bond in inzhur_bonds:
        isin         = bond["isin"]
        coupon_rate  = nbu_rates.get(isin)
        next_coupon  = find_next_coupon(bond["paymentSchedule"])

        if coupon_rate is None:
            log.info("  %-16s  → couponRate not found in NBU", isin)

        fire = (
            next_coupon is not None
            and today <= date.fromisoformat(next_coupon["date"]) <= cutoff
        )
        log.info(
            "  %-16s  rate=%-6s  qty=%-10s  buy=%-8s  sell=%-8s  next=%s%s",
            isin,
            f"{coupon_rate}%" if coupon_rate is not None else "n/a",
            bond["availableQuantity"] if bond["availableQuantity"] is not None else "n/a",
            bond["priceBuy"]  if bond["priceBuy"]  is not None else "—",
            bond["priceSell"] if bond["priceSell"] is not None else "—",
            next_coupon["date"] if next_coupon else "none",
            "  🔥" if fire else "",
        )

        candidates.append({
            "isin":              isin,
            "couponRate":        coupon_rate,
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
    nbu_rates    = fetch_nbu_coupon_rates()
    candidates   = build_candidates(inzhur_bonds, nbu_rates)

    log.info("\n" + "=" * 60)
    log.info("SUMMARY: %d bond(s)", len(candidates))
    if candidates:
        log.info("  %-16s  %-7s  %-8s  %-8s  %-10s  %s",
                 "ISIN", "Rate %", "Buy", "Sell", "Qty", "Next coupon")
        log.info("  " + "-" * 68)
        today  = date.today()
        cutoff = today + timedelta(days=COUPON_WINDOW_DAYS)
        for b in sorted(candidates, key=lambda x: float(x["couponRate"] or 0), reverse=True):
            nc = b["nextCoupon"]
            next_str = nc["date"] if nc else "—"
            if nc and today <= date.fromisoformat(nc["date"]) <= cutoff:
                next_str = "🔥 " + next_str
            log.info("  %-16s  %-7s  %-8s  %-8s  %-10s  %s",
                     b["isin"], f"{b['couponRate']}%" if b["couponRate"] else "—",
                     b["priceBuy"] or "—", b["priceSell"] or "—",
                     b["availableQuantity"] if b["availableQuantity"] is not None else "—",
                     next_str)
    log.info("=" * 60)

    if not candidates:
        log.warning("No active bonds found — nothing sent to routine.")
        sys.exit(0)

    fire_routine(candidates)
    log.info("Done.")
