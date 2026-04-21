#!/usr/bin/env python3
"""
OVDP Monthly Analysis Script

Fetches active OVDP (Ukrainian government bonds) from inzhur.reit,
filters bonds with upcoming coupon payments, selects top-2 by coupon rate,
and fires the Anthropic Claude Code routine for analysis and Notion reporting.

Usage:
    ANTHROPIC_ROUTINE_TOKEN=<token> python analyze_ovdp.py
"""

import json
import os
import sys
from datetime import date, timedelta

import requests

# ── Configuration ──────────────────────────────────────────────────────────────

INZHUR_API_URL = (
    "https://inzhur.reit/api/asset-pages"
    "?filters[$and][0][slug][$eqi]=ovdp"
    "&populate[SEO]=false"
    "&populate[assets][fields][0]=isin"
    "&populate[assets][fields][1]=status"
    "&populate[assets][fields][2]=securityProperties.availableQuantity"
    "&populate[assets][populate][securityProperties][fields][0]=id"
    "&populate[assets][populate][securityProperties][fields][1]=active"
    "&populate[assets][populate][securityProperties][fields][2]=availableQuantity"
    "&populate[assets][populate][paymentSchedule]=*"
    "&populate[Sections][on][assets.bond-units][populate][bondSegments][fields][0]=couponRate"
    "&populate[Sections][on][assets.bond-units][populate][paymentSchedule]=*"
)

NBU_API_TEMPLATE = "https://bank.gov.ua/depo_securities?json&isin={isin}"

ROUTINE_URL = (
    "https://api.anthropic.com/v1/claude_code/routines/"
    "trig_01TEs2S3TcShv7vDdxnjKmfx/fire"
)

COUPON_WINDOW_DAYS = 30
TOP_N = 2

TOKEN = os.environ.get("ANTHROPIC_ROUTINE_TOKEN")
if not TOKEN:
    sys.exit(
        "Error: ANTHROPIC_ROUTINE_TOKEN environment variable is not set.\n"
        "  export ANTHROPIC_ROUTINE_TOKEN=<your-token>"
    )

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "OVDP-Analyzer/1.0", "Accept": "application/json"})


# ── Fetch ──────────────────────────────────────────────────────────────────────

def fetch_inzhur():
    """Return raw JSON from inzhur.reit OVDP page."""
    print("Fetching OVDP data from inzhur.reit …")
    resp = SESSION.get(INZHUR_API_URL, timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_nbu_coupons(isin: str) -> list[dict]:
    """Fetch coupon payments (pay_type=1) from NBU for a given ISIN."""
    url = NBU_API_TEMPLATE.format(isin=isin)
    try:
        resp = SESSION.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        return [p for p in data if str(p.get("pay_type", "")) == "1"]
    except Exception as exc:
        print(f"  [warn] NBU request failed for {isin}: {exc}")
        return []


# ── Parse ──────────────────────────────────────────────────────────────────────

def extract_assets(raw: dict) -> list[dict]:
    """
    Extract flat asset list from Strapi response.
    Returns list of dicts with keys: isin, status, availableQuantity,
    couponRate, paymentSchedule.
    """
    pages = raw.get("data", [])
    if not pages:
        return []

    attrs = pages[0].get("attributes", {})

    # Build isin→couponRate map from Sections[].bondSegments
    coupon_map: dict[str, float | None] = {}
    section_schedule: dict[str, list] = {}
    for section in attrs.get("Sections", []):
        if section.get("__component") != "assets.bond-units":
            continue
        sched = section.get("paymentSchedule") or []

        # bondSegments may be a Strapi relation {"data":[...]} or a plain list
        segs_raw = section.get("bondSegments") or []
        if isinstance(segs_raw, dict):
            segs_raw = segs_raw.get("data") or []

        for seg in segs_raw:
            if not isinstance(seg, dict):
                print(f"  [warn] unexpected bondSegment type {type(seg).__name__}: {seg!r}")
                continue
            # Strapi relation items wrap fields under "attributes"
            seg_attrs = seg.get("attributes", seg)
            isin = seg_attrs.get("isin", "")
            coupon_rate = seg_attrs.get("couponRate")
            if isin:
                coupon_map[isin] = coupon_rate
                if isin not in section_schedule:
                    section_schedule[isin] = sched
            else:
                print(f"  [warn] bondSegment missing isin, couponRate={coupon_rate}")

    assets_data = attrs.get("assets", {})
    if isinstance(assets_data, dict):
        raw_list = assets_data.get("data", [])
    else:
        raw_list = assets_data or []

    result = []
    for item in raw_list:
        a = item.get("attributes", item)
        isin = a.get("isin", "")

        # availableQuantity — may be nested inside securityProperties
        sp = a.get("securityProperties") or {}
        if isinstance(sp, dict):
            sp = sp.get("data") or sp
            sp = sp.get("attributes") or sp if isinstance(sp, dict) else {}
        qty = sp.get("availableQuantity", 0) or 0

        schedule = a.get("paymentSchedule") or section_schedule.get(isin) or []

        result.append({
            "isin": isin,
            "status": a.get("status", ""),
            "availableQuantity": qty,
            "couponRate": coupon_map.get(isin),
            "paymentSchedule": schedule,
        })

    return result


# ── Filter ─────────────────────────────────────────────────────────────────────

def filter_active(assets: list[dict]) -> list[dict]:
    return [a for a in assets if a["status"] == "active" and a["availableQuantity"] > 0]


def upcoming_coupons(schedule: list[dict], window: int = COUPON_WINDOW_DAYS) -> list[dict]:
    """Return coupon payments within next `window` days."""
    today = date.today()
    cutoff = today + timedelta(days=window)
    hits = []
    for p in schedule:
        raw_date = (
            p.get("date") or p.get("pay_date") or p.get("payDate") or ""
        )
        pay_type = str(p.get("pay_type") or p.get("payType") or p.get("type") or "1")

        try:
            pay_date = date.fromisoformat(str(raw_date)[:10])
        except ValueError:
            continue

        is_coupon = pay_type in ("1", "coupon") or "coupon" in pay_type.lower()
        if is_coupon and today <= pay_date <= cutoff:
            hits.append({
                "date": pay_date.isoformat(),
                "amount": p.get("amount") or p.get("pay_val") or p.get("value"),
            })
    return hits


# ── Main ───────────────────────────────────────────────────────────────────────

def build_report() -> list[dict]:
    raw = fetch_inzhur()
    all_assets = extract_assets(raw)
    print(f"  Total assets: {len(all_assets)}")

    active = filter_active(all_assets)
    print(f"  Active with quantity > 0: {len(active)}")

    candidates = []
    for asset in active:
        schedule = asset["paymentSchedule"]
        if not schedule:
            print(f"  [{asset['isin']}] paymentSchedule empty — querying NBU …")
            schedule = fetch_nbu_coupons(asset["isin"])

        coupons = upcoming_coupons(schedule)
        if not coupons:
            continue

        candidates.append({
            "isin": asset["isin"],
            "couponRate": asset["couponRate"],
            "availableQuantity": asset["availableQuantity"],
            "upcomingCoupons": coupons,
        })

    print(f"  Bonds with coupons in next {COUPON_WINDOW_DAYS}d: {len(candidates)}")

    candidates.sort(key=lambda x: float(x["couponRate"] or 0), reverse=True)
    top = candidates[:TOP_N]

    print(f"\nTop-{TOP_N} by coupon rate:")
    for b in top:
        print(
            f"  {b['isin']:16s}  rate={b['couponRate']}%"
            f"  next={b['upcomingCoupons'][0]['date']}"
            f"  qty={b['availableQuantity']}"
        )

    return top


def fire_routine(bonds: list[dict]) -> dict:
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
    result = resp.json()
    print(f"  Routine response: {resp.status_code} — {result}")
    return result


if __name__ == "__main__":
    top_bonds = build_report()
    if top_bonds:
        fire_routine(top_bonds)
    else:
        print("\nNo qualifying bonds found — nothing sent to routine.")
