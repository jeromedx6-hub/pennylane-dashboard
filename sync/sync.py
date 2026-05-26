#!/usr/bin/env python3
"""
Sync quotidien Pennylane → Supabase  (via transactions bancaires)
Cron Railway : 0 6 * * *
"""
import os, sys, logging, time
from datetime import date, timedelta

import requests
from supabase import create_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

PENNYLANE_TOKEN  = os.environ["PENNYLANE_TOKEN"]
SUPABASE_URL     = os.environ["SUPABASE_URL"]
SUPABASE_KEY     = os.environ["SUPABASE_KEY"]
SYNC_WINDOW_DAYS = int(os.getenv("SYNC_WINDOW_DAYS", "7"))
BASE_URL         = "https://app.pennylane.com/api/external/v2"


# ── Pennylane ──────────────────────────────────────────────────────────────

def pl_get_transactions(token, date_from, date_to):
    """Récupère toutes les transactions bancaires (catégories inline)."""
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    params  = {"per_page": 100, "date_gte": date_from.isoformat(), "date_lte": date_to.isoformat()}
    rows = []
    while True:
        r = requests.get(f"{BASE_URL}/transactions", headers=headers, params=params)
        r.raise_for_status()
        data = r.json()
        rows.extend(data.get("items", []))
        if not data.get("has_more"):
            break
        params["cursor"] = data["next_cursor"]
        time.sleep(0.26)
    return rows


# ── Normalisation ──────────────────────────────────────────────────────────

def normalize_transaction(tx):
    """Convertit une transaction Pennylane en ligne Supabase."""
    amount = float(tx.get("currency_amount") or tx.get("amount") or 0)
    direction = "credit" if amount >= 0 else "debit"

    # Première catégorie (weight la plus haute si plusieurs)
    cats = tx.get("categories") or []
    if cats:
        cats_sorted = sorted(cats, key=lambda c: float(c.get("weight", 0)), reverse=True)
        cat = cats_sorted[0]
        category_name = cat.get("label", "")
        category_id   = str(cat.get("id", ""))
    else:
        category_name = ""
        category_id   = ""

    return {
        "id":            f"tx_{tx['id']}",
        "date":          tx["date"],
        "label":         tx.get("label") or "",
        "amount":        abs(amount),
        "direction":     direction,
        "currency":      tx.get("currency", "EUR"),
        "category_name": category_name,
        "category_id":   category_id,
        "account_id":    str((tx.get("bank_account") or {}).get("id", "")),
        "account_name":  "",
        "source_type":   "transaction",
        "source_id":     str(tx["id"]),
    }


# ── Agrégation P&L ────────────────────────────────────────────────────────

def compute_pl_daily(sb, mapping, target_date):
    date_str = target_date.isoformat()
    rows = sb.table("transactions").select("*").eq("date", date_str).execute().data

    aggregated = {}
    for tx in rows:
        m = mapping.get(tx.get("category_name", ""))
        if not m or m.get("pl_section") is None:
            continue

        key = m["poste_budgetaire"]
        if key not in aggregated:
            aggregated[key] = {
                "date":             date_str,
                "poste_budgetaire": key,
                "axe2_pole":        m.get("axe2_pole"),
                "axe3_analytics":   m.get("axe3_analytics"),
                "pl_section":       m.get("pl_section"),
                "is_revenue":       m.get("is_revenue", False),
                "amount":           0.0,
                "tx_count":         0,
            }
        sign = 1 if tx["direction"] == "credit" else -1
        aggregated[key]["amount"]   += sign * float(tx["amount"] or 0)
        aggregated[key]["tx_count"] += 1

    if aggregated:
        sb.table("pl_daily").upsert(list(aggregated.values()), on_conflict="date,poste_budgetaire").execute()
        log.info(f"  pl_daily : {len(aggregated)} postes — {date_str}")


def compute_kpis(sb, target_date):
    date_str = target_date.isoformat()
    rows = sb.table("pl_daily").select("pl_section,is_revenue,amount,axe3_analytics").eq("date", date_str).execute().data

    totals = {1: 0.0, 2: 0.0, 3: 0.0, 4: 0.0, 5: 0.0}
    total_pub = 0.0
    for r in rows:
        s = r.get("pl_section")
        if s:
            totals[s] += float(r["amount"] or 0)
        if r.get("axe3_analytics") in ("Pub_Meta", "Pub_Event", "Affiliés"):
            total_pub += abs(float(r["amount"] or 0))

    ca              = totals[1]
    total_acq       = abs(totals[2])
    total_sales     = abs(totals[3])
    total_ops       = abs(totals[4])
    total_structure = abs(totals[5])
    total_charges   = total_acq + total_sales + total_ops + total_structure
    mc1             = ca - total_acq
    mc2             = mc1 - total_sales
    marge_brute     = mc2 - total_ops
    ebitda          = marge_brute - total_structure

    def pct(v, b):
        if not b: return None
        r = round(v / b * 100, 2)
        return r if abs(r) < 1000 else None  # NUMERIC(5,2) max ±999.99

    sb.table("kpis_daily").upsert({
        "date":               date_str,
        "ca_ht":              round(ca, 2),
        "total_acquisition":  round(total_acq, 2),
        "total_sales":        round(total_sales, 2),
        "total_ops":          round(total_ops, 2),
        "total_structure":    round(total_structure, 2),
        "total_charges":      round(total_charges, 2),
        "mc1":                round(mc1, 2),  "mc1_pct": pct(mc1, ca),
        "mc2":                round(mc2, 2),  "mc2_pct": pct(mc2, ca),
        "marge_brute":        round(marge_brute, 2), "marge_brute_pct": pct(marge_brute, ca),
        "ebitda":             round(ebitda, 2), "ebitda_pct": pct(ebitda, ca),
        "roas_cash":          round(ca / total_pub, 2) if total_pub else None,
    }, on_conflict="date").execute()

    log.info(f"  kpis : CA={ca:.0f}€  EBITDA={ebitda:.0f}€ ({pct(ebitda,ca)}%)  — {date_str}")


# ── Main ──────────────────────────────────────────────────────────────────

def run(date_from=None, date_to=None):
    date_to   = date_to   or date.today()
    date_from = date_from or date_to - timedelta(days=SYNC_WINDOW_DAYS)
    log.info(f"Sync {date_from} → {date_to}")

    sb      = create_client(SUPABASE_URL, SUPABASE_KEY)
    mapping = {r["pennylane_category_name"]: r for r in sb.table("category_mapping").select("*").execute().data}
    log.info(f"Mapping : {len(mapping)} catégories chargées")

    log.info("Récupération transactions Pennylane…")
    transactions = pl_get_transactions(PENNYLANE_TOKEN, date_from, date_to)
    log.info(f"  {len(transactions)} transactions récupérées")

    rows = [normalize_transaction(tx) for tx in transactions]
    rows = [r for r in rows if r]

    if rows:
        sb.table("transactions").upsert(rows, on_conflict="id").execute()
        log.info(f"  {len(rows)} transactions upsertées dans Supabase")

    current = date_from
    while current <= date_to:
        compute_pl_daily(sb, mapping, current)
        compute_kpis(sb, current)
        current += timedelta(days=1)

    log.info("✅ Sync terminé.")


if __name__ == "__main__":
    args = sys.argv[1:]
    if len(args) == 2:
        from datetime import datetime
        run(datetime.strptime(args[0], "%Y-%m-%d").date(),
            datetime.strptime(args[1], "%Y-%m-%d").date())
    else:
        run()
