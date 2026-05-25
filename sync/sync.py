#!/usr/bin/env python3
"""
Sync quotidien Pennylane → Supabase
Cron Railway : 0 6 * * *
"""
import os
import sys
import logging
from datetime import date, timedelta
from decimal import Decimal

from supabase import create_client, Client
from pennylane_client import PennylaneClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

PENNYLANE_TOKEN  = os.environ["PENNYLANE_TOKEN"]
SUPABASE_URL     = os.environ["SUPABASE_URL"]
SUPABASE_KEY     = os.environ["SUPABASE_KEY"]

# Nombre de jours à re-syncer (pour corriger les transactions en retard)
SYNC_WINDOW_DAYS = int(os.getenv("SYNC_WINDOW_DAYS", "7"))


def get_clients() -> tuple[PennylaneClient, Client]:
    pl = PennylaneClient(PENNYLANE_TOKEN)
    sb = create_client(SUPABASE_URL, SUPABASE_KEY)
    return pl, sb


def get_mapping(sb: Client) -> dict:
    """Retourne le mapping catégorie_name → infos analytiques."""
    rows = sb.table("category_mapping").select("*").execute().data
    return {r["pennylane_category_name"]: r for r in rows}


def normalize_invoice(inv: dict, source_type: str, mapping: dict) -> dict | None:
    """Transforme une facture Pennylane en ligne transactions."""
    try:
        category_name = (
            inv.get("category", {}) or {}
        ).get("name") or inv.get("category_name", "")

        amount_raw = inv.get("amount") or inv.get("total_amount") or 0
        amount = Decimal(str(amount_raw))

        direction = "credit" if source_type == "customer_invoice" else "debit"

        return {
            "id":            f"{source_type}_{inv['id']}",
            "date":          inv.get("date") or inv.get("invoice_date"),
            "label":         inv.get("label") or inv.get("subject") or "",
            "amount":        float(amount),
            "direction":     direction,
            "currency":      inv.get("currency", "EUR"),
            "category_name": category_name,
            "category_id":   str((inv.get("category") or {}).get("id", "")),
            "account_id":    str(inv.get("account_id", "")),
            "account_name":  inv.get("account_name", ""),
            "source_type":   source_type,
            "source_id":     str(inv["id"]),
        }
    except Exception as e:
        log.warning(f"Skipping {source_type} {inv.get('id')}: {e}")
        return None


def upsert_transactions(sb: Client, rows: list[dict]):
    if not rows:
        return
    sb.table("transactions").upsert(rows, on_conflict="id").execute()
    log.info(f"  Upserted {len(rows)} transactions")


def compute_pl_daily(sb: Client, mapping: dict, target_date: date):
    """Agrège les transactions du jour par poste et écrit dans pl_daily."""
    date_str = target_date.isoformat()

    rows = (
        sb.table("transactions")
        .select("*")
        .eq("date", date_str)
        .execute()
        .data
    )

    # Agrégation par poste
    aggregated: dict[str, dict] = {}
    for tx in rows:
        cat = tx.get("category_name", "")
        m = mapping.get(cat)
        if not m:
            poste = f"[Non mappé] {cat}" if cat else "[Non catégorisé]"
            key = poste
            if key not in aggregated:
                aggregated[key] = {
                    "date": date_str,
                    "poste_budgetaire": poste,
                    "axe2_pole": None,
                    "axe3_analytics": None,
                    "pl_section": None,
                    "is_revenue": False,
                    "amount": 0,
                    "tx_count": 0,
                }
        else:
            key = m["poste_budgetaire"]
            if key not in aggregated:
                aggregated[key] = {
                    "date": date_str,
                    "poste_budgetaire": key,
                    "axe2_pole":       m.get("axe2_pole"),
                    "axe3_analytics":  m.get("axe3_analytics"),
                    "pl_section":      m.get("pl_section"),
                    "is_revenue":      m.get("is_revenue", False),
                    "amount": 0,
                    "tx_count": 0,
                }

        sign = 1 if tx["direction"] == "credit" else -1
        aggregated[key]["amount"]   += sign * tx["amount"]
        aggregated[key]["tx_count"] += 1

    if aggregated:
        sb.table("pl_daily").upsert(
            list(aggregated.values()),
            on_conflict="date,poste_budgetaire"
        ).execute()
        log.info(f"  pl_daily : {len(aggregated)} postes pour {date_str}")


def compute_kpis(sb: Client, target_date: date):
    """Calcule les KPIs du jour depuis pl_daily et écrit dans kpis_daily."""
    date_str = target_date.isoformat()

    rows = (
        sb.table("pl_daily")
        .select("pl_section, is_revenue, amount")
        .eq("date", date_str)
        .execute()
        .data
    )

    totals = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
    for r in rows:
        section = r.get("pl_section")
        if section:
            totals[section] += float(r["amount"] or 0)

    ca               = totals[1]
    total_acq        = abs(totals[2])
    total_sales      = abs(totals[3])
    total_ops        = abs(totals[4])
    total_structure  = abs(totals[5])
    total_charges    = total_acq + total_sales + total_ops + total_structure

    mc1          = ca - total_acq
    mc2          = mc1 - total_sales
    marge_brute  = mc2 - total_ops
    ebitda       = marge_brute - total_structure

    def pct(val, base):
        return round(val / base * 100, 2) if base else None

    # ROAS : CA / dépenses pub (Axe3 = Pub_Meta ou Pub_Google)
    pub_rows = (
        sb.table("pl_daily")
        .select("amount")
        .eq("date", date_str)
        .in_("axe3_analytics", ["Pub_Meta", "Pub_Google"])
        .execute()
        .data
    )
    total_pub = sum(abs(float(r["amount"] or 0)) for r in pub_rows)
    roas = round(ca / total_pub, 2) if total_pub else None

    kpi = {
        "date":               date_str,
        "ca_ht":              ca,
        "total_acquisition":  total_acq,
        "total_sales":        total_sales,
        "total_ops":          total_ops,
        "total_structure":    total_structure,
        "total_charges":      total_charges,
        "mc1":                mc1,
        "mc1_pct":            pct(mc1, ca),
        "mc2":                mc2,
        "mc2_pct":            pct(mc2, ca),
        "marge_brute":        marge_brute,
        "marge_brute_pct":    pct(marge_brute, ca),
        "ebitda":             ebitda,
        "ebitda_pct":         pct(ebitda, ca),
        "roas_cash":          roas,
    }

    sb.table("kpis_daily").upsert(kpi, on_conflict="date").execute()
    log.info(f"  kpis_daily : EBITDA={ebitda:.0f}€ ({pct(ebitda,ca)}%) pour {date_str}")


def run(date_from: date = None, date_to: date = None):
    date_to   = date_to   or date.today()
    date_from = date_from or date_to - timedelta(days=SYNC_WINDOW_DAYS)

    log.info(f"Sync {date_from} → {date_to}")
    pl, sb = get_clients()
    mapping = get_mapping(sb)
    log.info(f"Mapping chargé : {len(mapping)} catégories")

    # 1. Récupère les transactions
    customer = pl.get_customer_invoices(date_from, date_to)
    supplier = pl.get_supplier_invoices(date_from, date_to)
    log.info(f"Pennylane : {len(customer)} factures clients, {len(supplier)} fournisseurs")

    rows = []
    for inv in customer:
        n = normalize_invoice(inv, "customer_invoice", mapping)
        if n: rows.append(n)
    for inv in supplier:
        n = normalize_invoice(inv, "supplier_invoice", mapping)
        if n: rows.append(n)

    upsert_transactions(sb, rows)

    # 2. Recalcule pl_daily + kpis_daily pour chaque jour de la fenêtre
    current = date_from
    while current <= date_to:
        compute_pl_daily(sb, mapping, current)
        compute_kpis(sb, current)
        current += timedelta(days=1)

    log.info("Sync terminé.")


if __name__ == "__main__":
    # Usage : python sync.py [date_from YYYY-MM-DD] [date_to YYYY-MM-DD]
    args = sys.argv[1:]
    if len(args) == 2:
        from datetime import datetime
        run(
            datetime.strptime(args[0], "%Y-%m-%d").date(),
            datetime.strptime(args[1], "%Y-%m-%d").date(),
        )
    else:
        run()
