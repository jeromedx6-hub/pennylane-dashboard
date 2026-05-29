#!/usr/bin/env python3
"""
Sync quotidien Pennylane → Supabase  (via transactions bancaires)
Cron Railway : 0 6 * * *
"""
import os, sys, logging, time, json
from collections import defaultdict
from datetime import date, timedelta, datetime as _dt

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

    # DELETE + INSERT (pas upsert) pour supprimer les postes qui n'ont
    # plus de transactions (ex : tx supprimée dans Pennylane depuis le dernier sync)
    sb.table("pl_daily").delete().eq("date", date_str).execute()
    if aggregated:
        sb.table("pl_daily").insert(list(aggregated.values())).execute()
        log.info(f"  pl_daily : {len(aggregated)} postes — {date_str}")
    else:
        log.info(f"  pl_daily : 0 postes — {date_str} (toutes lignes supprimées)")


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


# ── Réconciliation hybride MCP ↔ API ──────────────────────────────────────

def verify_sync(token, sb, date_from, date_to, pl_transactions=None):
    """
    Réconciliation hybride : compare Pennylane API vs Supabase par jour.
    Vérifie le nb de transactions ET les montants totaux.
    Auto-corrige les jours désynchronisés (upsert Pennylane + delete ghosts + recalcul P&L).
    Écrit le rapport dans la table sync_audit.
    """
    log.info("🔍 Réconciliation Pennylane ↔ Supabase…")

    # ── 1. Source Pennylane ───────────────────────────────────────────────
    pl_txs = pl_transactions if pl_transactions is not None \
             else pl_get_transactions(token, date_from, date_to)

    pl_by_day = defaultdict(lambda: {"count": 0, "amount": 0.0})
    for tx in pl_txs:
        d      = tx["date"]
        amount = abs(float(tx.get("currency_amount") or tx.get("amount") or 0))
        pl_by_day[d]["count"]  += 1
        pl_by_day[d]["amount"] += amount

    # ── 2. Source Supabase (pagination) ───────────────────────────────────
    d_from_str, d_to_str = date_from.isoformat(), date_to.isoformat()
    sb_rows, page, size = [], 0, 1000
    while page < 30:
        batch = (
            sb.table("transactions")
            .select("date, amount, id")
            .gte("date", d_from_str)
            .order("date")
            .range(page * size, (page + 1) * size - 1)
            .execute().data
        )
        sb_rows.extend(r for r in batch if r["date"] <= d_to_str)
        if len(batch) < size:
            break
        if batch and batch[-1]["date"] > d_to_str:
            break
        page += 1

    sb_by_day = defaultdict(lambda: {"count": 0, "amount": 0.0, "ids": []})
    for tx in sb_rows:
        d = tx["date"]
        sb_by_day[d]["count"]  += 1
        sb_by_day[d]["amount"] += abs(float(tx["amount"] or 0))
        sb_by_day[d]["ids"].append(tx["id"])

    # ── 3. Détection des écarts ───────────────────────────────────────────
    gaps = []
    current = date_from
    while current <= date_to:
        d    = current.isoformat()
        pl_d = pl_by_day.get(d,  {"count": 0, "amount": 0.0})
        sb_d = sb_by_day.get(d,  {"count": 0, "amount": 0.0, "ids": []})

        count_diff  = abs(pl_d["count"]  - sb_d["count"])
        amount_diff = abs(pl_d["amount"] - sb_d["amount"])

        if count_diff > 0 or amount_diff > 0.50:   # tolérance 50 centimes
            gaps.append({
                "date":        d,
                "pl_count":    pl_d["count"],
                "sb_count":    sb_d["count"],
                "pl_amount":   round(pl_d["amount"], 2),
                "sb_amount":   round(sb_d["amount"], 2),
                "amount_diff": round(amount_diff, 2),
            })
        current += timedelta(days=1)

    # ── 4. Auto-correction des jours en écart ─────────────────────────────
    auto_fixed = []
    if gaps:
        log.warning(f"  ⚠️  {len(gaps)} jour(s) désynchronisé(s) → auto-correction")
        mapping = {r["pennylane_category_name"]: r
                   for r in sb.table("category_mapping").select("*").execute().data}

        for gap in gaps:
            d        = gap["date"]
            gap_date = date.fromisoformat(d)
            day_pl   = [tx for tx in pl_txs if tx["date"] == d]
            day_pl_ids = {f"tx_{tx['id']}" for tx in day_pl}

            # Upsert les transactions Pennylane du jour
            normalized = [r for r in (normalize_transaction(tx) for tx in day_pl) if r]
            if normalized:
                sb.table("transactions").upsert(normalized, on_conflict="id").execute()

            # Supprimer ghost rows : présents dans Supabase mais plus dans Pennylane
            sb_ids_day = sb_by_day.get(d, {}).get("ids", [])
            ghost_ids  = [i for i in sb_ids_day if i not in day_pl_ids]
            if ghost_ids:
                log.warning(f"    🗑️  {len(ghost_ids)} ghost(s) supprimé(s) le {d}: {ghost_ids}")
                for gid in ghost_ids:
                    sb.table("transactions").delete().eq("id", gid).execute()

            # Recalcul P&L + KPIs
            compute_pl_daily(sb, mapping, gap_date)
            compute_kpis(sb, gap_date)
            auto_fixed.append(d)
            log.info(f"    ✅ {d} corrigé — PL:{gap['pl_count']} tx / SB:{gap['sb_count']} tx"
                     f" / écart montant:{gap['amount_diff']}€")
    else:
        log.info("  ✅ Réconciliation OK — aucun écart détecté")

    # ── 5. Écriture audit trail ───────────────────────────────────────────
    status = "auto_fixed" if auto_fixed else ("gaps_found" if gaps else "ok")
    n_days = (date_to - date_from).days + 1
    audit_row = {
        "checked_at": _dt.utcnow().isoformat() + "Z",
        "date_from":  date_from.isoformat(),
        "date_to":    date_to.isoformat(),
        "status":     status,
        "gaps":       gaps,
        "summary":    f"{len(gaps)} écart(s) sur {n_days} jour(s) — {len(auto_fixed)} auto-corrigé(s)",
    }
    try:
        sb.table("sync_audit").insert(audit_row).execute()
    except Exception as e:
        log.warning(f"  sync_audit write failed (table créée ?) : {e}")

    log.info(f"  Audit : {status} | {audit_row['summary']}")
    return {"status": status, "gaps": gaps, "auto_fixed": auto_fixed}


# ── Customer invoices ─────────────────────────────────────────────────────

def sync_customer_invoices(token, sb, date_from, date_to):
    """Sync factures clients Pennylane → customer_invoices_sync (nouveaux vs récurrents)."""
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    params  = {"per_page": 100, "date_gte": date_from.isoformat(), "date_lte": date_to.isoformat()}

    log.info("Récupération customer_invoices Pennylane…")
    invoices = []
    while True:
        r = requests.get(f"{BASE_URL}/customer_invoices", headers=headers, params=params)
        r.raise_for_status()
        data = r.json()
        invoices.extend(data.get("items", []))
        if not data.get("has_more"):
            break
        params["cursor"] = data["next_cursor"]
        time.sleep(0.26)

    log.info(f"  {len(invoices)} customer_invoices récupérées")
    if not invoices:
        return

    rows = []
    for inv in invoices:
        customer = inv.get("customer") or inv.get("third_party") or {}
        cid = str(customer.get("id", "")).strip()
        if not cid or cid == "None":
            continue
        amount = float(inv.get("amount") or inv.get("currency_amount") or 0)
        rows.append({
            "id":            f"ci_{inv['id']}",
            "customer_id":   cid,
            "customer_name": customer.get("name", ""),
            "date":          inv.get("date", ""),
            "amount":        abs(amount),
            "status":        inv.get("status", ""),
            "currency":      inv.get("currency", "EUR"),
        })

    if rows:
        sb.table("customer_invoices_sync").upsert(rows, on_conflict="id").execute()
        log.info(f"  {len(rows)} customer_invoices upsertées")


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

    sync_customer_invoices(PENNYLANE_TOKEN, sb, date_from, date_to)

    # Réconciliation hybride : vérifie la cohérence Pennylane ↔ Supabase
    # Réutilise les transactions déjà fetchées → 0 appel API supplémentaire
    audit = verify_sync(PENNYLANE_TOKEN, sb, date_from, date_to,
                        pl_transactions=transactions)

    log.info("✅ Sync terminé.")
    return audit


if __name__ == "__main__":
    args = sys.argv[1:]
    if len(args) == 2:
        from datetime import datetime
        run(datetime.strptime(args[0], "%Y-%m-%d").date(),
            datetime.strptime(args[1], "%Y-%m-%d").date())
    else:
        run()
