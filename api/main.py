import csv
import hashlib
import hmac
import io
import json
import logging
import os
import re
import sys as _sys
import threading
import calendar as _cal
from collections import defaultdict
from datetime import date, timedelta, datetime as _dt
from fastapi import FastAPI, Query, Request, Header
from fastapi.responses import StreamingResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from supabase import create_client

# Rend sync.py importable depuis l'API
_sync_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'sync')
if _sync_dir not in _sys.path:
    _sys.path.insert(0, _sync_dir)

_sync_lock  = threading.Lock()
_sync_state = {"running": False, "started_at": None, "finished_at": None,
               "error": None, "date_from": None, "date_to": None, "audit": None}

app = FastAPI(title="FinBoard API")

# ── Helpers extraction email / nom depuis libellé Pennylane ────
_EMAIL_RE = re.compile(r'[\w.+%-]+@[\w.-]+\.[a-z]{2,}', re.IGNORECASE)
_NAME_RE  = re.compile(r'-\s+(.+?)\s+-\s+[\w.+%-]+@[\w.-]+', re.IGNORECASE)

def _extract_email(label: str):
    m = _EMAIL_RE.search(label or "")
    return m.group(0).lower() if m else None

def _extract_name(label: str):
    m = _NAME_RE.search(label or "")
    return m.group(1).strip() if m else None

def _add_months(d: date, n: int) -> date:
    month = d.month + n
    year  = d.year + (month - 1) // 12
    month = (month - 1) % 12 + 1
    return date(year, month, 1)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

# ── Filtre catégories techniques Pennylane ─────────────────────────────────────
# Seules les familles purement techniques sont exclues (trésorerie, tests, TVA).
# Toutes les autres catégories sont affichées — les non classifiables sont
# regroupées sous "Divers" côté frontend.
_CAT_IGNORED_FAMILIES = {
    "Suivi de trésorerie", "Test", "Test 156",
    "Transfert interne", "TVA",
}

def _cat_is_technical(c: dict) -> bool:
    """Retourne True si la catégorie est purement technique/hors P&L."""
    return c.get("family_label") in _CAT_IGNORED_FAMILIES


# ── Auto-sync release notes au démarrage ──────────────────────────────────────
def _sync_release_notes():
    """
    Lit releases.json et insère dans Supabase les entrées absentes.
    Dedup par details->>'uid'. Idempotent — safe à relancer à chaque deploy.
    """
    import json as _json
    releases_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'releases.json')
    try:
        with open(releases_file) as f:
            entries = _json.load(f)
    except (FileNotFoundError, _json.JSONDecodeError) as e:
        logging.warning(f"releases.json introuvable ou invalide : {e}")
        return

    # UIDs déjà en base
    existing = sb.table("release_notes").select("details").execute().data
    existing_uids = {
        r["details"].get("uid")
        for r in existing
        if r.get("details") and isinstance(r["details"], dict)
    }

    to_insert = []
    for e in entries:
        uid = e.get("uid")
        if not uid or uid in existing_uids:
            continue
        details = dict(e.get("details") or {})
        details["uid"] = uid          # stocke le uid dans details pour dedup futur
        to_insert.append({
            "released_at": e["released_at"],
            "category":    e["category"],
            "title":       e["title"],
            "description": e.get("description", ""),
            "impact":      e.get("impact", "low"),
            "details":     details,
        })

    if to_insert:
        sb.table("release_notes").insert(to_insert).execute()
        logging.info(f"Release notes : {len(to_insert)} nouvelle(s) entrée(s) insérée(s)")
    else:
        logging.info("Release notes : déjà à jour")

try:
    _sync_release_notes()
except Exception as _e:
    logging.warning(f"_sync_release_notes échec (non bloquant) : {_e}")


# ── Auto-sync au démarrage si dernier sync > 23h ───────────────────────────
def _startup_auto_sync():
    """
    Déclenché en background au démarrage de l'API.
    Si last_synced_at > 23h (ou absent), lance un sync sur les 7 derniers jours.
    Remplace le cron Railway qui n'est pas fiable en single-service.
    """
    try:
        meta = sb.table("sync_meta").select("value").eq("key", "last_synced_at").execute().data
        if meta:
            last = _dt.fromisoformat(meta[0]["value"].rstrip("Z"))
            age_hours = (_dt.utcnow() - last).total_seconds() / 3600
            if age_hours < 23:
                logging.info(f"Auto-sync: dernier sync il y a {age_hours:.1f}h — skip")
                return
            logging.info(f"Auto-sync: dernier sync il y a {age_hours:.1f}h — lancement")
        else:
            logging.info("Auto-sync: pas de last_synced_at — lancement premier sync")

        today  = date.today()
        d_from = today - timedelta(days=7)
        d_to   = today

        with _sync_lock:
            if _sync_state["running"]:
                logging.info("Auto-sync: un sync est déjà en cours — skip")
                return
            _sync_state.update({
                "running": True, "started_at": _dt.now().strftime("%H:%M:%S"),
                "finished_at": None, "error": None,
                "date_from": d_from.isoformat(), "date_to": d_to.isoformat(),
            })

        import sync as _sync_mod
        try:
            audit = _sync_mod.run(d_from, d_to)
            with _sync_lock:
                _sync_state.update({"running": False,
                                    "finished_at": _dt.now().strftime("%H:%M:%S"),
                                    "audit": audit})
            logging.info("Auto-sync: terminé avec succès")
        except Exception as e:
            with _sync_lock:
                _sync_state.update({"running": False, "error": str(e),
                                    "finished_at": _dt.now().strftime("%H:%M:%S")})
            logging.error(f"Auto-sync: échec — {e}")

    except Exception as e:
        logging.warning(f"Auto-sync: erreur inattendue — {e}")


threading.Thread(target=_startup_auto_sync, daemon=True).start()


def _month_range(month: str) -> tuple[str, str]:
    """'2025-05' → ('2025-05-01', '2025-05-31')"""
    y, m = int(month[:4]), int(month[5:7])
    first = date(y, m, 1)
    last  = date(y, m + 1, 1) - timedelta(days=1) if m < 12 else date(y, 12, 31)
    return first.isoformat(), last.isoformat()


@app.get("/api/kpis")
def get_kpis(month: str = Query(default=None), ytd: bool = Query(default=False)):
    """KPIs agrégés sur un mois ou en YTD Jan→aujourd'hui."""
    today = date.today()
    if ytd:
        d_from = f"{today.year}-01-01"
        d_to   = today.isoformat()
        month  = f"{today.year}-ytd"
    else:
        month  = month or today.strftime("%Y-%m")
        d_from, d_to = _month_range(month)

    rows = (
        sb.table("kpis_daily")
        .select("*")
        .gte("date", d_from)
        .lte("date", d_to)
        .execute()
        .data
    )

    if not rows:
        return {"month": month, "data": None}

    def s(key):
        return sum(float(r.get(key) or 0) for r in rows)

    ca = s("ca_ht")

    def pct(val):
        return round(val / ca * 100, 2) if ca else 0

    # ROAS : recalculé depuis pl_daily mensuel (pub payée en lump sum, pas quotidien)
    pl_pub = (
        sb.table("pl_daily")
        .select("amount")
        .gte("date", d_from)
        .lte("date", d_to)
        .in_("axe3_analytics", ["Pub_Meta", "Pub_Event", "Affiliés"])
        .execute()
        .data
    )
    total_pub = sum(abs(float(r["amount"] or 0)) for r in pl_pub)
    roas = round(ca / total_pub, 2) if total_pub else None

    # Nombre total de transactions brutes (COUNT sans fetch de données)
    try:
        res = (sb.table("transactions")
               .select("*", count="exact")
               .gte("date", d_from)
               .lte("date", d_to)
               .limit(0)
               .execute())
        tx_total_count = res.count or 0
    except Exception:
        tx_total_count = 0

    data = {
        "ca_ht":             round(ca, 2),
        "total_charges":     round(s("total_charges"), 2),
        "total_acquisition": round(s("total_acquisition"), 2),
        "total_sales":       round(s("total_sales"), 2),
        "total_ops":         round(s("total_ops"), 2),
        "total_structure":   round(s("total_structure"), 2),
        "mc1":               round(s("mc1"), 2),
        "mc1_pct":           pct(s("mc1")),
        "mc2":               round(s("mc2"), 2),
        "mc2_pct":           pct(s("mc2")),
        "marge_brute":       round(s("marge_brute"), 2),
        "marge_brute_pct":   pct(s("marge_brute")),
        "ebitda":            round(s("ebitda"), 2),
        "ebitda_pct":        pct(s("ebitda")),
        "roas_cash":         roas,
        "tx_count":          tx_total_count,
    }
    return {"month": month, "data": data}


@app.get("/api/pl")
def get_pl(month: str = Query(default=None), ytd: bool = Query(default=False)):
    """P&L analytique par poste — mois ou YTD Jan→aujourd'hui."""
    today = date.today()
    if ytd:
        d_from = f"{today.year}-01-01"
        d_to   = today.isoformat()
        month  = f"{today.year}-ytd"
    else:
        month  = month or today.strftime("%Y-%m")
        d_from, d_to = _month_range(month)

    rows = (
        sb.table("pl_daily")
        .select("poste_budgetaire, axe2_pole, axe3_analytics, pl_section, is_revenue, amount, tx_count")
        .gte("date", d_from)
        .lte("date", d_to)
        .execute()
        .data
    )

    aggregated: dict[str, dict] = {}
    for r in rows:
        key = r["poste_budgetaire"]
        if key not in aggregated:
            aggregated[key] = {
                "poste_budgetaire": key,
                "axe2_pole":        r["axe2_pole"],
                "axe3_analytics":   r["axe3_analytics"],
                "pl_section":       r["pl_section"],
                "is_revenue":       r["is_revenue"],
                "amount":           0,
                "tx_count":         0,
            }
        aggregated[key]["amount"]   += float(r["amount"] or 0)
        aggregated[key]["tx_count"] += int(r["tx_count"] or 0)

    # ── Encaissements non catégorisés (crédits category='Revenu', hors P&L CA) ──
    unc_rev_total, unc_rev_count = 0.0, 0
    pg = 0
    while True:
        batch = (
            sb.table("transactions").select("amount")
            .gte("date", d_from).lte("date", d_to)
            .eq("category_name", "Revenu").eq("direction", "credit")
            .range(pg * 1000, (pg + 1) * 1000 - 1).execute().data
        )
        for tx in batch:
            unc_rev_total += float(tx["amount"] or 0)
            unc_rev_count += 1
        if len(batch) < 1000:
            break
        pg += 1

    if unc_rev_total > 0.5:
        aggregated["_uncategorized_revenue"] = {
            "poste_budgetaire": "⚠ Encaissements non catégorisés",
            "axe2_pole":        None,
            "axe3_analytics":   "_uncategorized_revenue",
            "pl_section":       1,
            "is_revenue":       True,
            "amount":           round(unc_rev_total, 2),
            "tx_count":         unc_rev_count,
        }

    # ── Dépenses non catégorisées (débits category vide, hors P&L charges) ──
    unc_exp_total, unc_exp_count = 0.0, 0
    pg = 0
    while True:
        batch = (
            sb.table("transactions").select("amount")
            .gte("date", d_from).lte("date", d_to)
            .eq("category_name", "").eq("direction", "debit")
            .range(pg * 1000, (pg + 1) * 1000 - 1).execute().data
        )
        for tx in batch:
            unc_exp_total += float(tx["amount"] or 0)
            unc_exp_count += 1
        if len(batch) < 1000:
            break
        pg += 1

    if unc_exp_total > 0.5:
        aggregated["_uncategorized_expenses"] = {
            "poste_budgetaire": "⚠ Dépenses non catégorisées",
            "axe2_pole":        None,
            "axe3_analytics":   "_uncategorized_expenses",
            "pl_section":       5,
            "is_revenue":       False,
            "amount":           round(unc_exp_total, 2),
            "tx_count":         unc_exp_count,
        }

    return {"month": month, "data": sorted(aggregated.values(), key=lambda x: (x["pl_section"] or 9, x["poste_budgetaire"]))}


@app.get("/api/uncategorized_detail")
def get_uncategorized_detail(
    month: str = Query(default=None),
    ytd:   bool = Query(default=False),
    type:  str  = Query(default="revenue"),   # "revenue" | "expenses"
):
    """Détail des transactions non catégorisées (pour modal drill-down + export)."""
    today = date.today()
    if ytd:
        d_from = f"{today.year}-01-01"
        d_to   = today.isoformat()
    else:
        month  = month or today.strftime("%Y-%m")
        d_from, d_to = _month_range(month)

    def _fetch_all(extra_filters: list) -> list:
        """Paginate a query built with a list of (method, *args) filter calls."""
        out, pg = [], 0
        while True:
            q = (sb.table("transactions")
                 .select("date,label,amount,direction,category_name,third_party")
                 .gte("date", d_from).lte("date", d_to)
                 .order("date", desc=True))
            for method, *args in extra_filters:
                q = getattr(q, method)(*args)
            batch = q.range(pg * 1000, (pg + 1) * 1000 - 1).execute().data
            out.extend(batch)
            if len(batch) < 1000:
                break
            pg += 1
        return out

    if type == "revenue":
        rows = _fetch_all([("eq", "category_name", "Revenu"), ("eq", "direction", "credit")])
    elif type == "expenses":
        rows = _fetch_all([("eq", "category_name", ""), ("eq", "direction", "debit")])
    else:  # "all"
        rev  = _fetch_all([("eq", "category_name", "Revenu"), ("eq", "direction", "credit")])
        exp  = _fetch_all([("eq", "category_name", ""), ("eq", "direction", "debit")])
        rows = sorted(rev + exp, key=lambda r: r["date"], reverse=True)

    total = sum(float(r["amount"] or 0) * (1 if r["direction"] == "credit" else -1) for r in rows)
    return {"type": type, "data": rows, "total": round(total, 2), "count": len(rows)}


@app.get("/api/evolution")
def get_evolution(months: int = Query(default=12)):
    """Évolution mensuelle CA / Charges / EBITDA sur N mois."""
    today = date.today()
    d_from = date(today.year, today.month, 1) - timedelta(days=months * 31)

    rows = (
        sb.table("kpis_monthly")
        .select("month, ca_ht, total_acquisition, total_sales, total_ops, total_structure, ebitda")
        .gte("month", d_from.isoformat())
        .order("month")
        .execute()
        .data
    )
    return {"data": rows}


@app.get("/api/charges_breakdown")
def get_charges_breakdown(month: str = Query(default=None)):
    """Répartition des charges par pôle pour un mois."""
    month = month or date.today().strftime("%Y-%m")
    d_from, d_to = _month_range(month)

    rows = (
        sb.table("pl_daily")
        .select("axe2_pole, amount")
        .gte("date", d_from)
        .lte("date", d_to)
        .eq("is_revenue", False)
        .execute()
        .data
    )

    by_pole: dict[str, float] = {}
    for r in rows:
        pole = r.get("axe2_pole") or "Autre"
        by_pole[pole] = by_pole.get(pole, 0) + abs(float(r["amount"] or 0))

    total = sum(by_pole.values())
    result = [
        {"pole": k, "amount": round(v, 2), "pct": round(v / total * 100, 1) if total else 0}
        for k, v in sorted(by_pole.items(), key=lambda x: -x[1])
    ]
    return {"month": month, "total": round(total, 2), "data": result}


def _fill_months(year: str, by_month: dict, is_revenue=None) -> list:
    """Retourne tous les mois de janvier au mois courant (ou déc si année passée)."""
    today = date.today()
    max_m = today.month if int(year) == today.year else 12
    result, ytd = [], 0.0
    for m in range(1, max_m + 1):
        key = f"{year}-{m:02d}"
        entry = by_month.get(key, {"amount": 0.0, "tx_count": 0})
        ytd += entry["amount"]
        result.append({
            "month":      key,
            "amount":     round(entry["amount"], 2),
            "tx_count":   entry.get("tx_count", 0),
            "ytd":        round(ytd, 2),
            "is_revenue": is_revenue,
        })
    return result


@app.get("/api/pl_line")
def get_pl_line(poste: str = Query(...), year: str = Query(default=None)):
    """Évolution mensuelle d'un poste P&L (mensuel + YTD cumulé, tous les mois)."""
    year = year or str(date.today().year)
    d_from, d_to = f"{year}-01-01", f"{year}-12-31"

    rows = (
        sb.table("pl_daily")
        .select("date, amount, tx_count, is_revenue")
        .eq("poste_budgetaire", poste)
        .gte("date", d_from).lte("date", d_to)
        .execute().data
    )

    by_month: dict[str, dict] = {}
    is_revenue_val = None
    for r in rows:
        m = r["date"][:7]
        if m not in by_month:
            by_month[m] = {"amount": 0.0, "tx_count": 0}
            if is_revenue_val is None:
                is_revenue_val = r.get("is_revenue")
        by_month[m]["amount"]   += float(r["amount"] or 0)
        by_month[m]["tx_count"] += int(r["tx_count"] or 0)

    return {"poste": poste, "year": year, "data": _fill_months(year, by_month, is_revenue_val)}


@app.get("/api/pl_line_transactions")
def get_pl_line_transactions(poste: str = Query(...), year: str = Query(default=None)):
    """Liste des transactions Pennylane sous-jacentes à un poste P&L.
    Filtre catégories en Python (évite le bug .in_() SDK Supabase sur accents).
    Pagination .range() pour dépasser la limite 1000 lignes.
    """
    year = year or str(date.today().year)
    d_from, d_to = f"{year}-01-01", f"{year}-12-31"

    mapping = (
        sb.table("category_mapping")
        .select("pennylane_category_name")
        .eq("poste_budgetaire", poste)
        .execute().data
    )
    if not mapping:
        return {"poste": poste, "year": year, "data": []}

    cat_set = {r["pennylane_category_name"] for r in mapping}

    # Fetch sans .in_() ni double filtre date — pagination + filtre Python
    rows, page, size = [], 0, 1000
    while page < 20:
        batch = (
            sb.table("transactions")
            .select("date, label, amount, direction, account_name, category_name")
            .gte("date", d_from)
            .order("date", desc=False)
            .range(page * size, (page + 1) * size - 1)
            .execute().data
        )
        # Filtre Python : date range + catégorie
        relevant = [r for r in batch if r["date"] <= d_to and r.get("category_name") in cat_set]
        rows.extend(relevant)
        if len(batch) < size:
            break
        # Arrêt anticipé si toutes les lignes restantes sont après d_to
        if batch and batch[-1]["date"] > d_to:
            break
        page += 1

    return {"poste": poste, "year": year, "data": rows}


@app.get("/api/debug_pl")
def debug_pl(poste: str = Query(...), month: str = Query(...)):
    """Diagnostic : compare pl_daily vs transactions pour un poste+mois."""
    d_from = f"{month}-01"
    # Fin du mois
    y, m = int(month[:4]), int(month[5:7])
    import calendar
    d_to = f"{month}-{calendar.monthrange(y, m)[1]:02d}"

    # pl_daily pour ce poste ce mois
    pl_rows = (
        sb.table("pl_daily")
        .select("date, amount, tx_count")
        .eq("poste_budgetaire", poste)
        .gte("date", d_from)
        .execute().data
    )
    pl_rows = [r for r in pl_rows if r["date"] <= d_to]
    pl_total = sum(float(r["amount"] or 0) for r in pl_rows)

    # Catégories de ce poste
    cat_mapping = sb.table("category_mapping").select("pennylane_category_name").eq("poste_budgetaire", poste).execute().data
    cat_set = {r["pennylane_category_name"] for r in cat_mapping}

    # Transactions brutes (Python filter)
    all_txs = (
        sb.table("transactions")
        .select("date, label, amount, direction, category_name")
        .gte("date", d_from)
        .order("date")
        .execute().data
    )
    tx_rows = [t for t in all_txs if t["date"] <= d_to and t.get("category_name") in cat_set]
    tx_total = sum(
        (1 if t["direction"] == "credit" else -1) * float(t["amount"] or 0)
        for t in tx_rows
    )

    return {
        "poste":      poste,
        "month":      month,
        "cat_names":  list(cat_set),
        "pl_daily":   {"rows": pl_rows, "total": round(pl_total, 2)},
        "transactions": {"count": len(tx_rows), "total": round(tx_total, 2), "rows": tx_rows},
    }


@app.get("/api/pl_section")
def get_pl_section(
    section: int  = Query(...),
    year:    str  = Query(default=None),
    revenue: bool = Query(default=False),
):
    """Évolution mensuelle du total d'une section P&L (charges ou revenus)."""
    year = year or str(date.today().year)
    d_from, d_to = f"{year}-01-01", f"{year}-12-31"

    rows = (
        sb.table("pl_daily")
        .select("date, amount")
        .eq("pl_section", section)
        .eq("is_revenue", revenue)
        .gte("date", d_from).lte("date", d_to)
        .execute().data
    )

    by_month: dict[str, dict] = {}
    for r in rows:
        m = r["date"][:7]
        if m not in by_month:
            by_month[m] = {"amount": 0.0, "tx_count": 0}
        by_month[m]["amount"] += float(r["amount"] or 0)

    return {"section": section, "year": year, "data": _fill_months(year, by_month, revenue)}


@app.get("/api/transactions")
def get_transactions(
    category: str = Query(default=None),
    date_from: str = Query(default=None),
    date_to: str = Query(default=None),
    limit: int = Query(default=50, le=200),
):
    """Transactions brutes pour le drill-down des modals."""
    q = sb.table("transactions").select("*").order("date", desc=True).limit(limit)
    if category:   q = q.eq("category_name", category)
    if date_from:  q = q.gte("date", date_from)
    if date_to:    q = q.lte("date", date_to)
    return {"data": q.execute().data}


@app.get("/api/debug_kpi")
def debug_kpi(date_from: str = Query(...), date_to: str = Query(...)):
    """Diagnostic : compte les transactions revenue dans la période."""
    rev_cats  = sb.table("category_mapping").select("pennylane_category_name").eq("is_revenue", True).execute().data
    cat_names = [r["pennylane_category_name"] for r in rev_cats]

    # Transactions brutes dans la période (sans filtre catégorie, sans filtre direction)
    all_period = (
        sb.table("transactions")
        .select("date, direction, category_name, label")
        .gte("date", date_from)
        .order("date")
        .execute().data
    )
    all_period = [t for t in all_period if t["date"] <= date_to]
    credit_period = [t for t in all_period if t.get("direction") == "credit"]
    rev_period    = [t for t in credit_period if t.get("category_name") in cat_names]
    with_email    = [t for t in rev_period if _extract_email(t.get("label", ""))]

    return {
        "n_rev_cats":        len(cat_names),
        "rev_cats_sample":   cat_names[:5],
        "n_all_period":      len(all_period),
        "n_credit_period":   len(credit_period),
        "n_rev_period":      len(rev_period),
        "n_with_email":      len(with_email),
        "sample":            rev_period[:2],
    }


@app.get("/api/kpi_clients")
def get_kpi_clients(
    date_from: str = Query(...),
    date_to:   str = Query(...),
):
    """
    4 segments clients — Nouveau / Récurrent / Nouveau produit / Impayés.
    Filtrage catégories en Python (le .in_() Supabase SDK échoue silencieusement
    sur les noms accentués). Pagination via .range() pour dépasser 1000 lignes.
    """
    DATE_HISTORY = "2026-01-01"

    # 1. Catégories revenus (set pour lookup O(1))
    rev_cats = sb.table("category_mapping").select("pennylane_category_name").eq("is_revenue", True).execute().data
    cat_set  = {r["pennylane_category_name"] for r in rev_cats}
    if not cat_set:
        return {"date_from": date_from, "date_to": date_to, "data": None}

    def _fetch_credit(gte_date, max_pages=15):
        """Récupère toutes les transactions credit depuis gte_date via pagination."""
        rows, page, size = [], 0, 1000
        while page < max_pages:
            batch = (
                sb.table("transactions")
                .select("date, label, amount, category_name, direction")
                .eq("direction", "credit")
                .gte("date", gte_date)
                .order("date")
                .range(page * size, (page + 1) * size - 1)
                .execute().data
            )
            rows.extend(batch)
            if len(batch) < size:
                break
            page += 1
        return rows

    # 2a. Transactions PÉRIODE (credit, filtre catégorie en Python)
    period_credit = _fetch_credit(date_from)
    period_raw    = [tx for tx in period_credit
                     if tx["date"] <= date_to and tx.get("category_name") in cat_set]

    # 2b. Historique AVANT la période (credit depuis DATE_HISTORY, filtre Python)
    history_credit = _fetch_credit(DATE_HISTORY)
    before_raw     = [tx for tx in history_credit
                      if tx["date"] < date_from and tx.get("category_name") in cat_set]

    if not period_raw:
        return {"date_from": date_from, "date_to": date_to, "data": {
            "new_clients": {"count": 0, "ca": 0.0},
            "recurring":   {"count": 0, "ca": 0.0},
            "new_product": {"count": 0, "ca": 0.0},
            "impayes":     {"count": 0, "details": [], "client_history": {}},
        }}

    # 3. Enrichir avec email + nom
    for tx in period_raw + before_raw:
        tx["email"] = _extract_email(tx.get("label", ""))
        tx["name"]  = _extract_name(tx.get("label", ""))

    period_ok = [tx for tx in period_raw if tx["email"]]
    before_ok = [tx for tx in before_raw if tx["email"]]

    # 4. Historique complet par client
    history = defaultdict(list)
    names   = {}
    for tx in before_ok + period_ok:
        history[tx["email"]].append(tx)
        if tx["email"] not in names and tx["name"]:
            names[tx["email"]] = tx["name"]

    # 5. Transactions dans la période
    period_txs = period_ok

    # 6. Emails et produits vus AVANT la période
    emails_before   = {tx["email"] for tx in before_ok}
    products_before = defaultdict(set)
    for tx in before_ok:
        products_before[tx["email"]].add(tx["category_name"])

    # 7. Classifier chaque client actif dans la période
    period_by_email = defaultdict(lambda: {"ca": 0.0, "cats": set()})
    for tx in period_txs:
        period_by_email[tx["email"]]["ca"]   += float(tx["amount"] or 0)
        period_by_email[tx["email"]]["cats"].add(tx["category_name"])

    new_clients = {"count": 0, "ca": 0.0}
    recurring   = {"count": 0, "ca": 0.0}
    new_product = {"count": 0, "ca": 0.0}

    for email, info in period_by_email.items():
        if email not in emails_before:
            new_clients["count"] += 1
            new_clients["ca"]    += info["ca"]
        else:
            old_prods = info["cats"] & products_before[email]
            nw_prods  = info["cats"] - products_before[email]
            if old_prods:
                recurring["count"] += 1
                recurring["ca"]    += sum(
                    float(tx["amount"] or 0) for tx in period_txs
                    if tx["email"] == email and tx["category_name"] in old_prods
                )
            if nw_prods:
                new_product["count"] += 1
                new_product["ca"]    += sum(
                    float(tx["amount"] or 0) for tx in period_txs
                    if tx["email"] == email and tx["category_name"] in nw_prods
                )

    # 8. Détection impayés (gaps mensuels + alerte dès le 8 du mois)
    today_d = date.today()
    df      = date.fromisoformat(date_from)
    dt      = date.fromisoformat(date_to)

    impayes_list   = []
    impayes_emails = set()

    for email, txs in history.items():
        by_prod = defaultdict(set)
        for tx in txs:
            m = date.fromisoformat(tx["date"]).replace(day=1)
            by_prod[tx["category_name"]].add(m)

        for prod, months_paid in by_prod.items():
            if len(months_paid) < 2:
                continue
            months_sorted = sorted(months_paid)

            # Gaps entre paiements consécutifs
            for i in range(len(months_sorted) - 1):
                m1, m2   = months_sorted[i], months_sorted[i + 1]
                expected = _add_months(m1, 1)
                if m2 > expected:
                    curr = expected
                    while curr < m2:
                        if df <= curr <= dt:
                            impayes_list.append({
                                "email":   email,
                                "name":    names.get(email, email),
                                "product": prod,
                                "month":   curr.isoformat(),
                                "active":  False,
                            })
                            impayes_emails.add(email)
                        curr = _add_months(curr, 1)

            # Mois courant : alerte si aujourd'hui >= 8 et paiement manquant
            if today_d.day >= 8:
                last_paid    = months_sorted[-1]
                expected_now = _add_months(last_paid, 1)
                current_m    = today_d.replace(day=1)
                if (expected_now == current_m
                        and current_m not in months_paid
                        and df <= current_m <= dt):
                    impayes_list.append({
                        "email":   email,
                        "name":    names.get(email, email),
                        "product": prod,
                        "month":   current_m.isoformat(),
                        "active":  True,
                    })
                    impayes_emails.add(email)

    # 9. Historique détaillé uniquement pour les clients en impayé
    client_history = {}
    for email in impayes_emails:
        client_history[email] = sorted([
            {
                "date":     tx["date"],
                "amount":   round(float(tx["amount"] or 0), 2),
                "category": tx["category_name"],
                "label":    tx.get("label", ""),
            }
            for tx in history[email]
        ], key=lambda x: x["date"])

    return {
        "date_from": date_from,
        "date_to":   date_to,
        "data": {
            "new_clients":  {"count": new_clients["count"], "ca": round(new_clients["ca"], 2)},
            "recurring":    {"count": recurring["count"],   "ca": round(recurring["ca"], 2)},
            "new_product":  {"count": new_product["count"], "ca": round(new_product["ca"], 2)},
            "impayes": {
                "count":          len(impayes_list),
                "details":        impayes_list,
                "client_history": client_history,
            },
        }
    }


@app.get("/api/customers_kpis")
def get_customers_kpis(
    date_from: str = Query(...),
    date_to:   str = Query(...),
):
    """Nouveaux clients vs récurrents sur une période — basé sur customer_invoices_sync."""
    # 1. Factures dans la période
    in_period = (
        sb.table("customer_invoices_sync")
        .select("customer_id, amount")
        .gte("date", date_from)
        .lte("date", date_to)
        .execute().data
    )
    if not in_period:
        return {"date_from": date_from, "date_to": date_to, "data": None}

    customer_ids = list({r["customer_id"] for r in in_period})

    # 2. Ces clients ont-ils une facture AVANT la période ? → récurrents
    before = (
        sb.table("customer_invoices_sync")
        .select("customer_id")
        .in_("customer_id", customer_ids)
        .lt("date", date_from)
        .execute().data
    )
    recurring_ids = {r["customer_id"] for r in before}

    # 3. Calcul CA nouveaux / récurrents
    new_clients  = set()
    ca_new       = 0.0
    ca_recurring = 0.0
    for inv in in_period:
        cid    = inv["customer_id"]
        amount = float(inv["amount"] or 0)
        if cid in recurring_ids:
            ca_recurring += amount
        else:
            new_clients.add(cid)
            ca_new += amount

    ca_total = ca_new + ca_recurring
    return {
        "date_from": date_from,
        "date_to":   date_to,
        "data": {
            "new_clients_count":   len(new_clients),
            "ca_new":              round(ca_new, 2),
            "ca_recurring":        round(ca_recurring, 2),
            "ca_total":            round(ca_total, 2),
            "pct_new":             round(ca_new / ca_total * 100, 1) if ca_total else 0,
            "pct_recurring":       round(ca_recurring / ca_total * 100, 1) if ca_total else 0,
            "avg_new_client_value": round(ca_new / len(new_clients), 2) if new_clients else 0,
        }
    }


@app.post("/api/sync")
def trigger_sync(
    date_from: str = Query(default=None),
    date_to:   str = Query(default=None),
):
    """Lance le sync Pennylane → Supabase en arrière-plan."""
    with _sync_lock:
        if _sync_state["running"]:
            return {"status": "already_running", "state": _sync_state}

    today  = date.today()
    d_to   = date.fromisoformat(date_to)   if date_to   else today
    d_from = date.fromisoformat(date_from) if date_from else d_to - timedelta(days=60)

    def _run():
        with _sync_lock:
            _sync_state["running"]     = True
            _sync_state["started_at"]  = _dt.now().strftime("%H:%M:%S")
            _sync_state["finished_at"] = None
            _sync_state["error"]       = None
            _sync_state["date_from"]   = d_from.isoformat()
            _sync_state["date_to"]     = d_to.isoformat()
        try:
            import sync as _sync_mod
            audit = _sync_mod.run(d_from, d_to)
            with _sync_lock:
                _sync_state["running"]     = False
                _sync_state["finished_at"] = _dt.now().strftime("%H:%M:%S")
                _sync_state["audit"]       = audit
        except Exception as e:
            with _sync_lock:
                _sync_state["running"]     = False
                _sync_state["error"]       = str(e)
                _sync_state["finished_at"] = _dt.now().strftime("%H:%M:%S")

    threading.Thread(target=_run, daemon=True).start()
    return {"status": "started", "date_from": d_from.isoformat(), "date_to": d_to.isoformat()}


@app.post("/api/sync/categories")
def sync_categories_only():
    """
    Sync rapide : récupère uniquement les catégories Pennylane → pennylane_categories.
    5-10 secondes. Permet de détecter les nouvelles catégories sans full sync.
    Synchrone (bloque jusqu'à la fin) pour retourner le résultat immédiatement.
    """
    try:
        import sync as _sync_mod
        PENNYLANE_TOKEN = os.environ["PENNYLANE_TOKEN"]
        mapping = {r["pennylane_category_name"]: r
                   for r in sb.table("category_mapping").select("*").execute().data}
        result = _sync_mod.sync_pennylane_categories(PENNYLANE_TOKEN, sb, mapping)

        # Retourner les nouvelles catégories non mappées pour affichage immédiat
        unmapped = (sb.table("pennylane_categories")
                    .select("id,label,family_label,first_seen")
                    .eq("is_mapped", False)
                    .order("first_seen", desc=True)
                    .limit(100)
                    .execute().data)
        unmapped = [c for c in unmapped if not _cat_is_technical(c)]

        return {
            "status":   "ok",
            "stats":    result,
            "unmapped": unmapped,
            "count":    len(unmapped),
        }
    except Exception as e:
        logging.error(f"sync_categories_only: {e}")
        return {"status": "error", "detail": str(e)}


@app.get("/api/sync_status")
def get_sync_status():
    """État du sync en cours ou dernier sync (inclut le rapport d'audit et la version)."""
    # Récupérer la version stockée dans sync_meta (écrite par le moteur de sync)
    try:
        meta = sb.table("sync_meta").select("key,value").in_("key", ["sync_version", "last_synced_at"]).execute().data
        meta_dict = {r["key"]: r["value"] for r in meta}
    except Exception:
        meta_dict = {}
    last_synced_at = meta_dict.get("last_synced_at", "—")
    # Calcul de l'ancienneté du dernier sync
    sync_age_hours = None
    sync_overdue   = False
    if last_synced_at and last_synced_at != "—":
        try:
            age = (_dt.utcnow() - _dt.fromisoformat(last_synced_at.rstrip("Z"))).total_seconds() / 3600
            sync_age_hours = round(age, 1)
            sync_overdue   = age > 25   # > 25h = le cron quotidien a raté
        except Exception:
            pass
    return {
        **_sync_state,
        "sync_version":   meta_dict.get("sync_version", "—"),
        "last_synced_at": last_synced_at,
        "sync_age_hours": sync_age_hours,
        "sync_overdue":   sync_overdue,
    }


@app.get("/api/alerts")
def get_alerts(days: int = Query(default=30)):
    """
    Tableau de bord des alertes :
      - Transactions sans mapping P&L (invisibles dans le superviseur)
      - Nouvelles catégories Pennylane non intégrées
      - Recatégorisations récentes détectées lors des syncs
    """
    today  = date.today()
    d_from = (today - timedelta(days=days)).isoformat()

    # 1. Catégories mappées avec section P&L
    mapping_data = (sb.table("category_mapping")
                    .select("pennylane_category_name,pl_section,poste_budgetaire")
                    .execute().data)
    mapped_cats = {r["pennylane_category_name"]
                   for r in mapping_data if r.get("pl_section")}

    # 2. Transactions récentes sans mapping P&L (pagination)
    tx_rows, page, size = [], 0, 1000
    while page < 5:
        batch = (sb.table("transactions")
                 .select("date, label, amount, direction, category_name, id")
                 .gte("date", d_from)
                 .order("date", desc=True)
                 .range(page * size, (page + 1) * size - 1)
                 .execute().data)
        tx_rows.extend(batch)
        if len(batch) < size:
            break
        page += 1

    unmapped_txs = [t for t in tx_rows
                    if not t.get("category_name")
                    or t["category_name"] not in mapped_cats]

    # Grouper par catégorie pour synthèse
    by_cat = defaultdict(lambda: {"count": 0, "total": 0.0,
                                   "sample": "", "last_date": ""})
    for t in unmapped_txs:
        cat = t.get("category_name") or "(aucune catégorie)"
        by_cat[cat]["count"] += 1
        by_cat[cat]["total"] += float(t["amount"] or 0)
        if not by_cat[cat]["sample"] and t.get("label"):
            by_cat[cat]["sample"] = t["label"][:60]
        if t["date"] > by_cat[cat]["last_date"]:
            by_cat[cat]["last_date"] = t["date"]

    unmapped_summary = sorted(
        [{"category": cat, "count": v["count"],
          "total": round(v["total"], 2), "sample": v["sample"],
          "last_date": v["last_date"]}
         for cat, v in by_cat.items()],
        key=lambda x: -x["total"]
    )

    # 3. Nouvelles catégories Pennylane non mappées
    try:
        new_cats = (sb.table("pennylane_categories")
                    .select("id,label,family_label,first_seen")
                    .eq("is_mapped", False)
                    .order("first_seen", desc=True)
                    .limit(50)
                    .execute().data)
        new_cats = [c for c in new_cats if not _cat_is_technical(c)]
    except Exception:
        new_cats = []

    # 4. Recatégorisations récentes (sync_audit.cat_changes)
    try:
        recent_audits = (sb.table("sync_audit")
                         .select("checked_at, cat_changes, summary")
                         .order("checked_at", desc=True)
                         .limit(20)
                         .execute().data)
        recent_changes = []
        for audit in recent_audits:
            for c in (audit.get("cat_changes") or []):
                recent_changes.append({**c, "detected_at": audit["checked_at"]})
    except Exception:
        recent_changes = []

    return {
        "total_alerts": len(unmapped_txs) + len(new_cats),
        "unmapped_transactions": {
            "count":       len(unmapped_txs),
            "by_category": unmapped_summary[:30],
        },
        "unmapped_categories": {
            "count": len(new_cats),
            "data":  new_cats,
        },
        "cat_changes": {
            "count": len(recent_changes),
            "data":  recent_changes[:30],
        },
    }


@app.get("/api/mapping_status")
def get_mapping_status(month: str = Query(default=None)):
    """
    Statut du mapping catégories par section P&L pour un mois donné.
    Retourne pour chaque section : nb catégories actives / total + détail par catégorie.
    Permet de détecter les sections vides et les catégories non mappées.
    """
    if not month:
        month = date.today().strftime("%Y-%m")

    year, mo  = int(month[:4]), int(month[5:7])
    last_day  = _cal.monthrange(year, mo)[1]
    d_from    = f"{month}-01"
    d_to      = f"{month}-{last_day:02d}"

    # Mapping complet
    mapping_data = sb.table("category_mapping").select("*").execute().data

    # Stats transactions du mois (pagination)
    tx_stats: dict = {}
    page = 0
    while True:
        batch = (sb.table("transactions")
                 .select("category_name,amount,direction")
                 .gte("date", d_from)
                 .lte("date", d_to)
                 .range(page * 1000, (page + 1) * 1000 - 1)
                 .execute().data)
        for tx in batch:
            cn  = tx.get("category_name") or ""
            amt = float(tx.get("amount") or 0)
            d   = tx.get("direction", "")
            if cn not in tx_stats:
                tx_stats[cn] = {"nb": 0, "credit": 0.0, "debit": 0.0}
            tx_stats[cn]["nb"] += 1
            if d == "credit":
                tx_stats[cn]["credit"] += amt
            else:
                tx_stats[cn]["debit"] += amt
        if len(batch) < 1000:
            break
        page += 1

    SECTION_NAMES = {
        1: "CA / Revenus",
        2: "Coûts Acquisition",
        3: "Sales & Closing",
        4: "Coûts Delivery",
        5: "Structure",
    }

    # Grouper par section
    by_section: dict = defaultdict(list)
    for m in mapping_data:
        sec = m.get("pl_section")
        if sec:
            by_section[sec].append(m)

    sections = []
    for sec in sorted(by_section.keys()):
        entries = by_section[sec]
        cats = []
        for m in entries:
            cn    = m["pennylane_category_name"]
            stats = tx_stats.get(cn, {"nb": 0, "credit": 0.0, "debit": 0.0})
            cats.append({
                "name":   cn,
                "poste":  m["poste_budgetaire"],
                "nb_tx":  stats["nb"],
                "credit": round(stats["credit"], 2),
                "debit":  round(stats["debit"], 2),
                "active": stats["nb"] > 0,
            })
        active_count = sum(1 for c in cats if c["active"])
        sections.append({
            "id":           sec,
            "name":         SECTION_NAMES.get(sec, f"Section {sec}"),
            "categories":   sorted(cats, key=lambda x: -x["nb_tx"]),
            "active_count": active_count,
            "total_count":  len(cats),
            "all_ok":       active_count == len(cats),
            "empty":        active_count == 0,
        })

    # Catégories Pennylane non mappées (hors familles techniques)
    try:
        unmapped_pl = (sb.table("pennylane_categories")
                       .select("id,label,family_label,first_seen")
                       .eq("is_mapped", False)
                       .order("family_label")
                       .execute().data)
        unmapped_pl = [c for c in unmapped_pl if not _cat_is_technical(c)]
    except Exception:
        unmapped_pl = []

    total_cats   = sum(s["total_count"]  for s in sections)
    total_active = sum(s["active_count"] for s in sections)

    return {
        "month":              month,
        "total_mapped":       total_cats,
        "total_active":       total_active,
        "sections":           sections,
        "unmapped_pennylane": unmapped_pl,
    }


@app.get("/api/export_transactions")
def export_transactions(month: str = Query(default=None),
                        ytd:   bool = Query(default=False)):
    """
    Export CSV de toutes les transactions de la période sélectionnée.
    month=YYYY-MM  → transactions du mois
    ytd=true       → transactions Jan 1 → aujourd'hui
    """
    today = date.today()
    if ytd:
        d_from    = f"{today.year}-01-01"
        d_to      = today.isoformat()
        filename  = f"transactions_YTD_{today.year}.csv"
    else:
        month  = month or today.strftime("%Y-%m")
        d_from, d_to = _month_range(month)
        filename = f"transactions_{month}.csv"

    # Pagination complète
    rows, page = [], 0
    while True:
        batch = (sb.table("transactions")
                 .select("date,label,amount,direction,category_name,family_id,third_party,account_name")
                 .gte("date", d_from)
                 .lte("date", d_to)
                 .order("date", desc=True)
                 .range(page * 1000, (page + 1) * 1000 - 1)
                 .execute().data)
        rows.extend(batch)
        if len(batch) < 1000:
            break
        page += 1

    # Construire le CSV (BOM UTF-8 pour Excel)
    output = io.StringIO()
    output.write("﻿")   # BOM
    writer = csv.writer(output, delimiter=";", quoting=csv.QUOTE_ALL)
    writer.writerow(["Date", "Libellé", "Montant (€)", "Sens",
                     "Catégorie", "Tiers", "Compte"])
    for r in rows:
        amt    = float(r.get("amount") or 0)
        signed = amt if r.get("direction") == "credit" else -amt
        writer.writerow([
            r.get("date", ""),
            r.get("label", ""),
            f"{signed:.2f}".replace(".", ","),
            r.get("direction", ""),
            r.get("category_name", ""),
            r.get("third_party", ""),
            r.get("account_name", ""),
        ])

    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv; charset=utf-8-sig",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/api/revenue_30d")
def get_revenue_30d(month: str = Query(default=None)):
    """
    KPI Revenue — 30 jours glissants (défaut) ou mois spécifique (month=YYYY-MM).
    Retourne : CA quotidien (histogramme), remboursements, panier moyen, tx non traitées.
    """
    today = date.today()
    if month:
        d_from, d_to = _month_range(month)
        # Série : tous les jours du mois
        y, m_int = int(month[:4]), int(month[5:7])
        n_days   = _cal.monthrange(y, m_int)[1]
        day_range = [date(y, m_int, d).isoformat() for d in range(1, n_days + 1)]
    else:
        d_from    = (today - timedelta(days=29)).isoformat()
        d_to      = today.isoformat()
        day_range = [(today - timedelta(days=29 - i)).isoformat() for i in range(30)]

    # 1. CA quotidien depuis kpis_daily
    daily_rows = (
        sb.table("kpis_daily")
        .select("date,ca_ht")
        .gte("date", d_from)
        .lte("date", d_to)
        .order("date")
        .execute().data
    )
    daily_map = {r["date"]: float(r["ca_ht"] or 0) for r in daily_rows}
    total_ca  = sum(daily_map.values())

    # 2. Catégories revenus
    rev_cats   = sb.table("category_mapping").select("pennylane_category_name").eq("is_revenue", True).execute().data
    cat_set    = {r["pennylane_category_name"] for r in rev_cats}
    all_mapped = sb.table("category_mapping").select("pennylane_category_name").execute().data
    mapped_set = {r["pennylane_category_name"] for r in all_mapped if r.get("pennylane_category_name")}

    # 3. Transactions brutes (remboursements + panier + non traitées)
    all_tx, page = [], 0
    while True:
        batch = (
            sb.table("transactions")
            .select("amount,direction,category_name")
            .gte("date", d_from)
            .lte("date", d_to)
            .range(page * 1000, (page + 1) * 1000 - 1)
            .execute().data
        )
        all_tx.extend(batch)
        if len(batch) < 1000:
            break
        page += 1

    total_refunds, rev_tx_count, untreated = 0.0, 0, 0
    for tx in all_tx:
        amt       = float(tx.get("amount") or 0)
        cat       = tx.get("category_name") or ""
        direction = tx.get("direction") or ""
        if direction == "debit" and cat in cat_set:
            total_refunds += amt
        if direction == "credit" and cat in cat_set:
            rev_tx_count  += 1
        if not cat or cat not in mapped_set:
            untreated += 1

    panier_moyen = total_ca / rev_tx_count if rev_tx_count > 0 else 0.0
    daily_series = [{"date": d, "ca": round(daily_map.get(d, 0.0), 2)} for d in day_range]

    return {
        "period": {"from": d_from, "to": d_to},
        "kpis": {
            "ca_total":        round(total_ca, 2),
            "remboursements":  round(total_refunds, 2),
            "panier_moyen":    round(panier_moyen, 2),
            "tx_non_traitees": untreated,
        },
        "daily": daily_series,
    }


@app.get("/api/release_notes")
def get_release_notes(limit: int = Query(default=50, le=100)):
    """Historique des mises à jour du superviseur (changelog automatique)."""
    try:
        rows = (sb.table("release_notes")
                .select("*")
                .order("released_at", desc=True)
                .limit(limit)
                .execute().data)
        return {"data": rows}
    except Exception as e:
        return {"data": [], "error": str(e)}


@app.get("/api/sync_audit")
def get_sync_audit(limit: int = Query(default=10, le=50)):
    """
    Historique des réconciliations hybrides (Pennylane API ↔ Supabase).
    Retourne les N derniers rapports d'audit avec les écarts détectés et corrigés.
    """
    try:
        rows = (
            sb.table("sync_audit")
            .select("*")
            .order("checked_at", desc=True)
            .limit(limit)
            .execute().data
        )
        return {"data": rows}
    except Exception as e:
        return {"data": [], "error": str(e)}


# ── Systeme.io CRM ─────────────────────────────────────────────────────────────────────
import urllib.request as _urllib_req

SYSTEME_KEY  = os.getenv("SYSTEME_API_KEY", "")
SYSTEME_BASE = "https://api.systeme.io/api"

# Formations clés à suivre {id: label_court}
_SYSTEME_COURSES = {
    63490:  "Alchimiste",
    88824:  "Apprenti Alchimiste",
    200772: "Cure Régénérative",
    218921: "Académie",
    106880: "Tronc commun (DBS)",
}

_systeme_cache = {"data": None, "history": None, "refreshed_at": None, "hist_at": None}
_systeme_lock  = threading.Lock()


def _sys_fetch(path: str):
    req = _urllib_req.Request(
        f"{SYSTEME_BASE}{path}",
        headers={"X-API-Key": SYSTEME_KEY, "Accept": "application/json"}
    )
    with _urllib_req.urlopen(req, timeout=20) as r:
        return json.loads(r.read())


def _contact_date(c: dict) -> str:
    """
    Retourne la date de création réelle du contact (YYYY-MM-DD ou YYYY-MM-DDTHH:MM:SSZ).
    Priorité : createdAt > registeredAt (registeredAt = date d'inscription funnel/optin,
    pas la création du contact — peut être écrasée lors d'imports en masse).
    """
    return (c.get("createdAt") or c.get("registeredAt") or "")


def _sys_fetch_contacts_since(since_ym: str = "2026-01", max_pages: int = 300) -> list:
    """
    Pagine TOUS les contacts (pas de stop anticipé sur la date : l'API ne trie
    pas forcément par createdAt, donc on ne peut pas s'arrêter en cours de route).
    Filtre côté Python sur _contact_date(c) >= since_ym.
    """
    url = "/contacts?limit=100"
    in_range = []
    for page in range(1, max_pages + 1):
        try:
            d = _sys_fetch(f"{url}&page={page}")
        except Exception as e:
            logging.warning(f"systeme contacts page {page}: {e}")
            break
        items = d.get("items", [])
        if not items:
            break
        for c in items:
            ym = _contact_date(c)[:7]   # "YYYY-MM"
            if ym >= since_ym:
                in_range.append(c)
        if not d.get("hasMore"):
            break
    logging.info(f"_sys_fetch_contacts_since({since_ym}): {len(in_range)} contacts dans la fenêtre")
    return in_range


def _sys_refresh_main(contacts_2026: list = None) -> dict:
    today      = date.today()
    first_mtd  = date(today.year, today.month, 1)
    prev_last  = first_mtd - timedelta(days=1)
    first_prev = date(prev_last.year, prev_last.month, 1)

    if contacts_2026 is not None:
        # Compter depuis les contacts déjà chargés (évite un 2e appel API)
        mtd  = sum(1 for c in contacts_2026
                   if _contact_date(c)[:7] == today.strftime("%Y-%m"))
        prev = sum(1 for c in contacts_2026
                   if _contact_date(c)[:7] == prev_last.strftime("%Y-%m"))
    else:
        # Fallback : recharger si contacts non fournis
        c = _sys_fetch_contacts_since("2026-01")
        mtd  = sum(1 for x in c if _contact_date(x)[:7] == today.strftime("%Y-%m"))
        prev = sum(1 for x in c if _contact_date(x)[:7] == prev_last.strftime("%Y-%m"))

    growth = round((mtd - prev) / prev * 100, 1) if prev else None

    # Enrollments formations clés
    enrollments = {cid: {"name": n, "active": 0, "total": 0}
                   for cid, n in _SYSTEME_COURSES.items()}
    page, has_more = 1, True
    while has_more and page <= 70:
        d = _sys_fetch(f"/school/enrollments?limit=100&page={page}")
        for e in d.get("items", []):
            cid = e.get("course", {}).get("id")
            if cid in enrollments:
                enrollments[cid]["total"] += 1
                if e.get("active"):
                    enrollments[cid]["active"] += 1
        has_more = d.get("hasMore", False)
        page += 1

    total_active = sum(v["active"] for v in enrollments.values())

    return {
        "new_contacts_mtd":  mtd,
        "new_contacts_prev": prev,
        "growth_pct":        growth,
        "total_active_enrollments": total_active,
        "month":             today.strftime("%Y-%m"),
        "prev_month":        prev_last.strftime("%Y-%m"),
        "enrollments": [
            {"id": cid, "name": v["name"], "active": v["active"], "total": v["total"],
             "pct_active": round(v["active"]/v["total"]*100,1) if v["total"] else 0}
            for cid, v in enrollments.items() if v["total"] > 0
        ],
        "refreshed_at": _dt.utcnow().isoformat() + "Z",
    }


def _sys_refresh_history(contacts_2026: list = None) -> list:
    """Nouveaux contacts par mois depuis janvier 2026 — un seul appel API."""
    today = date.today()
    start = date(2026, 1, 1)

    # Liste des mois à couvrir
    months, cur = [], date(start.year, start.month, 1)
    while cur <= date(today.year, today.month, 1):
        months.append(cur.strftime("%Y-%m"))
        cur = _add_months(cur, 1)

    if contacts_2026 is None:
        contacts_2026 = _sys_fetch_contacts_since(f"{start.isoformat()}T00:00:00Z")

    # Bucketer par mois à partir de createdAt (fallback registeredAt)
    counts: dict = {}
    for c in contacts_2026:
        reg = _contact_date(c)
        if len(reg) >= 7:
            ym = reg[:7]
            counts[ym] = counts.get(ym, 0) + 1

    return [{"month": m, "new_contacts": counts.get(m, 0)} for m in months]


@app.get("/api/systeme")
def get_systeme(refresh: bool = Query(default=False)):
    """Métriques CRM systeme.io (cache 30 min)."""
    if not SYSTEME_KEY:
        return {"error": "SYSTEME_API_KEY non configuré", "data": None}

    with _systeme_lock:
        cached   = _systeme_cache["data"]
        ref_at   = _systeme_cache["refreshed_at"]
        hist     = _systeme_cache["history"]
        hist_at  = _systeme_cache["hist_at"]
        now = _dt.utcnow()
        main_stale = not ref_at  or (now - _dt.fromisoformat(ref_at.rstrip("Z"))).seconds > 1800
        hist_stale = not hist_at or (now - _dt.fromisoformat(hist_at.rstrip("Z"))).seconds > 21600

    # Premier appel ou refresh forcé → synchrone
    if refresh or (cached is None):
        try:
            # Paginer du plus récent → stopper dès contacts < jan 2026
            contacts_2026 = _sys_fetch_contacts_since("2026-01")
            data    = _sys_refresh_main(contacts_2026)
            history = _sys_refresh_history(contacts_2026)
        except Exception as e:
            return {"error": str(e), "data": None}
        with _systeme_lock:
            _systeme_cache["data"]         = data
            _systeme_cache["history"]      = history
            _systeme_cache["refreshed_at"] = data["refreshed_at"]
            _systeme_cache["hist_at"]      = data["refreshed_at"]
        return {"data": data, "history": history}

    # Refresh en arrière-plan si périmé
    if main_stale or hist_stale:
        def _bg():
            try:
                c26 = _sys_fetch_contacts_since("2026-01")
                if main_stale:
                    d = _sys_refresh_main(c26)
                    with _systeme_lock:
                        _systeme_cache["data"] = d
                        _systeme_cache["refreshed_at"] = d["refreshed_at"]
                if hist_stale:
                    h = _sys_refresh_history(c26)
                    with _systeme_lock:
                        _systeme_cache["history"] = h
                        _systeme_cache["hist_at"] = _dt.utcnow().isoformat() + "Z"
            except Exception as e:
                logging.warning(f"systeme bg refresh: {e}")
        threading.Thread(target=_bg, daemon=True).start()

    with _systeme_lock:
        return {"data": _systeme_cache["data"], "history": _systeme_cache["history"]}


@app.get("/api/systeme/debug-contact", include_in_schema=False)
def debug_contact():
    """
    Diagnostic CRM : inspecte les champs de date des premiers contacts.
    Permet de vérifier si createdAt / registeredAt sont les bons champs à utiliser.
    """
    if not SYSTEME_KEY:
        return {"error": "no key"}
    try:
        d     = _sys_fetch("/contacts?limit=10&page=1")
        items = d.get("items", [])
        first = items[0] if items else {}

        # Résumé champs date sur les 10 premiers contacts
        date_summary = []
        for c in items:
            date_summary.append({
                "email":        c.get("email", ""),
                "createdAt":    c.get("createdAt", "—"),
                "registeredAt": c.get("registeredAt", "—"),
                "_contact_date_resolved": _contact_date(c)[:10],
            })

        return {
            "total_items_on_page": len(items),
            "hasMore":             d.get("hasMore"),
            "first_contact_keys":  list(first.keys()),
            "date_field_used":     "createdAt" if first.get("createdAt") else "registeredAt (fallback)",
            "date_samples":        date_summary,
        }
    except Exception as e:
        return {"error": str(e)}


# ── Nouveaux Clients — Pennylane × Systeme.io ──────────────────────────────────────────
import urllib.parse as _urllib_parse
from concurrent.futures import ThreadPoolExecutor as _ThreadPoolExecutor, as_completed as _as_completed

# Mots-clés (lowercase) → nom produit affiché — ordre : plus précis en premier
_CLIENT_TAG_KEYWORDS = {
    "abonnement post-formation alchimiste": "Abonnement Alchimiste",
    "abonnement académie":                  "Abonnement Académie",
    "alchimiste coaching":                  "Alchimiste Coaching",
    "e-learning alchimiste":               "E-Learning Alchimiste",
    "alchimiste e-learning":               "E-Learning Alchimiste",
    "cure en ligne":                        "Cure en ligne",
    "académie":                            "Académie",
    "apprenti":                             "Apprenti",
}

_client_tag_ids: dict | None = None   # {tag_id: product_display} — chargé 1 fois
_client_tag_ids_lock = threading.Lock()
_new_clients_cache: dict = {}         # month → {data, cached_at}
_new_clients_lock = threading.Lock()


def _ensure_client_tag_ids() -> dict:
    global _client_tag_ids
    with _client_tag_ids_lock:
        if _client_tag_ids is not None:
            return _client_tag_ids
    result, page = {}, 1
    while True:
        try:
            d = _sys_fetch(f"/tags?limit=100&page={page}")
        except Exception:
            break
        for tag in d.get("items", []):
            nl = tag["name"].lower().strip()
            for kw, display in _CLIENT_TAG_KEYWORDS.items():
                if kw in nl or nl == kw:
                    result[tag["id"]] = display
                    break
        if not d.get("hasMore"):
            break
        page += 1
    logging.info(f"Client tag IDs resolved: {result}")
    with _client_tag_ids_lock:
        _client_tag_ids = result
    return result


def _sys_contact_by_email(email: str) -> dict | None:
    try:
        d = _sys_fetch(f"/contacts?limit=10&email={_urllib_parse.quote(email)}")
        for c in d.get("items", []):
            if c.get("email", "").lower() == email.lower():
                return c
    except Exception:
        pass
    return None


def _sys_sub_amount(contact_id: int) -> float:
    """Retourne le montant récurrent mensuel actif pour un contact (0 si aucun)."""
    try:
        d = _sys_fetch(f"/payment/subscriptions?contact={contact_id}&limit=100")
        total = 0.0
        for s in d.get("items", []):
            if str(s.get("status", "")).lower() not in ("active", "trialing"):
                continue
            plan = s.get("plan") or {}
            raw  = (plan.get("priceValue") or plan.get("price") or
                    plan.get("amount") or s.get("amount") or 0)
            amt  = float(raw)
            # systeme.io peut renvoyer en centimes → heuristique
            if amt > 1000:
                amt /= 100
            total += amt
        return round(total, 2)
    except Exception:
        return 0.0


@app.get("/api/new_clients")
def get_new_clients(month: str = Query(default=None)):
    """Nouveaux clients du mois : Pennylane transactions × enrichissement systeme.io."""
    if not SYSTEME_KEY:
        return {"error": "SYSTEME_API_KEY non configuré"}

    today = date.today()
    month = month or today.strftime("%Y-%m")
    d_from, d_to = _month_range(month)

    # ── Cache ──
    with _new_clients_lock:
        cached = _new_clients_cache.get(month)
        if cached:
            if (_dt.utcnow() - _dt.fromisoformat(cached["cached_at"])).seconds < 1800:
                return cached["data"]

    # ── 1. Transactions Pennylane depuis Supabase ──
    rev_cats = (sb.table("category_mapping")
                .select("pennylane_category_name")
                .eq("is_revenue", True)
                .execute().data)
    cat_set = {r["pennylane_category_name"] for r in rev_cats}

    DATE_HISTORY = "2026-01-01"

    def _fetch_credit(gte: str, max_p=15):
        rows, pg, sz = [], 0, 1000
        while pg < max_p:
            batch = (sb.table("transactions")
                     .select("date,label,amount,category_name,direction")
                     .eq("direction", "credit")
                     .gte("date", gte)
                     .order("date")
                     .range(pg * sz, (pg + 1) * sz - 1)
                     .execute().data)
            rows.extend(batch)
            if len(batch) < sz:
                break
            pg += 1
        return rows

    period_all = _fetch_credit(d_from)
    period_raw = [tx for tx in period_all
                  if tx["date"] <= d_to and tx.get("category_name") in cat_set]
    before_raw = [tx for tx in _fetch_credit(DATE_HISTORY)
                  if tx["date"] < d_from and tx.get("category_name") in cat_set]

    for tx in period_raw + before_raw:
        tx["email"] = _extract_email(tx.get("label", ""))
        tx["name"]  = _extract_name(tx.get("label", ""))

    period_ok     = [tx for tx in period_raw if tx["email"]]
    emails_before = {tx["email"] for tx in before_raw if tx["email"]}

    # Grouper par email
    by_email: dict = {}
    for tx in period_ok:
        em = tx["email"]
        if em not in by_email:
            by_email[em] = {"email": em, "name": tx["name"] or em, "txs": []}
        by_email[em]["txs"].append(tx)

    # Garder uniquement les NOUVEAUX (jamais achetés avant)
    new_infos = {em: info for em, info in by_email.items() if em not in emails_before}

    if not new_infos:
        result = {"month": month, "clients": [], "count": 0,
                  "total_ca": 0.0, "total_recurring": 0.0, "ca_restant": 0.0}
        with _new_clients_lock:
            _new_clients_cache[month] = {"data": result, "cached_at": _dt.utcnow().isoformat()}
        return result

    # ── 2. Enrichissement systeme.io (parallèle) ──
    client_tags = _ensure_client_tag_ids() if SYSTEME_KEY else {}
    client_tag_set = set(client_tags.keys())
    enriched: dict = {}

    def _enrich(email: str, info: dict):
        ca = sum(float(tx.get("amount") or 0) for tx in info["txs"])
        # Produit par défaut = catégorie Pennylane
        product = info["txs"][0].get("category_name", "—") if info["txs"] else "—"
        has_sub  = False
        recurring = 0.0
        contact  = _sys_contact_by_email(email)
        if contact:
            # Produit depuis les tags
            cids = {t["id"] for t in contact.get("tags", [])}
            match = cids & client_tag_set
            if match:
                product = client_tags[next(iter(match))]
            # Abonnement récurrent
            sub_amt = _sys_sub_amount(contact["id"])
            if sub_amt > 0:
                has_sub   = True
                recurring = sub_amt
        enriched[email] = {
            "email":        email,
            "name":         info["name"],
            "product":      product,
            "ca":           round(ca, 2),
            "has_sub":      has_sub,
            "recurring":    recurring,
            "transactions": [
                {"date": tx["date"],
                 "amount": round(float(tx.get("amount") or 0), 2),
                 "label": (tx.get("label") or "")[:80]}
                for tx in sorted(info["txs"], key=lambda x: x["date"])
            ],
        }

    with _ThreadPoolExecutor(max_workers=6) as pool:
        futs = {pool.submit(_enrich, em, info): em for em, info in new_infos.items()}
        for f in _as_completed(futs, timeout=60):
            try:
                f.result()
            except Exception as ex:
                logging.warning(f"enrich client error: {ex}")

    clients = sorted(enriched.values(), key=lambda x: x["ca"], reverse=True)
    total_ca        = round(sum(c["ca"]        for c in clients), 2)
    total_recurring = round(sum(c["recurring"] for c in clients), 2)

    result = {
        "month":           month,
        "clients":         clients,
        "count":           len(clients),
        "total_ca":        total_ca,
        "total_recurring": total_recurring,
        "ca_restant":      total_recurring,   # = ce qui sera encaissé le mois prochain
    }
    with _new_clients_lock:
        _new_clients_cache[month] = {"data": result, "cached_at": _dt.utcnow().isoformat()}
    return result


# ── Webhook systeme.io → Supabase sales_events ────────────────────────────────────────
SYSTEME_WEBHOOK_SECRET = os.getenv("SYSTEME_WEBHOOK_SECRET", "")

# Tags client produits (identiques à _CLIENT_TAG_KEYWORDS mais utilisés ici au démarrage)
_WEBHOOK_CLIENT_TAGS = {
    "abonnement post-formation alchimiste": "Abonnement Alchimiste",
    "abonnement académie":                  "Abonnement Académie",
    "alchimiste coaching":                  "Alchimiste Coaching",
    "e-learning alchimiste":               "E-Learning Alchimiste",
    "alchimiste e-learning":               "E-Learning Alchimiste",
    "cure en ligne":                        "Cure en ligne",
    "académie":                            "Académie",
    "apprenti":                             "Apprenti",
}

def _tag_to_product(tag_name: str) -> str | None:
    """Retourne le nom produit si le tag est un tag client, None sinon."""
    nl = (tag_name or "").lower().strip()
    for kw, display in _WEBHOOK_CLIENT_TAGS.items():
        if kw in nl or nl == kw:
            return display
    return None


@app.post("/api/webhook/systeme", include_in_schema=False)
async def systeme_webhook(request: Request):
    """
    Reçoit CONTACT_TAG_ADDED / CONTACT_TAG_REMOVED de systeme.io.
    Stocke chaque vente dans la table Supabase `sales_events`.
    """
    body_bytes = await request.body()

    # ── Vérification signature HMAC (optionnelle) ──
    if SYSTEME_WEBHOOK_SECRET:
        sig_header = (request.headers.get("X-Webhook-Signature") or
                      request.headers.get("X-Hub-Signature-256") or
                      request.headers.get("X-Signature") or "")
        expected = "sha256=" + hmac.new(
            SYSTEME_WEBHOOK_SECRET.encode(),
            body_bytes,
            hashlib.sha256
        ).hexdigest()
        if sig_header and not hmac.compare_digest(sig_header, expected):
            logging.warning(f"Webhook signature mismatch: {sig_header!r}")
            # On log mais on n'interrompt pas (payload capturé pour debug)

    try:
        payload = json.loads(body_bytes)
    except Exception:
        return {"status": "error", "detail": "invalid JSON"}

    event_type = request.headers.get("X-Webhook-Event", "")
    logging.info(f"Webhook systeme.io: {event_type} — headers: {dict(request.headers)}")
    logging.info(f"Webhook payload: {json.dumps(payload)[:500]}")

    if event_type not in ("CONTACT_TAG_ADDED", "CONTACT_TAG_REMOVED"):
        return {"status": "ignored", "event": event_type}

    # ── Parser le payload ──
    # Essayer plusieurs formats possibles (v1 / v2)
    contact = payload.get("contact") or {}
    tag     = payload.get("tag") or {}

    # Timestamp de l'événement (plusieurs noms possibles)
    event_at = (payload.get("occurredAt") or payload.get("triggeredAt") or
                payload.get("createdAt")  or payload.get("timestamp") or
                _dt.utcnow().isoformat() + "Z")

    tag_id   = tag.get("id")
    tag_name = tag.get("name") or ""
    product  = _tag_to_product(tag_name)

    if not product:
        logging.info(f"Tag ignoré (non client): '{tag_name}'")
        return {"status": "ignored", "reason": "not a client tag", "tag": tag_name}

    email  = (contact.get("email") or "").lower().strip()
    fields = contact.get("fields") or []
    fname  = next((f.get("value", "") for f in fields if f.get("slug") == "first_name"), "")
    lname  = next((f.get("value", "") for f in fields if f.get("slug") == "surname"), "")
    name   = f"{fname} {lname}".strip() or email

    source = "sale" if event_type == "CONTACT_TAG_ADDED" else "refund"

    try:
        sb.table("sales_events").insert({
            "event_at":    event_at,
            "contact_id":  contact.get("id"),
            "email":       email,
            "name":        name,
            "tag_id":      tag_id,
            "product":     product,
            "source":      source,
            "raw_payload": payload,
        }).execute()
        logging.info(f"✅ sales_events: {source} — {email} → {product} @ {event_at}")
    except Exception as e:
        logging.error(f"sales_events insert error: {e}")
        return {"status": "error", "detail": str(e)}

    return {"status": "ok", "source": source, "product": product, "email": email}


@app.get("/api/sales_events")
def get_sales_events(month: str = Query(default=None)):
    """Nouvelles ventes du mois depuis sales_events (webhook CONTACT_TAG_ADDED)."""
    today = date.today()
    month = month or today.strftime("%Y-%m")
    d_from, d_to = _month_range(month)

    try:
        rows = (sb.table("sales_events")
                .select("event_at,email,name,product,source,contact_id")
                .gte("event_at", f"{d_from}T00:00:00+00:00")
                .lte("event_at", f"{d_to}T23:59:59+00:00")
                .eq("source", "sale")
                .order("event_at", desc=True)
                .execute().data)
    except Exception as e:
        return {"month": month, "error": str(e), "events": [], "count": 0}

    # Enrichir avec les transactions Pennylane (match par email)
    if rows:
        emails = list({r["email"] for r in rows if r.get("email")})
        tx_rows = (sb.table("transactions")
                   .select("date,label,amount,direction,category_name")
                   .gte("date", d_from)
                   .lte("date", d_to)
                   .eq("direction", "credit")
                   .execute().data)

        by_email: dict = {}
        for tx in tx_rows:
            em = _extract_email(tx.get("label", ""))
            if em and em in {r["email"] for r in rows}:
                by_email.setdefault(em, []).append({
                    "date":   tx["date"],
                    "amount": round(float(tx.get("amount") or 0), 2),
                    "label":  (tx.get("label") or "")[:80],
                })

        enriched = []
        for r in rows:
            txs = by_email.get(r.get("email", ""), [])
            ca  = sum(t["amount"] for t in txs)
            enriched.append({**r, "transactions": txs, "ca": round(ca, 2)})
    else:
        enriched = []

    total_ca = round(sum(e.get("ca", 0) for e in enriched), 2)

    return {
        "month":    month,
        "count":    len(enriched),
        "total_ca": total_ca,
        "events":   enriched,
        "note":     "Données depuis le 01/06/2026 (date de mise en place du webhook)"
    }


# ── Middleware no-cache sur l'HTML (force le navigateur à recharger à chaque deploy) ──
class NoCacheHTMLMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        response = await call_next(request)
        ct = response.headers.get("content-type", "")
        if "text/html" in ct:
            response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
            response.headers["Pragma"]        = "no-cache"
            response.headers["Expires"]       = "0"
        return response

app.add_middleware(NoCacheHTMLMiddleware)

# Route explicite pour / → index.html avec no-cache garanti
_frontend_dir = os.path.join(os.path.dirname(__file__), "..", "frontend")
if os.path.exists(_frontend_dir):
    @app.get("/", include_in_schema=False)
    async def _root():
        return FileResponse(
            os.path.join(_frontend_dir, "index.html"),
            headers={"Cache-Control": "no-cache, no-store, must-revalidate",
                     "Pragma": "no-cache", "Expires": "0"}
        )
    app.mount("/", StaticFiles(directory=_frontend_dir, html=True), name="frontend")
