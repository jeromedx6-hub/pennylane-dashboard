import os
import re
import sys as _sys
import threading
from collections import defaultdict
from datetime import date, timedelta, datetime as _dt
from fastapi import FastAPI, Query
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from supabase import create_client

# Rend sync.py importable depuis l'API
_sync_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'sync')
if _sync_dir not in _sys.path:
    _sys.path.insert(0, _sync_dir)

_sync_lock  = threading.Lock()
_sync_state = {"running": False, "started_at": None, "finished_at": None,
               "error": None, "date_from": None, "date_to": None}

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

    return {"month": month, "data": sorted(aggregated.values(), key=lambda x: (x["pl_section"] or 9, x["poste_budgetaire"]))}


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
    """Liste des transactions Pennylane sous-jacentes à un poste P&L."""
    year = year or str(date.today().year)
    d_from, d_to = f"{year}-01-01", f"{year}-12-31"

    # Trouver les catégories Pennylane correspondant à ce poste
    mapping = (
        sb.table("category_mapping")
        .select("pennylane_category_name")
        .eq("poste_budgetaire", poste)
        .execute().data
    )
    if not mapping:
        return {"poste": poste, "year": year, "data": []}

    cat_names = [r["pennylane_category_name"] for r in mapping]

    rows = (
        sb.table("transactions")
        .select("date, label, amount, direction, account_name, category_name")
        .in_("category_name", cat_names)
        .gte("date", d_from).lte("date", d_to)
        .order("date", desc=False)
        .execute().data
    )
    return {"poste": poste, "year": year, "data": rows}


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
            _sync_mod.run(d_from, d_to)
            with _sync_lock:
                _sync_state["running"]     = False
                _sync_state["finished_at"] = _dt.now().strftime("%H:%M:%S")
        except Exception as e:
            with _sync_lock:
                _sync_state["running"]     = False
                _sync_state["error"]       = str(e)
                _sync_state["finished_at"] = _dt.now().strftime("%H:%M:%S")

    threading.Thread(target=_run, daemon=True).start()
    return {"status": "started", "date_from": d_from.isoformat(), "date_to": d_to.isoformat()}


@app.get("/api/sync_status")
def get_sync_status():
    """État du sync en cours ou dernier sync."""
    return _sync_state


# Sert le frontend HTML en production
_frontend_dir = os.path.join(os.path.dirname(__file__), "..", "frontend")
if os.path.exists(_frontend_dir):
    app.mount("/", StaticFiles(directory=_frontend_dir, html=True), name="frontend")
