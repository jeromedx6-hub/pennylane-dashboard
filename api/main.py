import os
from datetime import date, timedelta
from fastapi import FastAPI, Query
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from supabase import create_client

app = FastAPI(title="FinBoard API")

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
def get_kpis(month: str = Query(default=None)):
    """KPIs agrégés sur un mois (défaut : mois courant)."""
    month = month or date.today().strftime("%Y-%m")
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
def get_pl(month: str = Query(default=None)):
    """P&L analytique mensuel par poste."""
    month = month or date.today().strftime("%Y-%m")
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


# Sert le frontend HTML en production
_frontend_dir = os.path.join(os.path.dirname(__file__), "..", "frontend")
if os.path.exists(_frontend_dir):
    app.mount("/", StaticFiles(directory=_frontend_dir, html=True), name="frontend")
