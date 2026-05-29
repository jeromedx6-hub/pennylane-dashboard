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

# ── Version du moteur de sync ──────────────────────────────────────────────
# Incrémenter à chaque deploy significatif pour traçabilité dans le dashboard
SYNC_VERSION = "2026.05.29-1"

# IDs familles Pennylane à ignorer pour le P&L (trésorerie / technique)
# L'API ne retourne PAS le label dans category_group, uniquement l'id numérique
EXCLUDED_FAMILY_IDS = {
    2766080,        # Suivi de trésorerie
    12401360896,    # TVA
    12220481536,    # Test
    12473860096,    # Test 156
    12197789696,    # Transfert interne
    2791098,        # Pole Marketing
}

def _best_category(categories: list) -> str:
    """Retourne le label de catégorie principale (hors familles exclues)."""
    if not categories:
        return ""
    cats_sorted = sorted(categories, key=lambda c: float(c.get("weight", 0)), reverse=True)
    preferred   = [c for c in cats_sorted
                   if int((c.get("category_group") or {}).get("id", 0) or 0)
                   not in EXCLUDED_FAMILY_IDS]
    cat = preferred[0] if preferred else cats_sorted[0]
    return cat.get("label", "")


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


def pl_get_modified_since(token, since_dt):
    """
    Récupère TOUTES les transactions modifiées depuis since_dt via updated_at_gte.
    Attrape les recatégorisations rétroactives quelle que soit la date de la transaction.
    """
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    params  = {"per_page": 100, "updated_at_gte": since_dt.strftime("%Y-%m-%dT%H:%M:%SZ")}
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
    log.info(f"  pl_get_modified_since({since_dt.date()}): {len(rows)} transactions modifiées")
    return rows


# ── Normalisation ──────────────────────────────────────────────────────────

def normalize_transaction(tx):
    """Convertit une transaction Pennylane en ligne Supabase (champs étendus)."""
    amount    = float(tx.get("currency_amount") or tx.get("amount") or 0)
    direction = "credit" if amount >= 0 else "debit"

    # Catégorie principale : poids le plus élevé HORS familles exclues (cf. EXCLUDED_FAMILY_IDS)
    cats = tx.get("categories") or []
    if cats:
        cats_sorted   = sorted(cats, key=lambda c: float(c.get("weight", 0)), reverse=True)
        preferred     = [c for c in cats_sorted
                         if int((c.get("category_group") or {}).get("id", 0) or 0)
                         not in EXCLUDED_FAMILY_IDS]
        cat           = preferred[0] if preferred else cats_sorted[0]
        category_name = cat.get("label", "")
        category_id   = str(cat.get("id", ""))
        family_id     = str((cat.get("category_group") or {}).get("id", ""))
    else:
        category_name = ""
        category_id   = ""
        family_id     = ""

    # Tiers (client ou fournisseur si disponible)
    third_party      = tx.get("customer") or tx.get("supplier") or {}
    third_party_name = third_party.get("name", "") if isinstance(third_party, dict) else ""

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
        # Champs étendus (miroir Pennylane)
        "updated_at_pl": tx.get("updated_at", ""),
        "third_party":   third_party_name,
        "family_id":     family_id,
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
    Réconciliation hybride Pennylane API ↔ Supabase.

    Vérifie 2 niveaux :
      1. Écarts de volume/montant par jour (transactions manquantes ou supprimées)
      2. Changements de catégorie par transaction (recatégorisations rétroactives)

    Auto-corrige tout et écrit le rapport dans sync_audit.
    """
    log.info("🔍 Réconciliation Pennylane ↔ Supabase…")

    # ── 1. Source Pennylane ───────────────────────────────────────────────
    pl_txs = pl_transactions if pl_transactions is not None \
             else pl_get_transactions(token, date_from, date_to)

    # Aggrégation par jour (pour check volume/montant)
    pl_by_day = defaultdict(lambda: {"count": 0, "amount": 0.0})
    # Map par ID (pour check catégorie)
    pl_by_id  = {}
    for tx in pl_txs:
        d      = tx["date"]
        amount = abs(float(tx.get("currency_amount") or tx.get("amount") or 0))
        pl_by_day[d]["count"]  += 1
        pl_by_day[d]["amount"] += amount
        # Catégorie principale (weight la plus haute)
        cats = tx.get("categories") or []
        cat_name = ""
        if cats:
            cat_name = _best_category(cats)
        pl_by_id[f"tx_{tx['id']}"] = {"category_name": cat_name, "date": d}

    # ── 2. Source Supabase (pagination, avec category_name) ───────────────
    d_from_str, d_to_str = date_from.isoformat(), date_to.isoformat()
    sb_rows, page, size = [], 0, 1000
    while page < 30:
        batch = (
            sb.table("transactions")
            .select("date, amount, id, category_name")
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
    sb_by_id  = {}
    for tx in sb_rows:
        d = tx["date"]
        sb_by_day[d]["count"]  += 1
        sb_by_day[d]["amount"] += abs(float(tx["amount"] or 0))
        sb_by_day[d]["ids"].append(tx["id"])
        sb_by_id[tx["id"]] = tx.get("category_name", "")

    # ── 3a. Détection écarts de volume/montant par jour ───────────────────
    gaps = []
    current = date_from
    while current <= date_to:
        d    = current.isoformat()
        pl_d = pl_by_day.get(d, {"count": 0, "amount": 0.0})
        sb_d = sb_by_day.get(d, {"count": 0, "amount": 0.0, "ids": []})

        if abs(pl_d["count"] - sb_d["count"]) > 0 \
                or abs(pl_d["amount"] - sb_d["amount"]) > 0.50:
            gaps.append({
                "date":        d,
                "pl_count":    pl_d["count"],  "sb_count":    sb_d["count"],
                "pl_amount":   round(pl_d["amount"], 2),
                "sb_amount":   round(sb_d["amount"], 2),
                "amount_diff": round(abs(pl_d["amount"] - sb_d["amount"]), 2),
            })
        current += timedelta(days=1)

    # ── 3b. Détection changements de catégorie (recatégorisations) ────────
    cat_changes = []
    for tid, pl_info in pl_by_id.items():
        if tid in sb_by_id and sb_by_id[tid] != pl_info["category_name"]:
            cat_changes.append({
                "tx_id":        tid,
                "date":         pl_info["date"],
                "old_category": sb_by_id[tid],
                "new_category": pl_info["category_name"],
            })

    if cat_changes:
        log.warning(f"  ⚠️  {len(cat_changes)} recatégorisation(s) détectée(s)")
        for c in cat_changes:
            log.info(f"    🔄 {c['tx_id']} {c['date']}: "
                     f"'{c['old_category']}' → '{c['new_category']}'")

    # ── 4. Auto-correction (volume + catégories) ──────────────────────────
    auto_fixed_dates = set()
    needs_fix = gaps or cat_changes

    if needs_fix:
        mapping = {r["pennylane_category_name"]: r
                   for r in sb.table("category_mapping").select("*").execute().data}

        # 4a. Correction écarts de volume
        for gap in gaps:
            d          = gap["date"]
            day_pl     = [tx for tx in pl_txs if tx["date"] == d]
            day_pl_ids = {f"tx_{tx['id']}" for tx in day_pl}

            normalized = [r for r in (normalize_transaction(tx) for tx in day_pl) if r]
            if normalized:
                sb.table("transactions").upsert(normalized, on_conflict="id").execute()

            # Supprimer ghost rows
            ghost_ids = [i for i in sb_by_day.get(d, {}).get("ids", [])
                         if i not in day_pl_ids]
            if ghost_ids:
                log.warning(f"    🗑️  {len(ghost_ids)} ghost(s) supprimé(s) le {d}: {ghost_ids}")
                for gid in ghost_ids:
                    sb.table("transactions").delete().eq("id", gid).execute()

            auto_fixed_dates.add(d)
            log.info(f"    ✅ Volume corrigé {d} — "
                     f"PL:{gap['pl_count']} tx / SB:{gap['sb_count']} tx")

        # 4b. Correction recatégorisations : upsert la tx avec la nouvelle catégorie
        for change in cat_changes:
            tid     = change["tx_id"]
            src_id  = tid.replace("tx_", "")
            day_pl  = [tx for tx in pl_txs if str(tx["id"]) == src_id]
            if day_pl:
                normalized = [r for r in (normalize_transaction(tx) for tx in day_pl) if r]
                if normalized:
                    sb.table("transactions").upsert(normalized, on_conflict="id").execute()
            auto_fixed_dates.add(change["date"])

        # 4c. Recalcul P&L + KPIs pour toutes les dates touchées
        for d in sorted(auto_fixed_dates):
            compute_pl_daily(sb, mapping, date.fromisoformat(d))
            compute_kpis(sb, date.fromisoformat(d))

        log.info(f"  ✅ {len(auto_fixed_dates)} jour(s) recalculé(s) : "
                 f"{', '.join(sorted(auto_fixed_dates))}")
    else:
        log.info("  ✅ Réconciliation OK — aucun écart ni recatégorisation")

    # ── 5. Audit trail ────────────────────────────────────────────────────
    auto_fixed = sorted(auto_fixed_dates)
    n_days = (date_to - date_from).days + 1
    any_issue = gaps or cat_changes
    status = ("auto_fixed" if auto_fixed
              else ("gaps_found" if any_issue else "ok"))

    audit_row = {
        "checked_at":  _dt.utcnow().isoformat() + "Z",
        "date_from":   date_from.isoformat(),
        "date_to":     date_to.isoformat(),
        "status":      status,
        "gaps":        gaps,
        "cat_changes": cat_changes,
        "summary": (
            f"{len(gaps)} écart(s) volume, {len(cat_changes)} recatégorisation(s) "
            f"sur {n_days} jour(s) — {len(auto_fixed)} jour(s) auto-corrigé(s)"
        ),
    }
    try:
        sb.table("sync_audit").insert(audit_row).execute()
    except Exception as e:
        log.warning(f"  sync_audit write failed (table créée ?) : {e}")

    log.info(f"  Audit : {status} | {audit_row['summary']}")
    return {"status": status, "gaps": gaps, "cat_changes": cat_changes,
            "auto_fixed": auto_fixed}


# ── Release notes ────────────────────────────────────────────────────────

def write_release_note(sb, category, title, description="", impact="low", details=None):
    """Écrit une entrée dans la table release_notes (changelog du superviseur)."""
    try:
        sb.table("release_notes").insert({
            "released_at": _dt.utcnow().isoformat() + "Z",
            "category":    category,   # 'new_category' | 'sync_fix' | 'mapping' | 'feature'
            "title":       title,
            "description": description,
            "impact":      impact,     # 'high' | 'medium' | 'low'
            "details":     details or {},
        }).execute()
    except Exception as e:
        log.warning(f"  write_release_note failed: {e}")


# ── Miroir catégories Pennylane ───────────────────────────────────────────

def sync_pennylane_categories(token, sb, mapping):
    """
    Miroir complet des familles + catégories Pennylane → table pennylane_categories.
    - Détecte les nouvelles catégories non encore mappées dans FinBoard
    - Écrit une release note (impact high) si nouvelles catégories trouvées
    - Ignore les familles sans intérêt comptable (Suivi de trésorerie, Test, TVA…)
    """
    IGNORED_FAMILIES = {"Suivi de trésorerie", "Test", "Test 156",
                        "Transfert interne", "TVA"}

    log.info("Sync miroir catégories Pennylane…")
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    # Familles (category_groups)
    groups, params = [], {"per_page": 100}
    while True:
        r = requests.get(f"{BASE_URL}/category_groups", headers=headers, params=params)
        r.raise_for_status()
        data = r.json()
        groups.extend(data.get("items", []))
        if not data.get("has_more"): break
        params["cursor"] = data["next_cursor"]
        time.sleep(0.26)
    group_map = {g["id"]: g["label"] for g in groups}

    # Catégories
    cats, params = [], {"per_page": 100}
    while True:
        r = requests.get(f"{BASE_URL}/categories", headers=headers, params=params)
        r.raise_for_status()
        data = r.json()
        cats.extend(data.get("items", []))
        if not data.get("has_more"): break
        params["cursor"] = data["next_cursor"]
        time.sleep(0.26)

    log.info(f"  {len(cats)} catégories / {len(groups)} familles récupérées")

    # IDs déjà connus dans Supabase
    try:
        existing = {r["id"]: r for r in
                    sb.table("pennylane_categories").select("id,label,is_mapped").execute().data}
    except Exception:
        existing = {}

    now_str        = _dt.utcnow().isoformat() + "Z"
    rows_to_upsert = []
    newly_unmapped = []   # nouvelles catégories non mappées (jamais vues)

    for cat in cats:
        gid          = (cat.get("category_group") or {}).get("id")
        family_label = group_map.get(gid, "")
        is_mapped    = (cat["label"] in mapping
                        and mapping[cat["label"]].get("pl_section") is not None)
        mapped_to    = (mapping[cat["label"]]["poste_budgetaire"]
                        if is_mapped else None)
        is_new       = cat["id"] not in existing

        row = {
            "id":              cat["id"],
            "label":           cat["label"],
            "family_id":       gid,
            "family_label":    family_label,
            "analytical_code": cat.get("analytical_code"),
            "last_seen":       now_str,
            "is_mapped":       is_mapped,
            "mapped_to_poste": mapped_to,
        }
        if is_new:
            row["first_seen"] = now_str

        rows_to_upsert.append(row)

        if is_new and not is_mapped and family_label not in IGNORED_FAMILIES:
            newly_unmapped.append({
                "id":     cat["id"],
                "label":  cat["label"],
                "family": family_label,
            })

    if rows_to_upsert:
        sb.table("pennylane_categories").upsert(rows_to_upsert, on_conflict="id").execute()

    # Release note si nouvelles catégories non mappées
    if newly_unmapped:
        log.warning(f"  ⚠️  {len(newly_unmapped)} nouvelle(s) catégorie(s) non mappée(s)")
        for c in newly_unmapped:
            log.warning(f"    → '{c['label']}' (famille: {c['family']})")
        families = list(set(c["family"] for c in newly_unmapped))
        write_release_note(
            sb,
            category="new_category",
            title=f"{len(newly_unmapped)} nouvelle(s) catégorie(s) Pennylane sans mapping P&L",
            description=(f"Familles : {', '.join(families)}. "
                         "Ces transactions seront ignorées du P&L jusqu'au mapping."),
            impact="high" if len(newly_unmapped) > 2 else "medium",
            details={"categories": newly_unmapped},
        )

    total_unmapped = sum(1 for c in cats
                        if not (c["label"] in mapping
                                and mapping[c["label"]].get("pl_section") is not None)
                        and group_map.get((c.get("category_group") or {}).get("id"), "")
                        not in IGNORED_FAMILIES)
    log.info(f"  Miroir OK — {len(cats)} catégories, "
             f"{total_unmapped} sans mapping, {len(newly_unmapped)} nouvelles")
    return {"total": len(cats), "unmapped": total_unmapped, "new": len(newly_unmapped)}


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
    mapping = {r["pennylane_category_name"]: r
               for r in sb.table("category_mapping").select("*").execute().data}
    log.info(f"Mapping : {len(mapping)} catégories chargées")

    # ── 1. Récupération du dernier sync (pour updated_at_gte) ─────────────
    try:
        meta = sb.table("sync_meta").select("value").eq("key", "last_synced_at").execute().data
        last_synced_at = _dt.fromisoformat(meta[0]["value"].rstrip("Z")) if meta else None
    except Exception:
        last_synced_at = None

    # ── 2. Transactions par fenêtre de dates (nouvelles) ──────────────────
    log.info("Récupération transactions Pennylane (date range)…")
    transactions = pl_get_transactions(PENNYLANE_TOKEN, date_from, date_to)
    log.info(f"  {len(transactions)} transactions dans la fenêtre")

    # ── 3. Transactions modifiées depuis le dernier sync ──────────────────
    #    (recatégorisations rétroactives, peu importe la date de la transaction)
    modified_txs = []
    if last_synced_at:
        modified_txs = pl_get_modified_since(PENNYLANE_TOKEN, last_synced_at)
        if modified_txs:
            # Fusionner : les tx modifiées prennent la priorité (version la plus à jour)
            tx_by_id = {tx["id"]: tx for tx in transactions}
            for tx in modified_txs:
                tx_by_id[tx["id"]] = tx
            transactions = list(tx_by_id.values())
            log.info(f"  Après fusion : {len(transactions)} transactions uniques")

    # ── 4. Normalisation + upsert Supabase ────────────────────────────────
    rows = [r for r in (normalize_transaction(tx) for tx in transactions) if r]
    if rows:
        sb.table("transactions").upsert(rows, on_conflict="id").execute()
        log.info(f"  {len(rows)} transactions upsertées dans Supabase")

    # ── 5. P&L + KPIs : fenêtre + dates des tx modifiées ─────────────────
    dates_to_compute = set()
    current = date_from
    while current <= date_to:
        dates_to_compute.add(current)
        current += timedelta(days=1)
    for tx in modified_txs:                          # dates hors fenêtre
        try:
            dates_to_compute.add(date.fromisoformat(tx["date"]))
        except Exception:
            pass

    for d in sorted(dates_to_compute):
        compute_pl_daily(sb, mapping, d)
        compute_kpis(sb, d)

    # ── 6. Customer invoices ──────────────────────────────────────────────
    sync_customer_invoices(PENNYLANE_TOKEN, sb, date_from, date_to)

    # ── 7. Miroir catégories + détection nouvelles familles/catégories ────
    sync_pennylane_categories(PENNYLANE_TOKEN, sb, mapping)

    # ── 8. Réconciliation hybride (volume + recatégorisations) ────────────
    audit = verify_sync(PENNYLANE_TOKEN, sb, date_from, date_to,
                        pl_transactions=transactions)

    # ── 9. Release note si tx modifiées hors fenêtre ──────────────────────
    extra_dates = sorted(d for d in dates_to_compute
                         if d < date_from or d > date_to)
    if extra_dates:
        write_release_note(
            sb,
            category="sync_fix",
            title=f"{len(modified_txs)} transaction(s) recatégorisée(s) resyncées",
            description=(f"Dates hors fenêtre retraitées : {', '.join(d.isoformat() for d in extra_dates[:10])}"),
            impact="medium" if len(modified_txs) > 5 else "low",
            details={"modified_count": len(modified_txs),
                     "extra_dates": [d.isoformat() for d in extra_dates]},
        )

    # ── 10. Mise à jour last_synced_at + version ─────────────────────────
    try:
        now_iso = _dt.utcnow().isoformat() + "Z"
        sb.table("sync_meta").upsert([
            {"key": "last_synced_at", "value": now_iso},
            {"key": "sync_version",   "value": SYNC_VERSION},
        ], on_conflict="key").execute()
        log.info(f"  sync_meta mis à jour — version {SYNC_VERSION}")
    except Exception as e:
        log.warning(f"  sync_meta update failed: {e}")

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
