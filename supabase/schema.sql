-- ============================================================
-- SCHEMA : pennylane-dashboard
-- ============================================================

-- Transactions brutes Pennylane (sync quotidien)
CREATE TABLE IF NOT EXISTS transactions (
    id              TEXT PRIMARY KEY,
    date            DATE NOT NULL,
    label           TEXT,
    amount          DECIMAL(12,2) NOT NULL,
    direction       TEXT CHECK (direction IN ('credit', 'debit')),
    currency        TEXT DEFAULT 'EUR',
    category_id     TEXT,
    category_name   TEXT,
    account_id      TEXT,
    account_name    TEXT,
    source_type     TEXT, -- 'customer_invoice', 'supplier_invoice', 'bank_transaction'
    source_id       TEXT,
    synced_at       TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_transactions_date     ON transactions(date);
CREATE INDEX IF NOT EXISTS idx_transactions_category ON transactions(category_name);

-- Mapping : catégorie Pennylane → structure analytique du P&L
-- À remplir une fois manuellement, puis stable
CREATE TABLE IF NOT EXISTS category_mapping (
    id                      SERIAL PRIMARY KEY,
    pennylane_category_name TEXT UNIQUE NOT NULL,
    axe1_comptable          TEXT,
    axe2_pole               TEXT,   -- Acquisition | Commercial | Opérationnel | Structure
    axe3_analytics          TEXT,   -- Pub_Meta | Com_Closers | ...
    poste_budgetaire        TEXT,
    pl_section              INT CHECK (pl_section IN (1,2,3,4,5)),
    -- 1=CA  2=Acquisition  3=Sales  4=Ops  5=Structure
    is_revenue              BOOLEAN DEFAULT FALSE,
    updated_at              TIMESTAMPTZ DEFAULT NOW()
);

-- P&L agrégé par jour et par poste
CREATE TABLE IF NOT EXISTS pl_daily (
    id               SERIAL PRIMARY KEY,
    date             DATE NOT NULL,
    poste_budgetaire TEXT NOT NULL,
    axe2_pole        TEXT,
    axe3_analytics   TEXT,
    pl_section       INT,
    is_revenue       BOOLEAN,
    amount           DECIMAL(12,2),
    tx_count         INT DEFAULT 0,
    computed_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(date, poste_budgetaire)
);
CREATE INDEX IF NOT EXISTS idx_pl_daily_date    ON pl_daily(date);
CREATE INDEX IF NOT EXISTS idx_pl_daily_section ON pl_daily(pl_section);

-- KPIs calculés par jour (matérialisés pour perf dashboard)
CREATE TABLE IF NOT EXISTS kpis_daily (
    date                DATE PRIMARY KEY,
    -- Revenus
    ca_ht               DECIMAL(12,2),
    -- Charges par section
    total_acquisition   DECIMAL(12,2),
    total_sales         DECIMAL(12,2),
    total_ops           DECIMAL(12,2),
    total_structure     DECIMAL(12,2),
    total_charges       DECIMAL(12,2),
    -- Marges
    mc1                 DECIMAL(12,2),
    mc1_pct             DECIMAL(5,2),
    mc2                 DECIMAL(12,2),
    mc2_pct             DECIMAL(5,2),
    marge_brute         DECIMAL(12,2),
    marge_brute_pct     DECIMAL(5,2),
    ebitda              DECIMAL(12,2),
    ebitda_pct          DECIMAL(5,2),
    -- KPIs marketing
    roas_cash           DECIMAL(6,2),
    cac_cash            DECIMAL(10,2),
    taux_attrition      DECIMAL(5,2),
    taux_recouvrement   DECIMAL(5,2),
    computed_at         TIMESTAMPTZ DEFAULT NOW()
);

-- Vue mensuelle (utilisée par le dashboard)
CREATE OR REPLACE VIEW pl_monthly AS
SELECT
    DATE_TRUNC('month', date)::DATE         AS month,
    poste_budgetaire,
    axe2_pole,
    axe3_analytics,
    pl_section,
    is_revenue,
    SUM(amount)                             AS amount,
    SUM(tx_count)                           AS tx_count
FROM pl_daily
GROUP BY 1,2,3,4,5,6;

-- ── Customer invoices (nouveaux vs récurrents) ────────────────
CREATE TABLE IF NOT EXISTS customer_invoices_sync (
    id            TEXT PRIMARY KEY,
    customer_id   TEXT NOT NULL,
    customer_name TEXT,
    date          DATE NOT NULL,
    amount        DECIMAL(12,2),
    status        TEXT,
    currency      TEXT DEFAULT 'EUR',
    synced_at     TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_ci_customer ON customer_invoices_sync(customer_id);
CREATE INDEX IF NOT EXISTS idx_ci_date     ON customer_invoices_sync(date);

CREATE OR REPLACE VIEW kpis_monthly AS
SELECT
    DATE_TRUNC('month', date)::DATE         AS month,
    SUM(ca_ht)                              AS ca_ht,
    SUM(total_acquisition)                  AS total_acquisition,
    SUM(total_sales)                        AS total_sales,
    SUM(total_ops)                          AS total_ops,
    SUM(total_structure)                    AS total_structure,
    SUM(total_charges)                      AS total_charges,
    SUM(mc1)                                AS mc1,
    ROUND(SUM(mc1) / NULLIF(SUM(ca_ht),0) * 100, 2) AS mc1_pct,
    SUM(mc2)                                AS mc2,
    ROUND(SUM(mc2) / NULLIF(SUM(ca_ht),0) * 100, 2) AS mc2_pct,
    SUM(marge_brute)                        AS marge_brute,
    ROUND(SUM(marge_brute) / NULLIF(SUM(ca_ht),0) * 100, 2) AS marge_brute_pct,
    SUM(ebitda)                             AS ebitda,
    ROUND(SUM(ebitda) / NULLIF(SUM(ca_ht),0) * 100, 2) AS ebitda_pct,
    ROUND(AVG(roas_cash), 2)                AS roas_cash,
    ROUND(AVG(cac_cash), 2)                 AS cac_cash
FROM kpis_daily
GROUP BY 1;
