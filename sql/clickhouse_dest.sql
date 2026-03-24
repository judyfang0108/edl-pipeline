-- ============================================================
-- ClickHouse Destination Tables
-- ============================================================

CREATE DATABASE IF NOT EXISTS dest_db;

-- ============================================================
-- order_summary: orders ⋈ products ⋈ regions
-- ============================================================
CREATE TABLE IF NOT EXISTS dest_db.order_summary
(
    order_id         Int32,
    amount           Decimal(12, 2),
    status           LowCardinality(String),

    product_id       Int32,
    product_name     String,
    product_metadata String,

    region_id        Int32,
    region           String,
    currency         LowCardinality(String),

    _op          LowCardinality(String),
    _version     UInt64,
    _ingested_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY (order_id)
SETTINGS index_granularity = 8192;

CREATE VIEW IF NOT EXISTS dest_db.order_summary_live AS
SELECT * FROM dest_db.order_summary FINAL WHERE _op != 'd';

-- ============================================================
-- order_details: (orders ⋈ products) LEFT JOIN payment_methods LEFT JOIN promotions
--
-- Equivalent SQL logic:
--   WITH op AS (
--     SELECT o.*, p.name AS product_name, p.metadata AS product_metadata
--     FROM orders o JOIN products p ON o.product_id = p.id
--   )
--   SELECT op.*, pm.method_name, pm.provider, pr.promo_code, pr.discount_pct
--   FROM op
--   LEFT JOIN payment_methods pm ON op.payment_method_id = pm.id
--   LEFT JOIN promotions       pr ON op.promotion_id      = pr.id
-- ============================================================
CREATE TABLE IF NOT EXISTS dest_db.order_details
(
    order_id          Int32,
    amount            Decimal(12, 2),
    status            LowCardinality(String),

    -- INNER join (products — always present)
    product_id        Int32,
    product_name      String,
    product_metadata  String,

    -- LEFT join (payment_methods — nullable)
    payment_method_id Nullable(Int32),
    payment_method    LowCardinality(String),
    payment_provider  LowCardinality(String),

    -- LEFT join (promotions — nullable)
    promotion_id      Nullable(Int32),
    promo_code        LowCardinality(String),
    discount_pct      Nullable(Decimal(5, 2)),

    _op          LowCardinality(String),
    _version     UInt64,
    _ingested_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY (order_id)
SETTINGS index_granularity = 8192;

CREATE VIEW IF NOT EXISTS dest_db.order_details_live AS
SELECT * FROM dest_db.order_details FINAL WHERE _op != 'd';
