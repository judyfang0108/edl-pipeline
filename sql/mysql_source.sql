-- ============================================================
-- MySQL Source Tables
-- ============================================================

CREATE DATABASE IF NOT EXISTS source_db;
USE source_db;

-- Debezium user with replication privileges
CREATE USER IF NOT EXISTS 'debezium'@'%' IDENTIFIED BY 'dbz_pass';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium'@'%';
FLUSH PRIVILEGES;

-- products: dimension (what was ordered)
CREATE TABLE IF NOT EXISTS products (
    id       INT          NOT NULL AUTO_INCREMENT,
    name     VARCHAR(255) NOT NULL,
    metadata JSON,
    PRIMARY KEY (id)
);

-- regions: dimension (where the order is from)
CREATE TABLE IF NOT EXISTS regions (
    id       INT         NOT NULL AUTO_INCREMENT,
    region   VARCHAR(64) NOT NULL,
    currency CHAR(3)     NOT NULL DEFAULT 'USD',
    PRIMARY KEY (id)
);

-- payment_methods: dimension (how the order was paid)
CREATE TABLE IF NOT EXISTS payment_methods (
    id          INT         NOT NULL AUTO_INCREMENT,
    method_name VARCHAR(64) NOT NULL,
    provider    VARCHAR(64) NOT NULL DEFAULT 'internal',
    PRIMARY KEY (id)
);

-- promotions: dimension (discount applied to the order)
CREATE TABLE IF NOT EXISTS promotions (
    id           INT           NOT NULL AUTO_INCREMENT,
    promo_code   VARCHAR(32)   NOT NULL,
    discount_pct DECIMAL(5, 2) NOT NULL DEFAULT 0,
    PRIMARY KEY (id)
);

-- orders: fact table
CREATE TABLE IF NOT EXISTS orders (
    id                INT            NOT NULL AUTO_INCREMENT,
    product_id        INT            NOT NULL,
    region_id         INT            NOT NULL,
    payment_method_id INT            NULL,      -- nullable: LEFT JOIN
    promotion_id      INT            NULL,      -- nullable: LEFT JOIN
    amount            DECIMAL(12, 2) NOT NULL DEFAULT 0,
    status            VARCHAR(32)    NOT NULL DEFAULT 'pending',
    PRIMARY KEY (id),
    FOREIGN KEY (product_id)        REFERENCES products(id),
    FOREIGN KEY (region_id)         REFERENCES regions(id),
    FOREIGN KEY (payment_method_id) REFERENCES payment_methods(id),
    FOREIGN KEY (promotion_id)      REFERENCES promotions(id)
);

-- Seed dimension tables
INSERT INTO products (name, metadata) VALUES
    ('Widget Pro',  '{"color":"blue","weight":1.2}'),
    ('Gadget Plus', '{"color":"red","weight":0.8}'),
    ('Super Thing', '{"color":"green","weight":2.5}');

INSERT INTO regions (region, currency) VALUES
    ('us-east', 'USD'),
    ('eu-west', 'EUR'),
    ('ap-south', 'SGD');

INSERT INTO payment_methods (method_name, provider) VALUES
    ('credit_card',   'Stripe'),
    ('bank_transfer', 'Plaid'),
    ('crypto',        'Coinbase');

INSERT INTO promotions (promo_code, discount_pct) VALUES
    ('SAVE10', 10.00),
    ('HALF50', 50.00),
    ('VIP20',  20.00);

-- Seed orders (some rows have payment/promo, some don't — testing LEFT JOIN nulls)
INSERT INTO orders (product_id, region_id, payment_method_id, promotion_id, amount, status) VALUES
    (1, 1, 1,    1,    100.00, 'pending'),   -- has payment + promo
    (2, 2, 2,    NULL, 250.50, 'active'),    -- has payment, no promo
    (3, 3, NULL, NULL,  75.00, 'pending');   -- no payment, no promo
