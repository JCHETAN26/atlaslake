-- AtlasLake operational source ("fake production commerce database"), Phase 3.
-- Applied by `atlas db init`. Idempotent.

CREATE SCHEMA IF NOT EXISTS commerce;
CREATE SCHEMA IF NOT EXISTS ops;

DO $$ BEGIN
    CREATE TYPE commerce.order_status AS ENUM ('CREATED', 'PAID', 'CANCELLED', 'SHIPPED', 'DELIVERED');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN
    CREATE TYPE commerce.payment_status AS ENUM ('STARTED', 'COMPLETED', 'FAILED');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

CREATE TABLE IF NOT EXISTS commerce.stores (
    store_id     text PRIMARY KEY,
    name         text NOT NULL,
    city         text NOT NULL,
    country      text NOT NULL,
    currency     char(3) NOT NULL CHECK (currency IN ('GBP', 'EUR', 'USD')),
    opened_at    timestamptz NOT NULL,
    updated_at   timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS commerce.customers (
    customer_id      text PRIMARY KEY,
    email            text NOT NULL UNIQUE,
    phone            text NOT NULL,
    first_name       text NOT NULL,
    last_name        text NOT NULL,
    city             text NOT NULL,
    state            text,
    country          text NOT NULL,
    segment          text NOT NULL CHECK (segment IN ('CONSUMER', 'SMALL_BUSINESS', 'WHOLESALE')),
    created_at       timestamptz NOT NULL,
    updated_at       timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS commerce.products (
    product_id        text PRIMARY KEY,
    sku               text NOT NULL UNIQUE,
    name              text NOT NULL,
    category          text NOT NULL,
    unit_price_minor  bigint NOT NULL CHECK (unit_price_minor >= 0),
    currency          char(3) NOT NULL DEFAULT 'GBP',
    active            boolean NOT NULL DEFAULT true,
    created_at        timestamptz NOT NULL,
    updated_at        timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS commerce.orders (
    order_id       text PRIMARY KEY,
    customer_id    text NOT NULL REFERENCES commerce.customers (customer_id),
    store_id       text NOT NULL REFERENCES commerce.stores (store_id),
    status         commerce.order_status NOT NULL,
    sales_channel  text NOT NULL CHECK (sales_channel IN ('WEB', 'MOBILE', 'STORE')),
    amount_minor   bigint NOT NULL CHECK (amount_minor >= 0),
    currency       char(3) NOT NULL CHECK (currency IN ('GBP', 'EUR', 'USD')),
    created_at     timestamptz NOT NULL,
    updated_at     timestamptz NOT NULL,
    paid_at        timestamptz,
    cancelled_at   timestamptz,
    cancel_reason  text,
    shipped_at     timestamptz,
    delivered_at   timestamptz
);
CREATE INDEX IF NOT EXISTS orders_customer_idx ON commerce.orders (customer_id);
CREATE INDEX IF NOT EXISTS orders_created_idx ON commerce.orders (created_at);
CREATE INDEX IF NOT EXISTS orders_updated_idx ON commerce.orders (updated_at);

CREATE TABLE IF NOT EXISTS commerce.order_items (
    order_id          text NOT NULL REFERENCES commerce.orders (order_id),
    line_number       int  NOT NULL CHECK (line_number > 0),
    product_id        text NOT NULL REFERENCES commerce.products (product_id),
    quantity          int  NOT NULL CHECK (quantity > 0),
    unit_price_minor  bigint NOT NULL CHECK (unit_price_minor >= 0),
    PRIMARY KEY (order_id, line_number)
);
CREATE INDEX IF NOT EXISTS order_items_product_idx ON commerce.order_items (product_id);

CREATE TABLE IF NOT EXISTS commerce.payments (
    payment_id      text PRIMARY KEY,
    order_id        text NOT NULL REFERENCES commerce.orders (order_id),
    attempt         int  NOT NULL CHECK (attempt > 0),
    status          commerce.payment_status NOT NULL,
    method          text NOT NULL,
    amount_minor    bigint NOT NULL CHECK (amount_minor >= 0),
    currency        char(3) NOT NULL,
    failure_reason  text,
    created_at      timestamptz NOT NULL,
    updated_at      timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS payments_order_idx ON commerce.payments (order_id);

CREATE TABLE IF NOT EXISTS commerce.inventory (
    store_id           text NOT NULL REFERENCES commerce.stores (store_id),
    product_id         text NOT NULL REFERENCES commerce.products (product_id),
    quantity_on_hand   int  NOT NULL,
    quantity_reserved  int  NOT NULL,
    updated_at         timestamptz NOT NULL,
    PRIMARY KEY (store_id, product_id)
);

-- Transactional outbox: every business change is written here in the SAME transaction as the
-- entity tables. The relay (ingestion/cdc) publishes it to Kafka. `seq` gives commit order.
CREATE TABLE IF NOT EXISTS commerce.outbox (
    seq              bigserial PRIMARY KEY,
    event_id         uuid NOT NULL UNIQUE,
    event_type       text NOT NULL,
    topic            text NOT NULL,
    partition_key    text NOT NULL,
    event_timestamp  timestamptz NOT NULL,
    payload          jsonb NOT NULL,
    created_at       timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS outbox_event_ts_idx ON commerce.outbox (event_timestamp);

-- ------------------------------------------------------------------ operational metadata
CREATE TABLE IF NOT EXISTS ops.relay_checkpoint (
    relay_name  text PRIMARY KEY,
    last_seq    bigint NOT NULL,
    updated_at  timestamptz NOT NULL DEFAULT now()
);

-- One row per physical Kafka delivery, including injected faults. Lets reconciliation attribute
-- every DLQ record (even undecodable bytes) back to a source event_id.
CREATE TABLE IF NOT EXISTS ops.delivery_log (
    topic          text NOT NULL,
    kafka_partition int NOT NULL,
    kafka_offset   bigint NOT NULL,
    event_id       uuid,
    fault          text NOT NULL DEFAULT 'none',
    run_id         text NOT NULL,
    delivered_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (topic, kafka_partition, kafka_offset)
);
CREATE INDEX IF NOT EXISTS delivery_log_event_idx ON ops.delivery_log (event_id);

CREATE TABLE IF NOT EXISTS ops.simulator_runs (
    run_id       text PRIMARY KEY,
    mode         text NOT NULL,
    config       jsonb NOT NULL,
    started_at   timestamptz NOT NULL DEFAULT now(),
    finished_at  timestamptz,
    stats        jsonb
);

CREATE TABLE IF NOT EXISTS ops.batch_watermarks (
    dataset     text PRIMARY KEY,
    watermark   timestamptz NOT NULL,
    updated_at  timestamptz NOT NULL DEFAULT now()
);

-- ------------------------------------------------------------------ least-privilege grants
GRANT USAGE ON SCHEMA commerce TO commerce_app, etl_reader;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA commerce TO commerce_app;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA commerce TO commerce_app;
GRANT SELECT ON ALL TABLES IN SCHEMA commerce TO etl_reader;
GRANT USAGE ON SCHEMA ops TO etl_reader;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA ops TO etl_reader;
