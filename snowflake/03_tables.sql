-- Gold serving tables — column-for-column mirrors of dbt/models/marts (DuckDB Gold).
-- Money stays in integer minor units (NUMBER(38,0)); timestamps are UTC (TIMESTAMP_NTZ).
USE ROLE ATLAS_TRANSFORMER;
USE SCHEMA ATLASLAKE.GOLD;

-- ------------------------------------------------------------------ dimensions
CREATE TABLE IF NOT EXISTS DIM_CUSTOMER (
    customer_sk        VARCHAR NOT NULL PRIMARY KEY,
    customer_id        VARCHAR NOT NULL,
    email_masked       VARCHAR,
    city               VARCHAR,
    state              VARCHAR,
    country            VARCHAR,
    segment            VARCHAR,
    signup_timestamp   TIMESTAMP_NTZ,
    valid_from         TIMESTAMP_NTZ NOT NULL,
    valid_to           TIMESTAMP_NTZ,
    is_current         BOOLEAN,
    version_number     NUMBER(10,0)
) COMMENT = 'SCD2 customer dimension; grain: customer x attribute version';

CREATE TABLE IF NOT EXISTS DIM_PRODUCT (
    product_sk         VARCHAR NOT NULL PRIMARY KEY,
    product_id         VARCHAR NOT NULL,
    sku                VARCHAR,
    product_name       VARCHAR,
    category           VARCHAR,
    list_price_minor   NUMBER(38,0),
    currency           VARCHAR(3),
    active             BOOLEAN,
    valid_from         TIMESTAMP_NTZ NOT NULL,
    valid_to           TIMESTAMP_NTZ,
    is_current         BOOLEAN,
    version_number     NUMBER(10,0)
) COMMENT = 'SCD2 product dimension; grain: product x (name, category, price, active) version';

CREATE TABLE IF NOT EXISTS DIM_STORE (
    store_sk    VARCHAR NOT NULL PRIMARY KEY,
    store_id    VARCHAR NOT NULL,
    store_name  VARCHAR,
    city        VARCHAR,
    country     VARCHAR,
    currency    VARCHAR(3),
    opened_at   TIMESTAMP_NTZ
) COMMENT = 'Type 1 store dimension; grain: store';

CREATE TABLE IF NOT EXISTS DIM_DATE (
    date_key         NUMBER(8,0) NOT NULL PRIMARY KEY,
    date_day         DATE NOT NULL,
    year             NUMBER(4,0),
    quarter          NUMBER(1,0),
    month            NUMBER(2,0),
    month_name       VARCHAR,
    day_of_month     NUMBER(2,0),
    iso_day_of_week  NUMBER(1,0),
    day_name         VARCHAR,
    is_weekend       BOOLEAN,
    iso_week         NUMBER(2,0)
) COMMENT = 'Conformed calendar; grain: day';

-- ------------------------------------------------------------------ facts
CREATE TABLE IF NOT EXISTS FACT_ORDERS (
    order_id           VARCHAR NOT NULL PRIMARY KEY,
    customer_sk        VARCHAR NOT NULL REFERENCES DIM_CUSTOMER (customer_sk),
    store_sk           VARCHAR REFERENCES DIM_STORE (store_sk),
    date_key           NUMBER(8,0) REFERENCES DIM_DATE (date_key),
    customer_id        VARCHAR,
    store_id           VARCHAR,
    order_date         DATE,
    order_timestamp    TIMESTAMP_NTZ,
    status             VARCHAR,
    sales_channel      VARCHAR,
    amount_minor       NUMBER(38,0),
    currency           VARCHAR(3),
    item_count         NUMBER(10,0),
    line_count         NUMBER(10,0),
    is_paid            BOOLEAN,
    is_cancelled       BOOLEAN,
    is_shipped         BOOLEAN,
    is_delivered       BOOLEAN,
    is_revenue         BOOLEAN,
    paid_at            TIMESTAMP_NTZ,
    cancelled_at       TIMESTAMP_NTZ,
    cancel_reason      VARCHAR,
    shipped_at         TIMESTAMP_NTZ,
    delivered_at       TIMESTAMP_NTZ,
    hours_to_deliver   FLOAT,
    silver_updated_at  TIMESTAMP_NTZ,
    dbt_loaded_at      TIMESTAMP_NTZ
) CLUSTER BY (order_date)
  COMMENT = 'Accumulating snapshot; grain: one row per order';

CREATE TABLE IF NOT EXISTS FACT_ORDER_ITEMS (
    order_id           VARCHAR NOT NULL,
    line_number        NUMBER(10,0) NOT NULL,
    product_sk         VARCHAR REFERENCES DIM_PRODUCT (product_sk),
    customer_sk        VARCHAR REFERENCES DIM_CUSTOMER (customer_sk),
    store_sk           VARCHAR REFERENCES DIM_STORE (store_sk),
    date_key           NUMBER(8,0) REFERENCES DIM_DATE (date_key),
    product_id         VARCHAR,
    order_date         DATE,
    quantity           NUMBER(10,0),
    unit_price_minor   NUMBER(38,0),
    line_amount_minor  NUMBER(38,0),
    currency           VARCHAR(3),
    silver_loaded_at   TIMESTAMP_NTZ,
    dbt_loaded_at      TIMESTAMP_NTZ,
    PRIMARY KEY (order_id, line_number)
) CLUSTER BY (order_date)
  COMMENT = 'Transaction fact; grain: one row per order line';

CREATE TABLE IF NOT EXISTS FACT_PAYMENTS (
    payment_id          VARCHAR NOT NULL PRIMARY KEY,
    order_id            VARCHAR,
    attempt             NUMBER(10,0),
    status              VARCHAR,
    method              VARCHAR,
    amount_minor        NUMBER(38,0),
    currency            VARCHAR(3),
    failure_reason      VARCHAR,
    date_key            NUMBER(8,0) REFERENCES DIM_DATE (date_key),
    payment_date        DATE,
    payment_timestamp   TIMESTAMP_NTZ,
    started_at          TIMESTAMP_NTZ,
    completed_at        TIMESTAMP_NTZ,
    failed_at           TIMESTAMP_NTZ,
    seconds_to_outcome  FLOAT,
    silver_updated_at   TIMESTAMP_NTZ,
    dbt_loaded_at       TIMESTAMP_NTZ
) COMMENT = 'Accumulating fact; grain: one row per payment attempt';

CREATE TABLE IF NOT EXISTS FACT_INVENTORY (
    event_id          VARCHAR NOT NULL PRIMARY KEY,
    inventory_key     VARCHAR,
    store_sk          VARCHAR REFERENCES DIM_STORE (store_sk),
    product_sk        VARCHAR REFERENCES DIM_PRODUCT (product_sk),
    date_key          NUMBER(8,0) REFERENCES DIM_DATE (date_key),
    store_id          VARCHAR,
    product_id        VARCHAR,
    reason            VARCHAR,
    on_hand_delta     NUMBER(10,0),
    reserved_delta    NUMBER(10,0),
    order_id          VARCHAR,
    event_timestamp   TIMESTAMP_NTZ,
    movement_date     DATE,
    silver_loaded_at  TIMESTAMP_NTZ,
    dbt_loaded_at     TIMESTAMP_NTZ
) CLUSTER BY (movement_date)
  COMMENT = 'Transaction fact; grain: one row per inventory movement';

-- ------------------------------------------------------------------ marts
CREATE TABLE IF NOT EXISTS MART_DAILY_SALES (
    order_date             DATE NOT NULL,
    store_id               VARCHAR NOT NULL,
    currency               VARCHAR(3) NOT NULL,
    orders_placed          NUMBER(38,0),
    orders_paid            NUMBER(38,0),
    gross_revenue_minor    NUMBER(38,0),
    items_sold             NUMBER(38,0),
    avg_order_value_minor  NUMBER(38,2),
    max_dbt_loaded_at      TIMESTAMP_NTZ,
    PRIMARY KEY (order_date, store_id, currency)
) COMMENT = 'Grain: order_date x store x currency';

CREATE TABLE IF NOT EXISTS MART_ORDER_FUNNEL (
    order_date         DATE NOT NULL PRIMARY KEY,
    orders_created     NUMBER(38,0),
    orders_paid        NUMBER(38,0),
    orders_shipped     NUMBER(38,0),
    orders_delivered   NUMBER(38,0),
    orders_cancelled   NUMBER(38,0),
    paid_rate          NUMBER(10,4),
    delivered_rate     NUMBER(10,4),
    max_dbt_loaded_at  TIMESTAMP_NTZ
) COMMENT = 'Grain: order_date';

CREATE TABLE IF NOT EXISTS MART_PAYMENT_METRICS (
    payment_date               DATE NOT NULL,
    method                     VARCHAR NOT NULL,
    currency                   VARCHAR(3) NOT NULL,
    attempts                   NUMBER(38,0),
    completed                  NUMBER(38,0),
    failed                     NUMBER(38,0),
    completed_amount_minor     NUMBER(38,0),
    success_rate               NUMBER(10,4),
    median_seconds_to_outcome  NUMBER(18,3),
    max_dbt_loaded_at          TIMESTAMP_NTZ,
    PRIMARY KEY (payment_date, method, currency)
) COMMENT = 'Grain: payment_date x method x currency';

CREATE TABLE IF NOT EXISTS MART_INVENTORY_HEALTH (
    store_id            VARCHAR NOT NULL,
    product_id          VARCHAR NOT NULL,
    store_sk            VARCHAR,
    product_sk          VARCHAR,
    product_name        VARCHAR,
    category            VARCHAR,
    quantity_on_hand    NUMBER(38,0),
    quantity_reserved   NUMBER(38,0),
    quantity_available  NUMBER(38,0),
    is_low_stock        BOOLEAN,
    movement_count      NUMBER(38,0),
    last_movement_at    TIMESTAMP_NTZ,
    PRIMARY KEY (store_id, product_id)
) COMMENT = 'Grain: store x product (current stock position)';

-- Loading pattern used by the publish step (per table):
--   PUT file://.../fact_orders/*.parquet @ATLASLAKE.STAGING.GOLD_STAGE/fact_orders/ AUTO_COMPRESS=FALSE;
--   CREATE OR REPLACE TEMPORARY TABLE ATLASLAKE.STAGING.FACT_ORDERS_LOAD LIKE ATLASLAKE.GOLD.FACT_ORDERS;
--   COPY INTO ATLASLAKE.STAGING.FACT_ORDERS_LOAD FROM @ATLASLAKE.STAGING.GOLD_STAGE/fact_orders/
--        FILE_FORMAT = (FORMAT_NAME = ATLASLAKE.STAGING.PARQUET_FMT) MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE PURGE = TRUE;
--   MERGE INTO ATLASLAKE.GOLD.FACT_ORDERS t USING ATLASLAKE.STAGING.FACT_ORDERS_LOAD s ON t.order_id = s.order_id
--        WHEN MATCHED THEN UPDATE SET ... WHEN NOT MATCHED THEN INSERT ...;   -- incremental, idempotent

-- ------------------------------------------------------------------ optional: Iceberg instead of copies
-- Snowflake can read the lake's Iceberg tables in place (no copy) via an external volume + catalog
-- integration. Sketch (requires an IAM role trusted by Snowflake; see docs/AWS_DEPLOYMENT.md):
--
-- CREATE EXTERNAL VOLUME ATLAS_LAKE_VOL
--   STORAGE_LOCATIONS = ((NAME = 'lake' STORAGE_PROVIDER = 'S3'
--                         STORAGE_BASE_URL = 's3://atlaslake-dev-lake-<account>/warehouse/'
--                         STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<account>:role/atlaslake-snowflake-read'
--                         ENCRYPTION = (TYPE = 'AWS_SSE_KMS' KMS_KEY_ID = '<cmk-arn>')));
-- CREATE CATALOG INTEGRATION ATLAS_OBJECT_STORE_CAT
--   CATALOG_SOURCE = OBJECT_STORE TABLE_FORMAT = ICEBERG ENABLED = TRUE;
-- CREATE ICEBERG TABLE ATLASLAKE.GOLD.SILVER_ORDERS_ICEBERG
--   EXTERNAL_VOLUME = 'ATLAS_LAKE_VOL' CATALOG = 'ATLAS_OBJECT_STORE_CAT'
--   METADATA_FILE_PATH = 'silver/orders/metadata/<current>.metadata.json';
-- ALTER ICEBERG TABLE ATLASLAKE.GOLD.SILVER_ORDERS_ICEBERG REFRESH '<new metadata file>';  -- after each commit
