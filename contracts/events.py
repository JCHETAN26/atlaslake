"""Domain vocabulary shared by every layer: topics, event types, statuses, valid values, field sets.

This module is the single source of truth for *what* exists; the Avro schemas in ``contracts/avro``
define the wire format, and tests assert the two agree.
"""

from __future__ import annotations

from typing import Final

PRODUCER: Final = "atlas-simulator"

TOPIC_ORDERS: Final = "commerce.orders"
TOPIC_PAYMENTS: Final = "commerce.payments"
TOPIC_INVENTORY: Final = "commerce.inventory"
TOPIC_CUSTOMERS: Final = "commerce.customers"
TOPIC_PRODUCTS: Final = "commerce.products"
TOPICS: Final = (TOPIC_ORDERS, TOPIC_PAYMENTS, TOPIC_INVENTORY, TOPIC_CUSTOMERS, TOPIC_PRODUCTS)

# short dataset name used by CLI flags / table names  ->  topic
DATASETS: Final = {
    "orders": TOPIC_ORDERS,
    "payments": TOPIC_PAYMENTS,
    "inventory": TOPIC_INVENTORY,
    "customers": TOPIC_CUSTOMERS,
    "products": TOPIC_PRODUCTS,
}
TOPIC_TO_DATASET: Final = {v: k for k, v in DATASETS.items()}

EVENT_TYPES: Final[dict[str, tuple[str, ...]]] = {
    TOPIC_ORDERS: ("ORDER_CREATED", "ORDER_UPDATED", "ORDER_CANCELLED", "ORDER_SHIPPED", "ORDER_DELIVERED"),
    TOPIC_PAYMENTS: ("PAYMENT_STARTED", "PAYMENT_COMPLETED", "PAYMENT_FAILED"),
    TOPIC_INVENTORY: ("INVENTORY_RESERVED", "INVENTORY_UPDATED"),
    TOPIC_CUSTOMERS: ("CUSTOMER_CREATED", "CUSTOMER_UPDATED"),
    TOPIC_PRODUCTS: ("PRODUCT_CREATED", "PRODUCT_UPDATED"),
}
EVENT_TYPE_TO_TOPIC: Final = {et: t for t, ets in EVENT_TYPES.items() for et in ets}

# Partition key field per topic (Kafka message key). Orders AND payments are keyed by order_id so
# every event of one order is totally ordered within a partition.
PARTITION_KEY: Final = {
    TOPIC_ORDERS: "order_id",
    TOPIC_PAYMENTS: "order_id",
    TOPIC_INVENTORY: "inventory_key",  # f"{store_id}:{product_id}"
    TOPIC_CUSTOMERS: "customer_id",
    TOPIC_PRODUCTS: "product_id",
}

ORDER_STATUSES: Final = ("CREATED", "PAID", "CANCELLED", "SHIPPED", "DELIVERED")
# Lifecycle rank: used only for tie-breaking identical event timestamps.
ORDER_STATUS_RANK: Final = {"CREATED": 0, "PAID": 1, "SHIPPED": 2, "DELIVERED": 3, "CANCELLED": 4}
VALID_TRANSITIONS: Final = {
    "CREATED": {"PAID", "CANCELLED"},
    "PAID": {"SHIPPED", "CANCELLED"},
    "SHIPPED": {"DELIVERED"},
    "DELIVERED": set(),
    "CANCELLED": set(),
}
PAYMENT_STATUSES: Final = ("STARTED", "COMPLETED", "FAILED")
PAYMENT_METHODS: Final = ("CARD", "PAYPAL", "APPLE_PAY", "GIFT_CARD")
INVENTORY_REASONS: Final = ("RESERVATION", "SHIPMENT", "RELEASE", "RESTOCK")
SALES_CHANNELS: Final = ("WEB", "MOBILE", "STORE")
CURRENCIES: Final = ("GBP", "EUR", "USD")
CUSTOMER_SEGMENTS: Final = ("CONSUMER", "SMALL_BUSINESS", "WHOLESALE")

EVENT_STATUS: Final = {
    "ORDER_CREATED": "CREATED",
    "ORDER_CANCELLED": "CANCELLED",
    "ORDER_SHIPPED": "SHIPPED",
    "ORDER_DELIVERED": "DELIVERED",
    # ORDER_UPDATED carries its status explicitly (PAID)
}

ENVELOPE_FIELDS: Final = ("event_id", "event_type", "event_timestamp", "producer", "schema_version")

# Payload fields per topic, in Avro field order (latest schema version).
PAYLOAD_FIELDS: Final[dict[str, tuple[str, ...]]] = {
    TOPIC_ORDERS: (
        "order_id",
        "customer_id",
        "store_id",
        "status",
        "amount_minor",
        "currency",
        "items",
        "cancel_reason",
        "carrier",
        "tracking_number",
        "sales_channel",
    ),
    TOPIC_PAYMENTS: (
        "payment_id",
        "order_id",
        "attempt",
        "status",
        "amount_minor",
        "currency",
        "method",
        "failure_reason",
    ),
    TOPIC_INVENTORY: (
        "inventory_key",
        "store_id",
        "product_id",
        "reason",
        "on_hand_delta",
        "reserved_delta",
        "order_id",
    ),
    TOPIC_CUSTOMERS: (
        "customer_id",
        "email",
        "phone",
        "first_name",
        "last_name",
        "city",
        "state",
        "country",
        "segment",
        "signup_timestamp",
    ),
    TOPIC_PRODUCTS: ("product_id", "sku", "name", "category", "unit_price_minor", "currency", "active"),
}

# Fields that only exist from a given schema version on (schema evolution, Phase 16).
VERSIONED_FIELDS: Final[dict[str, dict[str, int]]] = {TOPIC_ORDERS: {"sales_channel": 2}}
LATEST_SCHEMA_VERSION: Final = {
    TOPIC_ORDERS: 2,
    TOPIC_PAYMENTS: 1,
    TOPIC_INVENTORY: 1,
    TOPIC_CUSTOMERS: 1,
    TOPIC_PRODUCTS: 1,
}

PII_FIELDS: Final = {
    "email": "restricted",
    "phone": "restricted",
    "first_name": "confidential",
    "last_name": "confidential",
}

# FX used by the simulator to price non-GBP stores (reference prices are GBP).
FX_FROM_GBP: Final = {"GBP": 1.0, "EUR": 1.17, "USD": 1.27}
