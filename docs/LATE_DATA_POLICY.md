# Late and out-of-order data policy (Phase 12)

**Lateness** = `ingestion_timestamp − event_timestamp`. The ingestion timestamp is the Kafka broker's
`LogAppendTime`, and the event timestamp is when the fact happened at the source.

| Arrival | Silver entity state | Real-time aggregate (`silver.realtime_sales_1m`) | Gold (dbt) |
|---|---|---|---|
| on time | applied | counted in its event-time window | included |
| late, within watermark (≤ 10 min behind max event time) | applied | counted (window still open in Spark state) | included |
| late, beyond watermark (e.g. 1 h, 1 day) | **applied** | **excluded**, reported as `numRowsDroppedByWatermark` | **included** |
| out of order (e.g. SHIPPED before CREATED) | merged commutatively, so the final state is independent of arrival order | by event time | included |

How each rule is implemented:

* **Silver never drops valid events because they are late.** Entity state is built from
  last-writer-wins-by-event-time MERGEs, which are commutative (`spark/common/transforms.py`):
  * `status` takes the value with the greatest `(event_timestamp, status_rank)`;
  * lifecycle timestamps take `least()`;
  * creation attributes use `coalesce()`.

  A late `ORDER_CREATED` fills in the customer and amount but can never roll back a later status.
* **The real-time view is approximate by design for very late data.** Spark's watermark bounds state
  size. Anything beyond it is excluded from *that* aggregate only, and is counted.
* **Gold is exact.** dbt recomputes from Silver, and incremental models key on *processing* time
  (`silver_updated_at`), never event time, so a late event that changes an old order is always
  picked up and its (old) order date is recomputed.
* **Late events are counted.** `ops.stream_batches.late_events` counts accepted events whose lateness
  exceeds the allowed lateness (10 min). Grafana shows them.
* **Events too far in the future are rejected.** An event timestamp more than 1 day after ingestion
  is treated as a producer clock bug and sent to the DLQ as `EVENT_TIMESTAMP_IN_FUTURE`.
* **Orders that are not yet complete are held back from Gold.** An order whose `ORDER_CREATED` has
  not arrived has no creation attributes, so it is not in `fact_orders` yet. Reconciliation reports
  these as `incomplete_orders_held_from_gold`; they are an explained difference, not missing data.

Verified by `tests/integration/test_late_events.py`, `test_streaming_correctness.py::test_out_of_order...`
and `evaluation/late_event_test.json`. The last one reports, for each tier (on time, 5 min, 1 h,
1 day late), whether the event appears in the real-time aggregate, Silver and Gold.
